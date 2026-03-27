use std::cmp::Ordering;

use chrono::Datelike;

use arrow_schema::SchemaRef;
use delta_kernel::expressions::Scalar;
use parquet::data_type::AsBytes;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;

pub struct ColumnStats {
    pub null_count: i64,
    pub min: Option<Scalar>,
    pub max: Option<Scalar>,
    pub tight_bounds: bool,
}

pub struct DeltaStats {
    pub num_records: i64,
    pub tight_bounds: bool,
    pub columns: Vec<(String, ColumnStats)>,
}

fn stats_to_scalars(stats: &Statistics) -> (Option<Scalar>, Option<Scalar>) {
    match stats {
        Statistics::Int32(s) => (
            s.min_opt().map(|v| Scalar::Integer(*v)),
            s.max_opt().map(|v| Scalar::Integer(*v)),
        ),
        Statistics::Int64(s) => (
            s.min_opt().map(|v| Scalar::Long(*v)),
            s.max_opt().map(|v| Scalar::Long(*v)),
        ),
        Statistics::Float(s) => (
            s.min_opt().map(|v| Scalar::Float(*v)),
            s.max_opt().map(|v| Scalar::Float(*v)),
        ),
        Statistics::Double(s) => (
            s.min_opt().map(|v| Scalar::Double(*v)),
            s.max_opt().map(|v| Scalar::Double(*v)),
        ),
        Statistics::Boolean(s) => (
            s.min_opt().map(|v| Scalar::Boolean(*v)),
            s.max_opt().map(|v| Scalar::Boolean(*v)),
        ),
        Statistics::ByteArray(s) => (
            s.min_opt().map(|v| Scalar::Binary(v.data().to_vec())),
            s.max_opt().map(|v| Scalar::Binary(v.data().to_vec())),
        ),
        Statistics::FixedLenByteArray(s) => (
            s.min_opt().map(|v| Scalar::Binary(v.data().to_vec())),
            s.max_opt().map(|v| Scalar::Binary(v.data().to_vec())),
        ),
        Statistics::Int96(s) => (
            s.min_opt().map(|v| Scalar::Binary(v.as_bytes().to_vec())),
            s.max_opt().map(|v| Scalar::Binary(v.as_bytes().to_vec())),
        ),
    }
}

fn pick_bound(current: Option<Scalar>, new: Option<Scalar>, ordering: Ordering) -> Option<Scalar> {
    match (current, new) {
        (None, new) => new,
        (current, None) => current,
        (Some(cur), Some(new)) => {
            if new.logical_partial_cmp(&cur) == Some(ordering) {
                Some(new)
            } else {
                Some(cur)
            }
        }
    }
}

fn format_decimal_value(unscaled: i128, scale: u8) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }
    let divisor = 10i128.pow(scale as u32);
    let whole = unscaled / divisor;
    let frac = (unscaled % divisor).abs();
    format!("{}.{:0>width$}", whole, frac, width = scale as usize)
}

fn scalar_to_json_value(
    scalar: &Scalar,
    data_type: &arrow_schema::DataType,
) -> Option<serde_json::Value> {
    use arrow_schema::DataType::*;
    match (scalar, data_type) {
        (Scalar::Integer(v), Int8 | Int16 | Int32) => Some(serde_json::json!(v)),
        (Scalar::Long(v), Int64) => Some(serde_json::json!(v)),
        (Scalar::Short(v), Int16) => Some(serde_json::json!(v)),
        (Scalar::Byte(v), Int8) => Some(serde_json::json!(v)),
        (Scalar::Float(v), Float16 | Float32) => Some(serde_json::json!(v)),
        (Scalar::Double(v), Float64) => Some(serde_json::json!(v)),
        (Scalar::Boolean(v), Boolean) => Some(serde_json::json!(v)),

        (Scalar::String(s), Utf8 | LargeUtf8 | Utf8View) => Some(serde_json::json!(s)),
        (Scalar::Binary(bytes), Utf8 | LargeUtf8 | Utf8View) => String::from_utf8(bytes.clone())
            .ok()
            .map(|s| serde_json::json!(s)),

        (Scalar::Integer(days), Date32) | (Scalar::Date(days), Date32) => {
            chrono::NaiveDate::from_num_days_from_ce_opt(
                chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?.num_days_from_ce() + days,
            )
            .map(|d| serde_json::json!(d.format("%Y-%m-%d").to_string()))
        }

        (Scalar::Long(micros), Timestamp(_, Some(_)))
        | (Scalar::Timestamp(micros), Timestamp(_, Some(_))) => {
            chrono::DateTime::from_timestamp_micros(*micros)
                .map(|dt| serde_json::json!(dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()))
        }

        (Scalar::Long(micros), Timestamp(_, None))
        | (Scalar::TimestampNtz(micros), Timestamp(_, None)) => {
            chrono::DateTime::from_timestamp_micros(*micros).map(|dt| {
                serde_json::json!(dt.naive_utc().format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
            })
        }

        // Decimal128 can have fewer than 16 bytes if the precision is smaller
        (Scalar::Binary(bytes), Decimal128(_precision, scale)) => {
            // check sign and then fill based on negative or not
            let mut buf = if bytes.first().is_some_and(|b| b & 0x80 != 0) {
                [0xFF_u8; 16]
            } else {
                [0u8; 16]
            };
            // figure out where to start the bytes in the array
            let start = 16 - bytes.len();
            buf[start..].copy_from_slice(bytes);
            let value = i128::from_be_bytes(buf);
            Some(serde_json::json!(format_decimal_value(value, *scale as u8)))
        }
        (Scalar::Integer(v), Decimal32(_p, scale) | Decimal128(_p, scale)) => Some(
            serde_json::json!(format_decimal_value(*v as i128, *scale as u8)),
        ),
        (Scalar::Long(v), Decimal64(_p, scale) | Decimal128(_p, scale)) => Some(serde_json::json!(
            format_decimal_value(*v as i128, *scale as u8)
        )),
        (Scalar::Decimal(d), _) => {
            Some(serde_json::json!(format_decimal_value(d.bits(), d.scale())))
        }

        (Scalar::Binary(bytes), Binary | LargeBinary | BinaryView | FixedSizeBinary(_)) => {
            let escaped: String = bytes.iter().map(|b| format!("\\u{:04x}", b)).collect();
            Some(serde_json::json!(escaped))
        }

        _ => None,
    }
}

pub fn delta_stats_to_json(
    stats: &DeltaStats,
    schema: &SchemaRef,
) -> Result<String, anyhow::Error> {
    let mut min_values = serde_json::Map::new();
    let mut max_values = serde_json::Map::new();
    let mut null_count = serde_json::Map::new();

    for (col_name, col_stats) in &stats.columns {
        let field = match schema.field_with_name(col_name) {
            Ok(f) => f,
            Err(_) => continue,
        };

        let data_type = field.data_type();
        null_count.insert(col_name.clone(), serde_json::json!(col_stats.null_count));

        if let Some(ref min) = col_stats.min {
            if let Some(val) = scalar_to_json_value(min, data_type) {
                min_values.insert(col_name.clone(), val);
            }
        }
        if let Some(ref max) = col_stats.max {
            if let Some(val) = scalar_to_json_value(max, data_type) {
                max_values.insert(col_name.clone(), val);
            }
        }
    }

    let stats_obj = serde_json::json!({
        "numRecords": stats.num_records,
        "tightBounds": stats.tight_bounds,
        "minValues": min_values,
        "maxValues": max_values,
        "nullCount": null_count,
    });

    serde_json::to_string(&stats_obj).map_err(Into::into)
}

pub fn parquet_metadata_to_delta_stats(
    metadata: &ParquetMetaData,
    schema: &SchemaRef,
) -> DeltaStats {
    let num_records: i64 = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();

    let columns: Vec<(String, ColumnStats)> = (0..schema.fields().len())
        .map(|col_idx| {
            let col_name = schema.field(col_idx).name().clone();

            let stats = metadata
                .row_groups()
                .iter()
                .filter_map(|rg| rg.column(col_idx).statistics())
                .fold(
                    ColumnStats {
                        null_count: 0,
                        min: None,
                        max: None,
                        tight_bounds: true,
                    },
                    |acc, s| {
                        let (new_min, new_max) = stats_to_scalars(s);
                        ColumnStats {
                            null_count: acc.null_count + s.null_count_opt().unwrap_or(0) as i64,
                            min: pick_bound(acc.min, new_min, Ordering::Less),
                            max: pick_bound(acc.max, new_max, Ordering::Greater),
                            tight_bounds: acc.tight_bounds && s.min_is_exact() && s.max_is_exact(),
                        }
                    },
                );

            (col_name, stats)
        })
        .collect();

    // if the min and max are none then we drop this as
    // it will say the bounds are inexact which isn't exactly true
    let tight_bounds = columns
        .iter()
        .filter(|(_, s)| s.min.is_some() || s.max.is_some())
        .all(|(_, s)| s.tight_bounds);

    DeltaStats {
        num_records,
        tight_bounds,
        columns,
    }
}
