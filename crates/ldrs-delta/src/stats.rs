use std::cmp::Ordering;
use std::collections::HashMap;

use anyhow::Context;
use arrow::array::AsArray;
use arrow_array::Array;
use arrow_schema::{DataType, SchemaRef};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::engine_data::FilteredEngineData;
use delta_kernel::expressions::Scalar;
use parquet::data_type::AsBytes;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics::{self, Double, Float};

pub struct ColumnStats {
    pub null_count: Option<u64>,
    pub min: Option<Scalar>,
    pub max: Option<Scalar>,
    pub tight_bounds: bool,
}

pub struct DeltaStats {
    pub num_records: i64,
    pub tight_bounds: bool,
    pub columns: Vec<(String, ColumnStats)>,
}

/// The logged `add.stats` JSON of every selected file in one scan-output batch, keyed by path.
pub(crate) fn read_add_stats(
    scan_files: &FilteredEngineData,
) -> Result<HashMap<String, Option<String>>, anyhow::Error> {
    let batch = scan_files
        .data()
        .any_ref()
        .downcast_ref::<ArrowEngineData>()
        .ok_or_else(|| anyhow::anyhow!("scan output was not ArrowEngineData"))?
        .record_batch();
    let selection = scan_files.selection_vector();

    let paths = batch
        .column_by_name("path")
        .map(|c| c.as_string::<i32>())
        .ok_or_else(|| anyhow::anyhow!("scan row schema has no path column"))?;
    let stats = batch
        .column_by_name("stats")
        .map(|c| c.as_string::<i32>())
        .ok_or_else(|| anyhow::anyhow!("scan row schema has no stats column"))?;

    let mut out = HashMap::new();
    for row in 0..batch.num_rows() {
        // selection vector may be shorter than the batch; a missing tail entry means selected
        if !selection.get(row).copied().unwrap_or(true) {
            continue;
        }
        out.insert(
            paths.value(row).to_string(),
            (!stats.is_null(row)).then(|| stats.value(row).to_string()),
        );
    }
    Ok(out)
}

pub(crate) fn widen_stats(logged: &str, num_records: i64) -> Result<String, anyhow::Error> {
    let mut stats: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(logged).with_context(|| "logged add.stats is not a JSON object")?;
    stats.insert(
        "numRecords".to_string(),
        serde_json::Value::from(num_records),
    );
    stats.insert("tightBounds".to_string(), serde_json::Value::Bool(false));
    Ok(serde_json::to_string(&stats)?)
}

pub(crate) fn stats_to_scalars(stats: &Statistics) -> (Option<Scalar>, Option<Scalar>) {
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

/// Coerce a physical-type Scalar from parquet stats to the logical-type Scalar
/// expected by delta-kernel for a column of the given DataType.
/// Most cases pass through; only a few need conversion (Binary→String for Utf8,
/// Long→Timestamp, Integer→Date).
fn coerce_to_logical(scalar: Option<Scalar>, data_type: &DataType) -> Option<Scalar> {
    use arrow_schema::DataType::*;
    match (scalar?, data_type) {
        (Scalar::Binary(bytes), Utf8 | LargeUtf8 | Utf8View) => std::str::from_utf8(&bytes)
            .ok()
            .map(|s| Scalar::String(s.to_string())),
        (Scalar::Long(v), Timestamp(_, Some(_))) => Some(Scalar::Timestamp(v)),
        (Scalar::Long(v), Timestamp(_, None)) => Some(Scalar::TimestampNtz(v)),
        (Scalar::Integer(v), Date32) => Some(Scalar::Date(v)),
        // We need to go between an integer backed decimal and make sure it is the correct type
        // based on the logical type
        (Scalar::Integer(v), Decimal32(p, s) | Decimal64(p, s) | Decimal128(p, s)) => {
            Scalar::decimal(v, *p, *s as u8).ok()
        }
        (Scalar::Long(v), Decimal32(p, s) | Decimal64(p, s) | Decimal128(p, s)) => {
            Scalar::decimal(v, *p, *s as u8).ok()
        }
        (Scalar::Binary(bytes), Decimal128(p, s)) => {
            Scalar::decimal(decimal_be_bytes_to_i128(&bytes), *p, *s as u8).ok()
        }
        (scalar, _) => Some(scalar),
    }
}

pub(crate) fn pick_bound(
    current: Option<Scalar>,
    new: Option<Scalar>,
    ordering: Ordering,
) -> Option<Scalar> {
    match (current, new) {
        (current, Some(new)) if is_nan(&new) => current,
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

fn is_nan(scalar: &Scalar) -> bool {
    match scalar {
        Scalar::Float(v) => v.is_nan(),
        Scalar::Double(v) => v.is_nan(),
        _ => false,
    }
}

fn decimal_be_bytes_to_i128(bytes: &[u8]) -> i128 {
    // check sign and then fill based on negative or not
    let mut buf = if bytes.first().is_some_and(|b| b & 0x80 != 0) {
        [0xFF_u8; 16]
    } else {
        [0u8; 16]
    };
    // figure out where to start the bytes in the array
    let start = 16 - bytes.len();
    buf[start..].copy_from_slice(bytes);
    i128::from_be_bytes(buf)
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

        (Scalar::Float(v), Float16 | Float32) if v.is_nan() => Some(serde_json::json!("NaN")),
        (Scalar::Double(v), Float64) if v.is_nan() => Some(serde_json::json!("NaN")),

        (Scalar::Float(v), Float16 | Float32) => Some(serde_json::json!(v)),
        (Scalar::Double(v), Float64) => Some(serde_json::json!(v)),

        (Scalar::Boolean(v), Boolean) => Some(serde_json::json!(v)),

        (Scalar::String(s), Utf8 | LargeUtf8 | Utf8View) => Some(serde_json::json!(s)),
        (Scalar::Binary(bytes), Utf8 | LargeUtf8 | Utf8View) => String::from_utf8(bytes.clone())
            .ok()
            .map(|s| serde_json::json!(s)),

        (Scalar::Integer(days), Date32) | (Scalar::Date(days), Date32) => {
            chrono::NaiveDate::from_epoch_days(*days)
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

        (Scalar::Binary(bytes), Decimal128(_precision, scale)) => Some(serde_json::json!(
            format_decimal_value(decimal_be_bytes_to_i128(bytes), *scale as u8)
        )),
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
        if let Some(nulls) = col_stats.null_count {
            null_count.insert(col_name.clone(), serde_json::json!(nulls));
        }

        if let Some(ref min) = col_stats.min
            && let Some(val) = scalar_to_json_value(min, data_type)
        {
            min_values.insert(col_name.clone(), val);
        }
        if let Some(ref max) = col_stats.max
            && let Some(val) = scalar_to_json_value(max, data_type)
        {
            max_values.insert(col_name.clone(), val);
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

fn scalar_to_i64(scalar: &Scalar) -> Result<i64, anyhow::Error> {
    match scalar {
        Scalar::Byte(v) => Ok(*v as i64),
        Scalar::Short(v) => Ok(*v as i64),
        Scalar::Integer(v) => Ok(*v as i64),
        Scalar::Long(v) => Ok(*v),
        Scalar::Timestamp(micros) => Ok(*micros),
        Scalar::TimestampNtz(micros) => Ok(*micros),
        Scalar::Date(days) => Ok(*days as i64),
        other => anyhow::bail!(
            "watermark column type {:?} cannot be converted to i64",
            other
        ),
    }
}

pub(crate) fn max_stat_as_i64(
    source_files: &[(String, ParquetMetaData, u64)],
    col_name: &str,
) -> Result<i64, anyhow::Error> {
    let max_scalar = source_files
        .iter()
        .flat_map(|(_, m, _)| column_statistics(m, col_name))
        .try_fold(None, |acc, stats| {
            let stats = stats?;
            let (_, new_max) = stats_to_scalars(stats);
            Some(pick_bound(acc, new_max, Ordering::Greater))
        })
        .flatten()
        .ok_or_else(|| anyhow::anyhow!("no stats for watermark column '{}'", col_name))?;

    scalar_to_i64(&max_scalar)
}

pub(crate) fn key_bounds_as_scalars(
    source_files: &[(String, ParquetMetaData, u64)],
    key_col_name: &str,
    schema: &SchemaRef,
) -> Option<(Scalar, Scalar)> {
    let data_type = schema.field_with_name(key_col_name).ok()?.data_type();

    let (min, max) = source_files
        .iter()
        .flat_map(|(_, m, _)| column_statistics(m, key_col_name))
        .try_fold(None, |acc, stats| {
            let stats = stats?;
            match stats {
                // any Nan does not have a bound
                Float(_) | Double(_) if stats.nan_count_opt() != Some(0) => None,
                _ => Some(()),
            }?;
            let (new_min, new_max) = stats_to_scalars(stats);
            let (cur_min, cur_max) = acc.unzip();
            Some(pick_bound(cur_min, new_min, Ordering::Less).zip(pick_bound(
                cur_max,
                new_max,
                Ordering::Greater,
            )))
        })
        .flatten()?;

    Some((
        coerce_to_logical(Some(min), data_type)?,
        coerce_to_logical(Some(max), data_type)?,
    ))
}

/// Position of top-level `column` among the file's leaf columns
pub(crate) fn leaf_index(metadata: &ParquetMetaData, column: &str) -> Option<usize> {
    metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|descriptor| descriptor.path().parts() == [column])
}

/// The statistics each row group recorded for top-level `column`
pub(crate) fn column_statistics<'a>(
    metadata: &'a ParquetMetaData,
    column: &str,
) -> impl Iterator<Item = Option<&'a Statistics>> + Clone + 'a {
    let index = leaf_index(metadata, column);
    metadata
        .row_groups()
        .iter()
        .map(move |rg| index.and_then(|i| rg.column(i).statistics()))
}

/// Row groups whose stats on `column` overlap the key range.
/// A file that does not hold the column keeps every row group.
pub(crate) fn select_row_groups_by_scalars(
    metadata: &ParquetMetaData,
    column: &str,
    min_key: &Scalar,
    max_key: &Scalar,
) -> Vec<usize> {
    let col_idx = match leaf_index(metadata, column) {
        Some(idx) => idx,
        None => return (0..metadata.num_row_groups()).collect(),
    };

    metadata
        .row_groups()
        .iter()
        .enumerate()
        .filter(|(_, rg)| {
            let stats = match rg.column(col_idx).statistics() {
                Some(s) => s,
                None => return true,
            };
            let (rg_min, rg_max) = stats_to_scalars(stats);
            // rg_max >= min_key
            let max_ge_min = rg_max
                .as_ref()
                .and_then(|m| m.logical_partial_cmp(min_key))
                .is_none_or(|ord| ord != Ordering::Less);
            // rg_min <= max_key
            let min_le_max = rg_min
                .as_ref()
                .and_then(|m| m.logical_partial_cmp(max_key))
                .is_none_or(|ord| ord != Ordering::Greater);
            max_ge_min && min_le_max
        })
        .map(|(i, _)| i)
        .collect()
}

pub fn parquet_metadata_to_delta_stats(
    metadata: &ParquetMetaData,
    schema: &SchemaRef,
) -> DeltaStats {
    let num_records: i64 = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();

    let columns: Vec<(String, ColumnStats)> = schema
        .fields()
        .iter()
        .map(|field| {
            let col_name = field.name().clone();
            let row_groups = column_statistics(metadata, &col_name);

            // None when any row group lacks stats or a null count
            let null_count = row_groups
                .clone()
                .map(|s| s?.null_count_opt())
                .sum::<Option<u64>>();

            let stats = row_groups.clone().flatten().fold(
                ColumnStats {
                    null_count,
                    min: None,
                    max: None,
                    tight_bounds: true,
                },
                |acc, s| {
                    let (new_min, new_max) = stats_to_scalars(s);
                    ColumnStats {
                        min: pick_bound(acc.min, new_min, Ordering::Less),
                        max: pick_bound(acc.max, new_max, Ordering::Greater),
                        tight_bounds: acc.tight_bounds && s.min_is_exact() && s.max_is_exact(),
                        ..acc
                    }
                },
            );

            let nan = match field.data_type() {
                DataType::Float64 => Some(Scalar::Double(f64::NAN)),
                DataType::Float16 | DataType::Float32 => Some(Scalar::Float(f32::NAN)),
                _ => None,
            };

            let (min, max) = match (nan, null_count) {
                (_, None) => (None, None),
                (None, Some(_)) => (stats.min, stats.max),
                (Some(nan), Some(nulls)) => {
                    let nans = row_groups.map(|s| s?.nan_count_opt()).sum::<Option<u64>>();
                    let values = num_records as u64 - nulls;
                    match nans {
                        None => (None, None),
                        Some(0) => (stats.min, stats.max),
                        Some(n) if n == values => (Some(nan.clone()), Some(nan)),
                        Some(_) => (stats.min, Some(nan)),
                    }
                }
            };

            (col_name, ColumnStats { min, max, ..stats })
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_array::{Float32Array, Float64Array};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use delta_kernel::expressions::Scalar;
    use parquet::arrow::ArrowSchemaConverter;
    use parquet::file::metadata::{ColumnChunkMetaData, FileMetaData, RowGroupMetaData};
    use parquet::file::statistics::ValueStatistics;
    use serde_json::json;

    #[test]
    fn test_scalar_to_json_dates() {
        assert_eq!(
            scalar_to_json_value(&Scalar::Integer(20177), &DataType::Date32),
            Some(json!("2025-03-30"))
        );
        assert_eq!(
            scalar_to_json_value(&Scalar::Integer(-1), &DataType::Date32),
            Some(json!("1969-12-31"))
        );
    }

    #[test]
    fn coerce_int_backed_decimal_to_decimal_scalar() {
        // A Decimal(12,0) key is stored physically as int64 in parquet, so its stats yield a
        // Scalar::Long. It must lift to a Decimal scalar
        match coerce_to_logical(Some(Scalar::Long(42)), &DataType::Decimal64(12, 0)) {
            Some(Scalar::Decimal(d)) => {
                assert_eq!(d.bits(), 42);
                assert_eq!(d.scale(), 0);
            }
            other => panic!("int64-backed decimal should coerce to Decimal, got {other:?}"),
        }
        // int32-backed (Decimal32) and byte-backed (precision > 18) also lift
        assert!(matches!(
            coerce_to_logical(Some(Scalar::Integer(7)), &DataType::Decimal32(5, 0)),
            Some(Scalar::Decimal(_))
        ));
        assert!(matches!(
            coerce_to_logical(
                Some(Scalar::Binary(1234i128.to_be_bytes().to_vec())),
                &DataType::Decimal128(38, 2),
            ),
            Some(Scalar::Decimal(_))
        ));
        // non-decimal targets pass through unchanged
        assert_eq!(
            coerce_to_logical(Some(Scalar::Long(5)), &DataType::Int64),
            Some(Scalar::Long(5))
        );
    }

    #[test]
    fn test_scalar_to_json_timestamps() {
        let micros = 1_743_342_600 * 1_000_000;

        assert_eq!(
            scalar_to_json_value(
                &Scalar::Long(micros),
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            Some(json!("2025-03-30T13:50:00.000Z"))
        );
        assert_eq!(
            scalar_to_json_value(
                &Scalar::Long(micros),
                &DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            Some(json!("2025-03-30T13:50:00.000"))
        );
        assert_eq!(
            scalar_to_json_value(
                &Scalar::TimestampNtz(micros),
                &DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            Some(json!("2025-03-30T13:50:00.000"))
        );
    }

    #[test]
    fn test_scalar_to_json_unmatched_returns_none() {
        assert_eq!(
            scalar_to_json_value(&Scalar::Integer(42), &DataType::Float64),
            None
        );
    }

    #[test]
    fn nan_double_serializes_as_the_nan_string() {
        assert_eq!(
            scalar_to_json_value(&Scalar::Double(f64::NAN), &DataType::Float64),
            Some(json!("NaN"))
        );
    }

    #[test]
    fn nan_float_serializes_as_the_nan_string() {
        assert_eq!(
            scalar_to_json_value(&Scalar::Float(f32::NAN), &DataType::Float32),
            Some(json!("NaN"))
        );
    }

    fn metadata_from_batches(batches: &[&arrow_array::RecordBatch]) -> ParquetMetaData {
        let mut buf: Vec<u8> = Vec::new();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut buf, batches[0].schema(), None).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        parquet::file::metadata::ParquetMetaDataReader::new()
            .parse_and_finish(&bytes::Bytes::from(buf))
            .unwrap()
    }

    #[test]
    fn leaf_index_counts_the_leaves_of_a_struct_before_the_column() {
        use arrow_array::{ArrayRef, Int64Array, StructArray};
        use arrow_schema::{Field, Fields, Schema};
        use std::sync::Arc;

        // `nested` has two leaves, so top-level `id` is field 1 but leaf 2
        let inner = Fields::from(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);
        let nested = StructArray::new(
            inner.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2])) as ArrayRef,
            ],
            None,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("nested", DataType::Struct(inner), true),
            Field::new("id", DataType::Int64, true),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![Arc::new(nested), Arc::new(Int64Array::from(vec![3]))],
        )
        .unwrap();
        let metadata = metadata_from_batches(&[&batch]);

        // the nested `nested.id` leaf does not answer for top-level `id`
        assert_eq!(leaf_index(&metadata, "id"), Some(2));
        assert_eq!(leaf_index(&metadata, "b"), None);
        assert_eq!(leaf_index(&metadata, "missing"), None);
    }

    #[test]
    fn widen_stats_keeps_the_column_stats_and_widens_the_bounds() {
        let logged = r#"{"numRecords":10,"minValues":{"id":1},"maxValues":{"id":10},"nullCount":{"id":0},"tightBounds":true}"#;
        let stats: serde_json::Value =
            serde_json::from_str(&widen_stats(logged, 10).unwrap()).unwrap();
        assert_eq!(
            stats,
            json!({
                "numRecords": 10,
                "minValues": {"id": 1},
                "maxValues": {"id": 10},
                "nullCount": {"id": 0},
                "tightBounds": false,
            })
        );
    }

    #[test]
    fn widen_stats_writes_the_physical_row_count() {
        let stats: serde_json::Value =
            serde_json::from_str(&widen_stats(r#"{"minValues":{"id":1}}"#, 7).unwrap()).unwrap();
        assert_eq!(
            stats,
            json!({"minValues": {"id": 1}, "numRecords": 7, "tightBounds": false})
        );
    }

    #[test]
    fn pick_bound_never_returns_nan() {
        let nan = Scalar::Double(f64::NAN);
        let one = Scalar::Double(1.0);

        let min = [nan.clone(), one.clone()]
            .into_iter()
            .fold(None, |acc, s| pick_bound(acc, Some(s), Ordering::Less));

        assert_eq!(min, Some(one));

        assert_eq!(pick_bound(None, Some(nan), Ordering::Greater), None);
    }

    /// A footer for the single-column `schema`: one two-row row group per entry, each carrying
    /// exactly the given Statistics.
    fn metadata_with_stats(
        schema: &Schema,
        row_groups: Vec<Option<Statistics>>,
    ) -> ParquetMetaData {
        let descr = Arc::new(ArrowSchemaConverter::new().convert(schema).unwrap());
        let row_groups: Vec<RowGroupMetaData> = row_groups
            .into_iter()
            .map(|stats| {
                let column = ColumnChunkMetaData::builder(descr.column(0));
                let column = match stats {
                    Some(stats) => column.set_statistics(stats),
                    None => column,
                };
                RowGroupMetaData::builder(descr.clone())
                    .set_num_rows(2)
                    .add_column_metadata(column.build().unwrap())
                    .build()
                    .unwrap()
            })
            .collect();
        let num_rows = 2 * row_groups.len() as i64;
        ParquetMetaData::new(
            FileMetaData::new(1, num_rows, None, None, descr, None),
            row_groups,
        )
    }

    fn int64_stats(min: i64, max: i64) -> Statistics {
        Statistics::int64(Some(min), Some(max), None, Some(0), false)
    }

    fn double_stats(min: f64, max: f64, nan_count: Option<u64>) -> Statistics {
        Statistics::Double(
            ValueStatistics::new(Some(min), Some(max), None, Some(0), false)
                .with_nan_count(nan_count),
        )
    }

    fn id_schema(data_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", data_type, true)]))
    }

    #[test]
    fn row_group_without_stats_drops_bounds() {
        let schema = id_schema(DataType::Int64);
        let metadata = metadata_with_stats(&schema, vec![Some(int64_stats(1, 2)), None]);

        let stats = parquet_metadata_to_delta_stats(&metadata, &schema);
        let (_, id) = &stats.columns[0];
        assert_eq!(id.min, None);
        assert_eq!(id.max, None);
    }

    #[test]
    fn watermark_errors_when_a_row_group_has_no_stats() {
        // the row group without stats could hold the true max
        let schema = id_schema(DataType::Int64);
        let metadata = metadata_with_stats(&schema, vec![None, Some(int64_stats(1, 2))]);
        let source_files = vec![("a.parquet".to_string(), metadata, 0u64)];

        assert!(max_stat_as_i64(&source_files, "id").is_err());
    }

    #[test]
    fn key_bounds_are_none_when_a_row_group_has_no_stats() {
        let schema = id_schema(DataType::Int64);
        let metadata = metadata_with_stats(&schema, vec![None, Some(int64_stats(1, 2))]);
        let source_files = vec![("a.parquet".to_string(), metadata, 0u64)];

        assert_eq!(key_bounds_as_scalars(&source_files, "id", &schema), None);
    }

    #[test]
    fn unknown_null_count_is_left_out_of_the_json() {
        let schema = id_schema(DataType::Int64);
        let no_null_count = Statistics::int64(Some(1), Some(2), None, None, false);
        let metadata = metadata_with_stats(&schema, vec![Some(no_null_count)]);

        let stats = parquet_metadata_to_delta_stats(&metadata, &schema);
        let json: serde_json::Value =
            serde_json::from_str(&delta_stats_to_json(&stats, &schema).unwrap()).unwrap();
        assert_eq!(json["nullCount"], json!({}));
    }

    /// The add stats of a single float column `x` holding `values`.
    fn float_stats(values: arrow_array::ArrayRef) -> ColumnStats {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            values.data_type().clone(),
            true,
        )]));
        let batch = arrow_array::RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        let metadata = metadata_from_batches(&[&batch]);
        parquet_metadata_to_delta_stats(&metadata, &schema)
            .columns
            .remove(0)
            .1
    }

    fn is_nan_bound(bound: &Option<Scalar>) -> bool {
        bound.as_ref().is_some_and(is_nan)
    }

    #[test]
    fn float_without_nan_keeps_its_bounds() {
        let stats = float_stats(Arc::new(Float64Array::from(vec![1.0, 2.0])));
        assert_eq!(stats.min, Some(Scalar::Double(1.0)));
        assert_eq!(stats.max, Some(Scalar::Double(2.0)));
    }

    #[test]
    fn nan_makes_the_max_nan() {
        let stats = float_stats(Arc::new(Float64Array::from(vec![1.0, 2.0, f64::NAN])));
        assert_eq!(stats.min, Some(Scalar::Double(1.0)));
        assert!(is_nan_bound(&stats.max), "max: {:?}", stats.max);
    }

    #[test]
    fn all_nan_makes_both_bounds_nan() {
        let stats = float_stats(Arc::new(Float64Array::from(vec![f64::NAN, f64::NAN])));
        assert!(is_nan_bound(&stats.min), "min: {:?}", stats.min);
        assert!(is_nan_bound(&stats.max), "max: {:?}", stats.max);
    }

    #[test]
    fn nan_with_nulls_keeps_the_real_min() {
        let stats = float_stats(Arc::new(Float64Array::from(vec![
            Some(1.0),
            None,
            Some(f64::NAN),
        ])));
        assert_eq!(stats.min, Some(Scalar::Double(1.0)));
        assert!(is_nan_bound(&stats.max), "max: {:?}", stats.max);
    }

    #[test]
    fn nulls_do_not_count_as_values_beside_nan() {
        // the only value is NaN, so both bounds are NaN despite the null row
        let stats = float_stats(Arc::new(Float64Array::from(vec![None, Some(f64::NAN)])));
        assert!(is_nan_bound(&stats.min), "min: {:?}", stats.min);
        assert!(is_nan_bound(&stats.max), "max: {:?}", stats.max);
    }

    #[test]
    fn float32_nan_bound_is_a_float_scalar() {
        let stats = float_stats(Arc::new(Float32Array::from(vec![1.0, f32::NAN])));
        assert_eq!(stats.min, Some(Scalar::Float(1.0)));
        assert!(
            matches!(stats.max, Some(Scalar::Float(v)) if v.is_nan()),
            "max: {:?}",
            stats.max
        );
    }

    fn float_key_bounds(nan_count: Option<u64>) -> Option<(Scalar, Scalar)> {
        let schema = id_schema(DataType::Float64);
        let metadata = metadata_with_stats(&schema, vec![Some(double_stats(1.0, 2.0, nan_count))]);
        let source_files = vec![("a.parquet".to_string(), metadata, 0u64)];
        key_bounds_as_scalars(&source_files, "id", &schema)
    }

    #[test]
    fn key_bounds_are_none_when_the_source_holds_nan() {
        assert_eq!(float_key_bounds(Some(1)), None);
    }

    #[test]
    fn key_bounds_cover_a_float_key_without_nan() {
        assert_eq!(
            float_key_bounds(Some(0)),
            Some((Scalar::Double(1.0), Scalar::Double(2.0)))
        );
    }

    #[test]
    fn key_bounds_are_none_when_the_nan_count_is_unknown() {
        assert_eq!(float_key_bounds(None), None);
    }
}
