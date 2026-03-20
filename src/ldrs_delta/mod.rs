use std::cmp::Ordering;

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
