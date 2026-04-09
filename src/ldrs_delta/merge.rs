use std::collections::HashSet;
use std::sync::Arc;

use arrow::row::{RowConverter, SortField};
use arrow_array::ArrayRef;
use arrow_schema::SchemaRef;
use futures::StreamExt;
use object_store::ObjectStore;
use parquet::file::metadata::ParquetMetaData;

use crate::ldrs_parquet::stream_projected_parquet;

pub struct MergeConfig {
    pub merge_keys: Vec<String>,
    pub allow_null_keys: bool,
    pub max_rows: Option<usize>,
    pub max_bytes: Option<usize>,
    pub txn_config: TxnConfig,
}

pub enum TxnConfig {
    SourceWatermark {
        app_id: String,
        watermark_column: String,
    },
    ProcessingTime {
        app_id: String,
        batch_version: Option<i64>,
    },
    None,
}

pub struct MergeStats {
    pub source_rows: usize,
    pub matched_rows: usize,
    pub files_scanned: usize,
    pub files_with_dvs: usize,
    pub files_written: usize,
    pub skipped: bool,
}

impl MergeStats {
    pub fn empty() -> Self {
        MergeStats {
            source_rows: 0,
            matched_rows: 0,
            files_scanned: 0,
            files_with_dvs: 0,
            files_written: 0,
            skipped: false,
        }
    }

    pub fn skipped() -> Self {
        MergeStats {
            source_rows: 0,
            matched_rows: 0,
            files_scanned: 0,
            files_with_dvs: 0,
            files_written: 0,
            skipped: true,
        }
    }
}

pub(crate) async fn build_key_set(
    store: Arc<dyn ObjectStore>,
    base_path: &object_store::path::Path,
    source_files: &[(String, ParquetMetaData)],
    merge_keys: &[String],
    schema: &SchemaRef,
) -> Result<(HashSet<Vec<u8>>, RowConverter), anyhow::Error> {
    let key_fields: Vec<SortField> = merge_keys
        .iter()
        .map(|k| {
            let field = schema.field_with_name(k)?;
            Ok(SortField::new(field.data_type().clone()))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    let converter = RowConverter::new(key_fields)?;

    let mut key_set: HashSet<Vec<u8>> = HashSet::new();

    for (filename, _) in source_files {
        let path = base_path.child(filename.as_str());
        let mut stream =
            stream_projected_parquet(store.clone(), &path, merge_keys, None).await?;

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let key_columns: Vec<ArrayRef> = merge_keys
                .iter()
                .map(|k| batch.column_by_name(k).unwrap().clone())
                .collect();
            let rows = converter.convert_columns(&key_columns)?;
            for i in 0..batch.num_rows() {
                key_set.insert(rows.row(i).as_ref().to_vec());
            }
        }
    }

    Ok((key_set, converter))
}

pub(crate) fn validate_no_null_keys(
    source_files: &[(String, ParquetMetaData)],
    merge_keys: &[String],
    schema: &SchemaRef,
) -> Result<(), anyhow::Error> {
    let null_key = source_files
        .iter()
        .flat_map(|(_, metadata)| metadata.row_groups())
        .flat_map(|rg| {
            merge_keys.iter().map(move |key_name| {
                let col_idx = schema.index_of(key_name).unwrap();
                let null_count = rg
                    .column(col_idx)
                    .statistics()
                    .and_then(|s| s.null_count_opt())
                    .unwrap_or(0);
                (key_name.as_str(), null_count)
            })
        })
        .find(|(_, count)| *count > 0);

    if let Some((col, count)) = null_key {
        anyhow::bail!("merge key '{}' contains {} null values", col, count);
    }
    Ok(())
}

pub(crate) fn key_bounds_from_parquet_stats(
    source_files: &[(String, ParquetMetaData)],
    key_col_name: &str,
    schema: &SchemaRef,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let col_idx = schema.index_of(key_col_name).ok()?;

    source_files
        .iter()
        .flat_map(|(_, metadata)| metadata.row_groups())
        .fold(None, |acc, rg| {
            let stats = rg.column(col_idx).statistics()?;
            let rg_min = stats.min_bytes_opt()?;
            let rg_max = stats.max_bytes_opt()?;
            match acc {
                None => Some((rg_min.to_vec(), rg_max.to_vec())),
                Some((prev_min, prev_max)) => Some((
                    if rg_min < prev_min.as_slice() {
                        rg_min.to_vec()
                    } else {
                        prev_min
                    },
                    if rg_max > prev_max.as_slice() {
                        rg_max.to_vec()
                    } else {
                        prev_max
                    },
                )),
            }
        })
}

pub(crate) fn select_row_groups(
    metadata: &ParquetMetaData,
    key_col_name: &str,
    schema: &SchemaRef,
    min_key: &[u8],
    max_key: &[u8],
) -> Vec<usize> {
    let col_idx = match schema.index_of(key_col_name) {
        Ok(idx) => idx,
        Err(_) => return (0..metadata.num_row_groups()).collect(),
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
            match (stats.min_bytes_opt(), stats.max_bytes_opt()) {
                (Some(rg_min), Some(rg_max)) => rg_max >= min_key && rg_min <= max_key,
                _ => true,
            }
        })
        .map(|(i, _)| i)
        .collect()
}
