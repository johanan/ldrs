use std::collections::HashSet;
use std::sync::Arc;

use arrow::row::{RowConverter, SortField};
use arrow_array::{cast::AsArray, types::Int64Type, ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use futures::{Stream, StreamExt};
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::file::metadata::ParquetMetaData;
use uuid::Uuid;

use std::collections::HashMap;

use super::dv::{build_dv_file, build_dv_inline, serialize_dv};
use super::stats::{
    delta_stats_to_json, key_bounds_as_scalars, max_stat_as_i64, parquet_metadata_to_delta_stats,
    select_row_groups_by_scalars,
};
use crate::arrow_access::arrow_transforms::ArrowColumnTransformStrategy;
use crate::ldrs_delta::{
    build_add, build_commit_jsonl, build_engine, build_metadata, ensure_table, merge_protocol,
    version_to_log_filename, DeltaAction, DeltaCommitInfo, DeltaRemove, DeltaTxn,
    DEFAULT_MAX_BYTES, DEFAULT_MAX_ROWS, MERGE_MAX_RETRIES,
};
use crate::ldrs_parquet::{
    default_writer_props, read_parquet_metadata, stream_projected_parquet, with_bloom_filters,
    write_parquet_split, ROW_NUMBER_COLUMN,
};
use crate::storage::{base_or_relative_path, build_store};
use delta_kernel::expressions::{Expression as Expr, Predicate as Pred};
use delta_kernel::scan::state::ScanFile;
use delta_kernel::Snapshot;
use object_store::{PutMode, PutOptions, PutPayload};
use roaring::RoaringTreemap;

#[derive(Clone)]
pub struct MergeConfig {
    pub merge_keys: Vec<String>,
    pub allow_null_keys: bool,
    pub max_rows: Option<usize>,
    pub max_bytes: Option<usize>,
    pub txn_config: TxnConfig,
}

#[derive(Clone)]
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
    /// Target rows marked for deletion via DVs. Assumes merge keys are unique
    /// in the target: if a target has duplicate keys, one source row may delete
    /// multiple target rows, causing `matched_rows` to exceed `source_rows` and
    /// making `inserted_rows` a lower bound rather than exact.
    pub matched_rows: usize,
    /// `source_rows - matched_rows`, saturated at 0. Exact only when merge keys
    /// are unique in the target (see `matched_rows`).
    pub inserted_rows: usize,
    pub files_scanned: usize,
    pub files_with_dvs: usize,
    pub files_written: usize,
    pub skipped: bool,
    pub skipped_version: Option<i64>,
}

impl MergeStats {
    pub fn empty() -> Self {
        MergeStats {
            source_rows: 0,
            matched_rows: 0,
            inserted_rows: 0,
            files_scanned: 0,
            files_with_dvs: 0,
            files_written: 0,
            skipped: false,
            skipped_version: None,
        }
    }

    pub fn skipped(existing_version: i64) -> Self {
        MergeStats {
            skipped: true,
            skipped_version: Some(existing_version),
            ..Self::empty()
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

    let total_rows: usize = source_files
        .iter()
        .map(|(_, m)| m.file_metadata().num_rows() as usize)
        .sum();
    let mut key_set: HashSet<Vec<u8>> = HashSet::with_capacity(total_rows);

    for (filename, _) in source_files {
        let path = base_path.clone().join(filename.as_str());
        let mut stream = stream_projected_parquet(store.clone(), &path, merge_keys, None).await?;

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
    // Resolve indices up-front — fails with a clean error if a merge key
    // isn't in the schema (instead of panicking inside the iterator).
    let key_indices: Vec<(usize, &str)> = merge_keys
        .iter()
        .map(|k| schema.index_of(k).map(|idx| (idx, k.as_str())))
        .collect::<Result<Vec<_>, _>>()?;

    let null_key = source_files
        .iter()
        .flat_map(|(_, metadata)| metadata.row_groups())
        .flat_map(|rg| {
            key_indices.iter().map(move |(idx, name)| {
                let null_count = rg
                    .column(*idx)
                    .statistics()
                    .and_then(|s| s.null_count_opt())
                    .unwrap_or(0);
                (*name, null_count)
            })
        })
        .find(|(_, count)| *count > 0);

    if let Some((col, count)) = null_key {
        anyhow::bail!("merge key '{}' contains {} null values", col, count);
    }
    Ok(())
}

pub async fn merge_delta<S>(
    table_path: &str,
    schema: SchemaRef,
    transforms: Vec<Option<ArrowColumnTransformStrategy>>,
    stream: S,
    merge_config: MergeConfig,
) -> Result<MergeStats, anyhow::Error>
where
    S: Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + 'static,
{
    ensure_table(table_path, &schema).await?;

    let url = base_or_relative_path(table_path)?;
    let (store, base_path, _) = build_store(&url)?;

    let bloom_columns: Vec<Vec<String>> = merge_config
        .merge_keys
        .iter()
        .map(|k| vec![k.clone()])
        .collect();
    let props = with_bloom_filters(default_writer_props(), bloom_columns);

    let source_files = write_parquet_split(
        table_path,
        schema.clone(),
        transforms,
        stream,
        Some(merge_config.max_rows.unwrap_or(DEFAULT_MAX_ROWS)),
        Some(merge_config.max_bytes.unwrap_or(DEFAULT_MAX_BYTES)),
        |_| format!("{}.parquet", Uuid::new_v4()),
        Some(props),
    )
    .await?;

    if source_files.is_empty() {
        return Ok(MergeStats::empty());
    }

    if !merge_config.allow_null_keys {
        if let Err(e) = validate_no_null_keys(&source_files, &merge_config.merge_keys, &schema) {
            cleanup_source_files(&store, &base_path, &source_files).await;
            return Err(e);
        }
    }

    let (key_set, converter) = build_key_set(
        store.clone(),
        &base_path,
        &source_files,
        &merge_config.merge_keys,
        &schema,
    )
    .await?;

    let engine = build_engine(store.clone());

    // get the app_id and batch_version from either
    // watermark column (read written parquets and get max)
    // or an abitrary value that defaults to now
    // or None
    let (app_id, batch_version) =
        compute_batch_version(&merge_config.txn_config, &source_files, &schema)?;

    // Retry loop
    for _attempt in 0..MERGE_MAX_RETRIES {
        // get a fresh snapshot, this is either the first pass or we failed and we need fresh metadata
        let snapshot = Snapshot::builder_for(url.clone()).build(engine.as_ref())?;
        let version = snapshot.version();
        let metadata = snapshot.table_configuration().metadata();
        let table_id = metadata.id().to_string();
        let created_time = metadata.created_time();
        let has_deletion_vectors = snapshot
            .table_properties()
            .enable_deletion_vectors
            .unwrap_or(false);

        if let (Some(app_id), Some(batch_version)) = (app_id.as_ref(), batch_version) {
            if let Ok(Some(last_version)) = snapshot.get_app_id_version(app_id, engine.as_ref()) {
                // if there is a newer version committed, nothing to do so we clean up and exit
                if last_version >= batch_version {
                    cleanup_source_files(&store, &base_path, &source_files).await;
                    return Ok(MergeStats::skipped(last_version));
                }
            }
        }

        // find candidate target files whose stats overlap the source's key range on the first merge key
        let candidate_files = find_candidate_target_files(
            &snapshot,
            engine.as_ref(),
            &schema,
            &source_files,
            &merge_config.merge_keys[0],
        )?;

        let file_probes = narrow_to_eligible_row_groups(
            &candidate_files,
            &store,
            &base_path,
            &schema,
            &source_files,
            &merge_config.merge_keys,
        )
        .await?;

        let (file_matches, matched_rows) = probe_targets_for_matches(
            &file_probes,
            &store,
            &base_path,
            engine.as_ref(),
            &url,
            &merge_config.merge_keys,
            &key_set,
            &converter,
        )
        .await?;

        // Build commit and write atomically
        let now = chrono::Utc::now().timestamp_millis();

        let commit_info = DeltaCommitInfo {
            timestamp: now,
            operation: "MERGE".to_string(),
            operation_parameters: HashMap::from([("mode".into(), "Merge".into())]),
            engine_info: format!("ldrs-{}", env!("CARGO_PKG_VERSION")),
        };

        // Protocol + metadata upgrade only on first DV table
        let needs_dv_upgrade = !has_deletion_vectors;
        let protocol = if needs_dv_upgrade {
            Some(merge_protocol())
        } else {
            None
        };
        let metadata_action = if needs_dv_upgrade {
            Some(build_metadata(
                &schema,
                Some(&table_id),
                created_time,
                HashMap::from([("delta.enableDeletionVectors".into(), "true".into())]),
            )?)
        } else {
            None
        };

        let txn = match (app_id.as_ref(), batch_version) {
            (Some(app_id), Some(batch_version)) => Some(DeltaTxn {
                app_id: app_id.clone(),
                version: batch_version,
                last_updated: Some(now),
            }),
            _ => None,
        };

        // Removes must include the existing DV descriptor (if any) to tombstone the correct add
        let paths_of_interest: HashSet<&str> = file_matches
            .iter()
            .map(|fm| fm.scan_file.path.as_str())
            .collect();
        let existing_dvs = load_existing_dvs(&store, &base_path, &paths_of_interest).await?;

        let removes: Vec<DeltaRemove> = file_matches
            .iter()
            .map(|fm| DeltaRemove {
                path: fm.scan_file.path.clone(),
                deletion_timestamp: now,
                data_change: true,
                size: fm.scan_file.size,
                deletion_vector: existing_dvs.get(&fm.scan_file.path).cloned(),
            })
            .collect();

        const DV_INLINE_THRESHOLD: usize = 1024;

        let mut adds_with_dvs = Vec::with_capacity(file_matches.len());
        for fm in &file_matches {
            let dv_bytes = serialize_dv(&fm.deleted_rows);
            let cardinality = fm.deleted_rows.len() as i64;
            let descriptor = if dv_bytes.len() <= DV_INLINE_THRESHOLD {
                build_dv_inline(&dv_bytes, cardinality)
            } else {
                build_dv_file(store.as_ref(), &base_path, &dv_bytes, cardinality).await?
            };

            let file_stats = parquet_metadata_to_delta_stats(&fm.metadata, &schema);
            let stats_json = delta_stats_to_json(&file_stats, &schema)?;

            adds_with_dvs.push(super::DeltaAdd {
                path: fm.scan_file.path.clone(),
                partition_values: HashMap::new(),
                size: fm.scan_file.size,
                modification_time: fm.scan_file.modification_time,
                data_change: true,
                stats: Some(stats_json),
                deletion_vector: Some(descriptor),
            });
        }

        // Add actions for new source files
        let file_paths: Vec<_> = source_files
            .iter()
            .map(|(filename, _)| base_path.clone().join(filename.as_str()))
            .collect();

        let obj_metas =
            futures::future::try_join_all(file_paths.iter().map(|path| store.head(path))).await?;

        let new_adds = source_files
            .iter()
            .zip(obj_metas.iter())
            .map(|((filename, metadata), obj_meta)| {
                build_add(filename, metadata, obj_meta, &schema)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut actions: Vec<DeltaAction> = vec![DeltaAction::CommitInfo(&commit_info)];
        if let Some(txn) = txn.as_ref() {
            actions.push(DeltaAction::Txn(txn));
        }
        if let Some(p) = protocol.as_ref() {
            actions.push(DeltaAction::Protocol(p));
        }
        if let Some(m) = metadata_action.as_ref() {
            actions.push(DeltaAction::MetaData(m));
        }
        actions.extend(removes.iter().map(DeltaAction::Remove));
        actions.extend(adds_with_dvs.iter().map(DeltaAction::Add));
        actions.extend(new_adds.iter().map(DeltaAction::Add));

        let commit_body = build_commit_jsonl(&actions)?;
        let next_version = version + 1;
        let log_path = base_path
            .clone()
            .join("_delta_log")
            .clone()
            .join(version_to_log_filename(next_version));

        match store
            .put_opts(
                &log_path,
                PutPayload::from(commit_body),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {
                let source_rows: usize = source_files
                    .iter()
                    .map(|(_, m)| m.file_metadata().num_rows() as usize)
                    .sum();
                return Ok(MergeStats {
                    source_rows,
                    matched_rows,
                    inserted_rows: source_rows.saturating_sub(matched_rows),
                    files_scanned: candidate_files.len(),
                    files_with_dvs: file_matches.len(),
                    files_written: source_files.len(),
                    skipped: false,
                    skipped_version: None,
                });
            }
            Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }

    // Retry exhaustion: clean up orphaned source files
    cleanup_source_files(&store, &base_path, &source_files).await;
    anyhow::bail!(
        "failed to commit delta merge after {} attempts",
        MERGE_MAX_RETRIES
    )
}

struct FileProbe {
    scan_file: ScanFile,
    metadata: Arc<ParquetMetaData>,
    eligible_row_groups: Vec<usize>,
}

struct FileMatch {
    scan_file: ScanFile,
    metadata: Arc<ParquetMetaData>,
    deleted_rows: RoaringTreemap,
}

/// Stream key columns from eligible row groups, probe against the source key set,
/// and build a union deletion vector per matched target file. Returns matched files
/// and the total count of newly-deleted rows across all files.
async fn probe_targets_for_matches(
    probes: &[FileProbe],
    store: &Arc<dyn ObjectStore>,
    base_path: &object_store::path::Path,
    engine: &dyn delta_kernel::Engine,
    table_url: &url::Url,
    merge_keys: &[String],
    key_set: &HashSet<Vec<u8>>,
    converter: &RowConverter,
) -> Result<(Vec<FileMatch>, usize), anyhow::Error> {
    let mut file_matches: Vec<FileMatch> = Vec::new();
    let mut matched_rows: usize = 0;

    for probe in probes {
        let path = base_path.clone().join(probe.scan_file.path.as_str());

        let mut deleted_rows = if probe.scan_file.dv_info.has_vector() {
            probe
                .scan_file
                .dv_info
                .get_row_indexes(engine, table_url)?
                .map(RoaringTreemap::from_iter)
                .unwrap_or_default()
        } else {
            RoaringTreemap::new()
        };

        let original_len = deleted_rows.len();

        let mut stream = stream_projected_parquet(
            store.clone(),
            &path,
            merge_keys,
            Some(probe.eligible_row_groups.clone()),
        )
        .await?;

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let row_numbers = batch
                .column_by_name(ROW_NUMBER_COLUMN)
                .unwrap()
                .as_primitive::<Int64Type>();

            let key_columns: Vec<ArrayRef> = merge_keys
                .iter()
                .map(|k| batch.column_by_name(k).unwrap().clone())
                .collect();
            let rows = converter.convert_columns(&key_columns)?;

            for i in 0..batch.num_rows() {
                let physical_idx = row_numbers.value(i) as u64;

                if deleted_rows.contains(physical_idx) {
                    continue;
                }

                let row = rows.row(i);
                if key_set.contains(row.as_ref()) {
                    deleted_rows.insert(physical_idx);
                }
            }
        }

        let new_matches = (deleted_rows.len() - original_len) as usize;
        if new_matches > 0 {
            matched_rows += new_matches;
            file_matches.push(FileMatch {
                scan_file: probe.scan_file.clone(),
                metadata: probe.metadata.clone(),
                deleted_rows,
            });
        }
    }

    Ok((file_matches, matched_rows))
}

/// For each candidate target file, filter down to row groups whose stats overlap
/// the source's key range on ALL merge keys. Reads only parquet footers (no data).
async fn narrow_to_eligible_row_groups(
    candidates: &[ScanFile],
    store: &Arc<dyn ObjectStore>,
    base_path: &object_store::path::Path,
    schema: &SchemaRef,
    source_files: &[(String, ParquetMetaData)],
    merge_keys: &[String],
) -> Result<Vec<FileProbe>, anyhow::Error> {
    let mut file_probes: Vec<FileProbe> = Vec::new();

    for candidate in candidates {
        let path = base_path.clone().join(candidate.path.as_str());
        let metadata = read_parquet_metadata(store.clone(), &path).await?;

        let mut eligible_rgs: Vec<usize> = (0..metadata.num_row_groups()).collect();

        for key_name in merge_keys {
            if let Some((min_scalar, max_scalar)) =
                key_bounds_as_scalars(source_files, key_name, schema)
            {
                let key_eligible = select_row_groups_by_scalars(
                    &metadata,
                    key_name,
                    schema,
                    &min_scalar,
                    &max_scalar,
                );
                eligible_rgs.retain(|rg| key_eligible.contains(rg));
                if eligible_rgs.is_empty() {
                    break;
                }
            }
        }
        if eligible_rgs.is_empty() {
            continue;
        }

        file_probes.push(FileProbe {
            scan_file: candidate.clone(),
            metadata,
            eligible_row_groups: eligible_rgs,
        });
    }

    Ok(file_probes)
}

/// Compute the app-provided txn version from the merge configuration.
/// For SourceWatermark, the version is the max of the watermark column in source stats (free — no I/O).
/// For ProcessingTime, it is the caller's explicit version or wall-clock millis.
fn compute_batch_version(
    txn_config: &TxnConfig,
    source_files: &[(String, ParquetMetaData)],
    schema: &SchemaRef,
) -> Result<(Option<String>, Option<i64>), anyhow::Error> {
    match txn_config {
        TxnConfig::SourceWatermark {
            app_id,
            watermark_column,
        } => {
            let wm = max_stat_as_i64(source_files, watermark_column, schema)?;
            Ok((Some(app_id.clone()), Some(wm)))
        }
        TxnConfig::ProcessingTime {
            app_id,
            batch_version,
        } => {
            let v = batch_version.unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
            Ok((Some(app_id.clone()), Some(v)))
        }
        TxnConfig::None => Ok((None, None)),
    }
}

/// Find target files whose stats overlap the source's key range on the first merge key.
/// Uses a delta-kernel predicate scan — file-level skipping based on min/max stats.
fn find_candidate_target_files(
    snapshot: &delta_kernel::SnapshotRef,
    engine: &dyn delta_kernel::Engine,
    schema: &SchemaRef,
    source_files: &[(String, ParquetMetaData)],
    first_key: &str,
) -> Result<Vec<ScanFile>, anyhow::Error> {
    let scalar_bounds = key_bounds_as_scalars(source_files, first_key, schema);

    let scan = if let Some((min_scalar, max_scalar)) = scalar_bounds {
        let col = Expr::column([first_key]);
        let pred = Pred::and(
            col.clone().ge(Expr::literal(min_scalar)),
            col.le(Expr::literal(max_scalar)),
        );
        snapshot
            .clone()
            .scan_builder()
            .with_predicate(Arc::new(pred))
            .build()?
    } else {
        snapshot.clone().scan_builder().build()?
    };

    let mut candidate_files: Vec<ScanFile> = Vec::new();
    for scan_meta in scan.scan_metadata(engine)? {
        let scan_meta = scan_meta?;
        fn collect(acc: &mut Vec<ScanFile>, file: ScanFile) {
            acc.push(file);
        }
        candidate_files = scan_meta.visit_scan_files(candidate_files, collect)?;
    }
    Ok(candidate_files)
}

async fn cleanup_source_files(
    store: &Arc<dyn ObjectStore>,
    base_path: &object_store::path::Path,
    source_files: &[(String, ParquetMetaData)],
) {
    for (filename, _) in source_files {
        let path = base_path.clone().join(filename.as_str());
        let _ = store.delete(&path).await;
    }
}

/// Walk the delta log in reverse, finding the latest `add` action for each path of interest.
/// Returns a map from path to its current DV descriptor (only entries for paths that have a DV).
/// Paths whose latest add had no DV are absent from the returned map.
/// Does not handle checkpoints. To be added later
async fn load_existing_dvs(
    store: &Arc<dyn ObjectStore>,
    base_path: &object_store::path::Path,
    paths_of_interest: &HashSet<&str>,
) -> Result<HashMap<String, super::dv::DeletionVectorDescriptor>, anyhow::Error> {
    use futures::TryStreamExt;

    let log_dir = base_path.clone().join("_delta_log");
    let mut log_files: Vec<object_store::ObjectMeta> = store
        .list(Some(&log_dir))
        .try_filter(|m| {
            let is_json = m.location.as_ref().ends_with(".json");
            async move { is_json }
        })
        .try_collect()
        .await?;
    // reverse sort: newest first
    log_files.sort_by(|a, b| b.location.as_ref().cmp(a.location.as_ref()));

    let mut found: HashMap<String, super::dv::DeletionVectorDescriptor> = HashMap::new();
    let mut paths_remaining: HashSet<&str> = paths_of_interest.clone();

    for log_file in log_files {
        if paths_remaining.is_empty() {
            break;
        }
        let bytes = store.get(&log_file.location).await?.bytes().await?;
        let content = std::str::from_utf8(&bytes)?;

        for line in content.lines() {
            let action: serde_json::Value = serde_json::from_str(line)?;
            if let Some(add) = action.get("add") {
                let path = add["path"].as_str().unwrap_or("");
                if paths_remaining.contains(path) {
                    if let Some(dv) = add.get("deletionVector") {
                        let dv_desc: super::dv::DeletionVectorDescriptor =
                            serde_json::from_value(dv.clone())?;
                        found.insert(path.to_string(), dv_desc);
                    }
                    // Found the latest add for this path — stop tracking either way
                    paths_remaining.remove(path);
                }
            }
        }
    }

    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::Int64Builder;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::ParquetMetaDataReader;

    fn two_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn metadata_from_batch(batch: &RecordBatch) -> ParquetMetaData {
        let mut buf: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        let bytes = Bytes::from(buf);
        ParquetMetaDataReader::new()
            .parse_and_finish(&bytes)
            .unwrap()
    }

    #[test]
    fn validate_no_null_keys_ok_when_no_nulls() {
        let schema = two_col_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let source_files = vec![("test.parquet".to_string(), metadata_from_batch(&batch))];
        validate_no_null_keys(&source_files, &["id".to_string()], &schema).unwrap();
    }

    #[test]
    fn validate_no_null_keys_errors_on_nulls() {
        let schema = two_col_schema();
        let mut ids = Int64Builder::new();
        ids.append_value(1);
        ids.append_null();
        ids.append_value(3);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ids.finish()),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let source_files = vec![("test.parquet".to_string(), metadata_from_batch(&batch))];

        let err = validate_no_null_keys(&source_files, &["id".to_string()], &schema)
            .expect_err("should error on null keys");
        let msg = err.to_string();
        assert!(msg.contains("merge key 'id'"), "got: {msg}");
        assert!(msg.contains("null"), "got: {msg}");
    }

    #[test]
    fn validate_no_null_keys_errors_on_unknown_key_name() {
        let schema = two_col_schema();
        // An unknown merge key must return Err (not panic). No parquet metadata needed —
        // the key-to-index resolution runs first.
        let err = validate_no_null_keys(&[], &["does_not_exist".to_string()], &schema)
            .expect_err("should error on unknown merge key");
        assert!(
            err.to_string().contains("does_not_exist"),
            "error should mention the missing column; got: {err}"
        );
    }
}
