use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;

use anyhow::Context;
use arrow::compute::filter_record_batch;
use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::BooleanArray;
use arrow_schema::{DataType, SchemaRef};
use delta_kernel::expressions::Scalar;
use delta_kernel::scan::state::{DvInfo, ScanFile};
use delta_kernel::schema::{ColumnMetadataKey, MetadataValue};
use delta_kernel::table_features::TableFeature;
use delta_kernel::{Engine, Snapshot, Version};
use futures::{stream, StreamExt, TryStreamExt};
use ldrs_parquet::{read_parquet_metadata, stream_parquet, ParquetSink};
use ldrs_storage::{base_or_relative_path, build_store, store_path_from_uri};
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::metadata::ParquetMetaData;
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::features::{
    check_writer_version, files_are_enumerable, rewrite_is_supported, writer_features,
};
use crate::stats::{pick_bound, stats_to_scalars};
use crate::{
    build_add, build_engine, file_path, partition_values, snapshot_table_state,
    version_to_log_filename, Commit, DeltaRemove, Operation, OperationConfig, TableState,
};

const FOOTER_CONCURRENCY: usize = 16;

const DEFAULT_TARGET_FILE_SIZE: u64 = 256 * 1024 * 1024;

pub async fn plan_optimize(
    table_path: &str,
    target_size: Option<u64>,
    order_column: Option<&str>,
    cloud_io: &Handle,
) -> Result<OptimizePlan, anyhow::Error> {
    let url = base_or_relative_path(table_path)?;
    let (store, base_path, _) = build_store(&url)?;
    let engine = build_engine(store.clone(), cloud_io);
    let mut state = snapshot_table_state(engine.as_ref(), &url)?;

    refuse_unsupported_features(&state.snapshot).context("cannot optimize this table")?;

    let target_size = target_size
        .or_else(|| {
            state
                .snapshot
                .table_properties()
                .target_file_size
                .map(|size| size.get())
        })
        .unwrap_or(DEFAULT_TARGET_FILE_SIZE);

    // `min_of` searches a file by the name the column is written under, so resolve once here.
    let physical_order = order_column
        .map(|column| physical_name(&state.snapshot, column))
        .transpose()?;

    let files = candidates(std::mem::take(&mut state.active_files), target_size);
    let candidates = read_candidates(
        files,
        store,
        &base_path,
        physical_order.as_deref(),
        cloud_io,
    )
    .await?;

    let unordered = !candidates.is_empty() && candidates.iter().all(|c| c.order_min.is_none());
    if let Some(column) = order_column.filter(|_| unordered) {
        tracing::warn!(
            "no file reports a minimum for '{column}', so the rewrite cannot pack by it"
        );
    }

    let declared = state
        .snapshot
        .table_configuration()
        .metadata()
        .partition_columns()
        .to_vec();
    let bins = plan_bins(candidates, target_size, &declared)?;
    Ok(OptimizePlan { url, state, bins })
}

pub struct OptimizePlan {
    url: url::Url,
    state: TableState,
    bins: Vec<Bin>,
}

impl OptimizePlan {
    pub fn bins(&self) -> &[Bin] {
        &self.bins
    }

    pub fn is_empty(&self) -> bool {
        self.bins.is_empty()
    }
}

#[derive(Debug, Default)]
pub struct OptimizeOutcome {
    pub files_added: usize,
    pub files_removed: usize,
    pub bytes_added: u64,
    pub bytes_removed: u64,
    pub deletion_vectors_removed: usize,
    pub version: Option<Version>,
    pub skipped: bool,
}

impl OptimizeOutcome {
    /// Spelled as Spark spells them
    fn metrics(&self) -> HashMap<String, String> {
        HashMap::from([
            ("numAddedFiles".into(), self.files_added.to_string()),
            ("numRemovedFiles".into(), self.files_removed.to_string()),
            ("numAddedBytes".into(), self.bytes_added.to_string()),
            ("numRemovedBytes".into(), self.bytes_removed.to_string()),
            (
                "numDeletionVectorsRemoved".into(),
                self.deletion_vectors_removed.to_string(),
            ),
        ])
    }
}

/// Rewrite every bin, then commit the removes and adds as one version.
pub async fn execute_plan(
    plan: OptimizePlan,
    config: &OperationConfig,
    cloud_io: &Handle,
) -> Result<OptimizeOutcome, anyhow::Error> {
    if plan.bins.is_empty() {
        return Ok(OptimizeOutcome {
            skipped: true,
            ..Default::default()
        });
    }

    let (store, base_path, _) = build_store(&plan.url)?;
    let engine = build_engine(store.clone(), cloud_io);
    let now = chrono::Utc::now().timestamp_millis();

    let mut outcome = OptimizeOutcome::default();
    let mut adds = Vec::new();
    let mut removes = Vec::new();
    let remove = DeltaRemove::from_scan_file(now, &plan.state.existing_dvs);

    for bin in &plan.bins {
        let written = rewrite_bin(
            bin,
            store.clone(),
            &base_path,
            plan.state.snapshot.table_root(),
            engine.as_ref(),
            cloud_io,
        )
        .await?;

        if let Some((_, metadata, size)) = written {
            adds.push(build_add(
                &bin.output,
                &metadata,
                size,
                now,
                &bin.schema,
                bin.partition_values.clone(),
            )?);
            outcome.files_added += 1;
            outcome.bytes_added += size;
        }

        for file in &bin.files {
            outcome.bytes_removed += file.size.max(0) as u64;
            outcome.deletion_vectors_removed += usize::from(file.dv_info.has_vector());
            removes.push(remove(file.clone())?);
        }
    }
    outcome.files_removed = removes.len();

    let commit_body = Commit::for_maintenance(
        Operation::Optimize,
        &plan.state.snapshot,
        config,
        engine.as_ref(),
        now,
    )?
    .with_metrics(outcome.metrics())
    .with_removes(removes)
    .with_adds(adds)
    .to_jsonl()?;

    let next_version = plan.state.version + 1;
    let log_path = base_path
        .clone()
        .join("_delta_log")
        .join(version_to_log_filename(next_version));
    let orphans = "the files this run wrote are unreferenced; a vacuum past the retention window reclaims them";
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
            outcome.version = Some(next_version);
            Ok(outcome)
        }
        Err(object_store::Error::AlreadyExists { .. }) => Err(anyhow::anyhow!(
            "another writer committed version {next_version} while this optimize was running",
        )),
        Err(e) => Err(anyhow::Error::new(e)
            .context(format!("could not write version {next_version}; {orphans}"))),
    }
}

/// Features that can put files outside the table's add actions.
fn hides_files(features: &[TableFeature]) -> Vec<&TableFeature> {
    features
        .iter()
        .filter(|feature| !files_are_enumerable(feature))
        .collect()
}

/// Features a physical rewrite cannot carry forward.
fn blocks_rewrite(features: &[TableFeature]) -> Vec<&TableFeature> {
    features
        .iter()
        .filter(|feature| !rewrite_is_supported(feature))
        .collect()
}

fn refuse_unsupported_features(snapshot: &Snapshot) -> Result<(), anyhow::Error> {
    check_writer_version(snapshot)?;
    refuse_features(writer_features(snapshot))
}

fn refuse_features(features: &[TableFeature]) -> Result<(), anyhow::Error> {
    let named = |features: Vec<&TableFeature>| {
        features
            .iter()
            .map(|feature| feature.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };

    match hides_files(features) {
        hidden if hidden.is_empty() => Ok(()),
        hidden => Err(anyhow::anyhow!(
            "the feature(s) {} may reference files that are not in the table's add actions",
            named(hidden)
        )),
    }?;

    match blocks_rewrite(features) {
        blocked if blocked.is_empty() => Ok(()),
        blocked => Err(anyhow::anyhow!(
            "the feature(s) {} cannot be carried forward by rewriting a file",
            named(blocked)
        )),
    }
}

/// Files worth reading a footer for: too small to leave alone, or carrying a deletion vector.
fn candidates(files: Vec<ScanFile>, target_size: u64) -> Vec<ScanFile> {
    files
        .into_iter()
        .filter(|file| (file.size.max(0) as u64) < target_size || file.dv_info.has_vector())
        .collect()
}

struct Candidate {
    file: ScanFile,
    schema: SchemaRef,
    order_min: Option<Scalar>,
}

/// Read one footer per candidate.
async fn read_candidates(
    files: Vec<ScanFile>,
    store: Arc<dyn ObjectStore>,
    base_path: &object_store::path::Path,
    order_column: Option<&str>,
    cloud_io: &Handle,
) -> Result<Vec<Candidate>, anyhow::Error> {
    stream::iter(files)
        .map(|file| {
            let (store, handle) = (store.clone(), cloud_io.clone());
            let path = file_path(base_path, &file.path);
            async move {
                let path = path.context("cannot optimize this table")?;
                let metadata =
                    read_parquet_metadata(store, &path, file.size.max(0) as u64, handle).await?;
                let schema = Arc::new(parquet_to_arrow_schema(
                    metadata.file_metadata().schema_descr(),
                    metadata.file_metadata().key_value_metadata(),
                )?);
                let order_min = order_column.and_then(|column| min_of(&metadata, column));
                Ok(Candidate {
                    file,
                    schema,
                    order_min,
                })
            }
        })
        .buffered(FOOTER_CONCURRENCY)
        .try_collect()
        .await
}

/// The name the table's files write `column` under, which column mapping makes a different string
/// from the one in the schema. Errors when the table has no such column.
fn physical_name(snapshot: &Snapshot, column: &str) -> Result<String, anyhow::Error> {
    let schema = snapshot.schema();
    let field = schema.field(column).ok_or_else(|| {
        anyhow::anyhow!("cannot order by '{column}': the table has no such column")
    })?;

    match field.get_config_value(&ColumnMetadataKey::ColumnMappingPhysicalName) {
        Some(MetadataValue::String(physical)) => Ok(physical.clone()),
        _ => Ok(column.to_string()),
    }
}

/// The smallest value of `column` across the file's row groups. `None` when the file does not hold
/// the column, the writer tracked no statistics for it, or the minimum is a NaN.
fn min_of(metadata: &ParquetMetaData, column: &str) -> Option<Scalar> {
    let index = metadata
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|descriptor| descriptor.name() == column)?;

    let min = metadata
        .row_groups()
        .iter()
        .filter_map(|group| group.column(index).statistics())
        .fold(None, |min, statistics| {
            pick_bound(min, stats_to_scalars(statistics).0, Ordering::Less)
        })?;

    // Parquet excludes NaN from min/max
    match min {
        Scalar::Float(value) if value.is_nan() => None,
        Scalar::Double(value) if value.is_nan() => None,
        min => Some(min),
    }
}

/// The physical row positions a file's deletion vector removes, ascending; empty when it has none.
fn deleted_rows(
    dv_info: &DvInfo,
    engine: &dyn Engine,
    table_root: &url::Url,
) -> Result<Vec<u64>, anyhow::Error> {
    Ok(dv_info
        .get_row_indexes(engine, table_root)?
        .unwrap_or_default())
}

/// Which of the next `rows` rows survive, `false` where the deletion vector removed one. `None` when
/// this window holds no deletions at all.
///
/// `offset` is how many rows of the file have already been read, which lines up with the positions
/// only because the whole file is read in order: no predicate, no row group skipped.
fn keep_mask(deleted: &[u64], offset: u64, rows: usize) -> Option<BooleanArray> {
    let end = offset + rows as u64;
    let start = deleted.partition_point(|&position| position < offset);
    let stop = deleted.partition_point(|&position| position < end);
    if start == stop {
        return None;
    }

    let mut keep = BooleanBufferBuilder::new(rows);
    keep.append_n(rows, true);
    for &position in &deleted[start..stop] {
        keep.set_bit((position - offset) as usize, false);
    }
    Some(BooleanArray::new(keep.finish(), None))
}

/// Read every source file of a bin, drop the rows its deletion vector removed, and write what is
/// left as one file. `None` when every row was deleted: the bin still commits its removes, it just
/// has no add.
async fn rewrite_bin(
    bin: &Bin,
    store: Arc<dyn ObjectStore>,
    base_path: &object_store::path::Path,
    table_root: &url::Url,
    engine: &dyn Engine,
    cloud_io: &Handle,
) -> Result<Option<(String, ParquetMetaData, u64)>, anyhow::Error> {
    let output = bin.store_output.clone();
    let mut sink = ParquetSink::with_store(
        store.clone(),
        base_path.clone(),
        bin.schema.clone(),
        None,
        None,
        Box::new(move |_| Ok(output.clone())),
        None,
    )?;

    for file in &bin.files {
        let deleted = deleted_rows(&file.dv_info, engine, table_root)?;
        let path = file_path(base_path, &file.path)?;
        let mut stream = stream_parquet(
            store.clone(),
            &path,
            file.size.max(0) as u64,
            cloud_io.clone(),
        )
        .await?;

        let mut offset = 0u64;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            match keep_mask(&deleted, offset, batch.num_rows()) {
                Some(mask) => {
                    sink.write_batch(&filter_record_batch(&batch, &mask)?)
                        .await?
                }
                None => sink.write_batch(&batch).await?,
            }
            offset += batch.num_rows() as u64;
        }
    }

    let mut written = sink.finish().await?;
    match written.len() {
        // Every row was deleted, so no writer was ever opened.
        0 => Ok(None),
        1 => Ok(Some(written.remove(0))),
        n => Err(anyhow::anyhow!(
            "the sink split one bin across {n} files; only one can be committed"
        )),
    }
}

/// One rewrite: the files that go in, and where the file that comes out belongs.
#[derive(Debug)]
pub struct Bin {
    files: Vec<ScanFile>,
    partition_values: HashMap<String, Option<String>>,
    schema: SchemaRef,
    /// Where the new file goes, as a URI, for the path the add records.
    output: String,
    /// The same file as the object store spells it, for writing it.
    store_output: String,
}

impl Bin {
    pub fn input_files(&self) -> usize {
        self.files.len()
    }

    pub fn input_bytes(&self) -> u64 {
        self.files.iter().map(|file| file.size.max(0) as u64).sum()
    }

    pub fn output(&self) -> &str {
        &self.output
    }
}

/// Name, type and nullability per column, in order.
type SchemaKey = Vec<(String, DataType, bool)>;

/// Groups files that can be concatenated: same partition, and columns that match by name, order,
/// type and nullability.
fn schema_key(schema: &SchemaRef) -> SchemaKey {
    schema
        .fields()
        .iter()
        .map(|field| {
            (
                field.name().clone(),
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect()
}

fn partition_key(values: &HashMap<String, Option<String>>) -> String {
    let mut entries: Vec<_> = values.iter().collect();
    entries.sort();
    let mut key = String::new();
    for (name, value) in entries {
        let _ = match value {
            Some(value) => write!(key, "{}:{name}={}:{value}", name.len(), value.len()),
            None => write!(key, "{}:{name}=null", name.len()),
        };
    }
    key
}

/// Bins worth rewriting: two or more files becoming one, or a lone file with a deletion vector
fn plan_bins(
    candidates: Vec<Candidate>,
    target_size: u64,
    declared: &[String],
) -> Result<Vec<Bin>, anyhow::Error> {
    let mut groups: BTreeMap<String, Vec<(SchemaKey, Vec<Candidate>)>> = BTreeMap::new();

    for candidate in candidates {
        let key = schema_key(&candidate.schema);
        let by_schema = groups
            .entry(partition_key(&partition_values(
                declared,
                &candidate.file.partition_values,
            )))
            .or_default();
        match by_schema.iter_mut().find(|(seen, _)| *seen == key) {
            Some((_, group)) => group.push(candidate),
            None => by_schema.push((key, vec![candidate])),
        }
    }

    groups
        .into_values()
        .flatten()
        .flat_map(|(_, mut group)| {
            group.sort_by(by_order_min);
            pack(group, target_size)
        })
        .filter(|group| group.len() >= 2 || group[0].file.dv_info.has_vector())
        .map(|group| {
            let first = &group[0];
            let store_path = store_path_from_uri(&first.file.path)?.map(String::from).ok_or_else(|| {
                anyhow::anyhow!(
                    "cannot optimize a table that references the absolute path {}. Its files are outside the table root",
                    first.file.path
                )
            })?;
            // One name for both spellings, so they always denote the same file. A uuid means an
            // abandoned attempt's orphans can never collide with a later one's output.
            let name = format!("{}.parquet", Uuid::new_v4());
            Ok(Bin {
                partition_values: partition_values(declared, &first.file.partition_values),
                schema: first.schema.clone(),
                output: format!("{}{name}", directory_of(&first.file.path)),
                store_output: format!("{}{name}", directory_of(&store_path)),
                files: group.into_iter().map(|candidate| candidate.file).collect(),
            })
        })
        .collect()
}

/// Total within a group: one schema means one scalar variant, and [`min_of`] has already refused
/// the NaN that would make this intransitive.
fn by_order_min(a: &Candidate, b: &Candidate) -> Ordering {
    match (&a.order_min, &b.order_min) {
        (Some(a), Some(b)) => a.logical_partial_cmp(b).unwrap_or(Ordering::Equal),
        // Files with a known range pack together first; the rest cluster behind them.
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

/// Greedy over whole files. A bin closes when the next file would push it past the target.
fn pack(candidates: Vec<Candidate>, target_size: u64) -> Vec<Vec<Candidate>> {
    let mut bins: Vec<Vec<Candidate>> = Vec::new();
    let mut current: Vec<Candidate> = Vec::new();
    let mut current_bytes: u64 = 0;

    for candidate in candidates {
        let size = candidate.file.size.max(0) as u64;
        if !current.is_empty() && current_bytes.saturating_add(size) > target_size {
            bins.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes = current_bytes.saturating_add(size);
        current.push(candidate);
    }

    if !current.is_empty() {
        bins.push(current);
    }
    bins
}

/// Everything up to and including the last separator, so joining a filename to it lands beside the
/// sources. Empty when the file sits at the table root.
fn directory_of(path: &str) -> String {
    match path.rfind('/') {
        Some(index) => path[..=index].to_string(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Float64Array, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use delta_kernel::actions::deletion_vector::{
        DeletionVectorDescriptor, DeletionVectorStorageType,
    };
    use delta_kernel::scan::state::DvInfo;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::ParquetMetaDataReader;

    use super::*;

    const NONE: Vec<&TableFeature> = Vec::new();

    fn metadata_of(batches: &[RecordBatch]) -> ParquetMetaData {
        let mut buf: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batches[0].schema(), None).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
            writer.flush().unwrap();
        }
        writer.close().unwrap();
        ParquetMetaDataReader::new()
            .parse_and_finish(&Bytes::from(buf))
            .unwrap()
    }

    fn ids(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn scores(values: Vec<f64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "score",
            DataType::Float64,
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(values))]).unwrap()
    }

    #[test]
    fn min_of_folds_across_row_groups() {
        let metadata = metadata_of(&[ids(vec![50, 60]), ids(vec![10, 20]), ids(vec![30])]);
        assert_eq!(min_of(&metadata, "id"), Some(Scalar::Long(10)));
    }

    #[test]
    fn min_of_is_none_for_a_column_the_file_does_not_hold() {
        let metadata = metadata_of(&[ids(vec![1])]);
        assert_eq!(min_of(&metadata, "not_a_column"), None);
    }

    #[test]
    fn a_nan_never_becomes_the_minimum() {
        // Parquet leaves NaN out of min/max statistics, so an all-NaN row group reports none at
        // all and contributes nothing to the fold, whichever side of a real value it sits on.
        let only_nan = metadata_of(&[scores(vec![f64::NAN])]);
        assert_eq!(min_of(&only_nan, "score"), None);

        for batches in [
            vec![scores(vec![f64::NAN]), scores(vec![1.0, 5.0])],
            vec![scores(vec![1.0, 5.0]), scores(vec![f64::NAN])],
        ] {
            let metadata = metadata_of(&batches);
            assert_eq!(min_of(&metadata, "score"), Some(Scalar::Double(1.0)));
        }
    }

    fn scan_file(path: &str, size: i64, dv: bool) -> ScanFile {
        let dv_info = match dv {
            true => DvInfo::from(DeletionVectorDescriptor {
                storage_type: DeletionVectorStorageType::Inline,
                path_or_inline_dv: "irrelevant".to_string(),
                offset: None,
                size_in_bytes: 8,
                cardinality: 1,
            }),
            false => DvInfo::default(),
        };
        ScanFile {
            path: path.to_string(),
            size,
            modification_time: 0,
            stats: None,
            dv_info,
            transform: None,
            partition_values: HashMap::new(),
        }
    }

    fn paths(files: Vec<ScanFile>) -> Vec<String> {
        files.into_iter().map(|file| file.path).collect()
    }

    const TARGET: u64 = 1000;

    #[test]
    fn a_file_at_the_target_is_left_alone() {
        let files = vec![
            scan_file("under", TARGET as i64 - 1, false),
            scan_file("at", TARGET as i64, false),
            scan_file("over", TARGET as i64 + 1, false),
        ];
        assert_eq!(paths(candidates(files, TARGET)), vec!["under"]);
    }

    #[test]
    fn a_deletion_vector_makes_a_file_a_candidate_at_any_size() {
        let files = vec![
            scan_file("big_with_dv", TARGET as i64 * 100, true),
            scan_file("big_without", TARGET as i64 * 100, false),
        ];
        assert_eq!(paths(candidates(files, TARGET)), vec!["big_with_dv"]);
    }

    #[test]
    fn a_corrupt_negative_size_is_a_candidate() {
        let files = vec![scan_file("negative", -1, false)];
        assert_eq!(paths(candidates(files, TARGET)), vec!["negative"]);
    }

    #[test]
    fn nothing_in_nothing_out() {
        assert!(candidates(Vec::new(), TARGET).is_empty());
    }

    fn candidate(path: &str, size: i64, dv: bool, order_min: Option<i64>) -> Candidate {
        Candidate {
            file: scan_file(path, size, dv),
            schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
            order_min: order_min.map(Scalar::Long),
        }
    }

    fn partitioned(mut candidate: Candidate, values: &[(&str, &str)]) -> Candidate {
        candidate.file.partition_values = values
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        candidate
    }

    fn bin_paths(bins: &[Bin]) -> Vec<Vec<&str>> {
        bins.iter()
            .map(|bin| bin.files.iter().map(|file| file.path.as_str()).collect())
            .collect()
    }

    /// `true` where the row survives, for readable assertions.
    fn kept(deleted: &[u64], offset: u64, rows: usize) -> Option<Vec<bool>> {
        keep_mask(deleted, offset, rows).map(|mask| mask.values().iter().collect())
    }

    #[test]
    fn a_window_with_no_deletions_needs_no_mask() {
        assert_eq!(kept(&[], 0, 3), None);
        // Deletions entirely before the window.
        assert_eq!(kept(&[0, 1], 2, 3), None);
        // Deletions entirely after it.
        assert_eq!(kept(&[9, 10], 2, 3), None);
    }

    #[test]
    fn a_deletion_at_either_edge_of_the_window_is_caught() {
        // First row of the window.
        assert_eq!(kept(&[10], 10, 3), Some(vec![false, true, true]));
        assert_eq!(kept(&[12], 10, 3), Some(vec![true, true, false]));
        assert_eq!(kept(&[13], 10, 3), None);
    }

    #[test]
    fn several_deletions_in_one_window() {
        assert_eq!(
            kept(&[0, 2, 4], 0, 5),
            Some(vec![false, true, false, true, false])
        );
    }

    #[test]
    fn every_row_deleted_keeps_nothing() {
        assert_eq!(kept(&[5, 6, 7], 5, 3), Some(vec![false, false, false]));
    }

    #[test]
    fn the_offset_carries_across_consecutive_windows() {
        let deleted = [1u64, 4, 7];
        assert_eq!(kept(&deleted, 0, 3), Some(vec![true, false, true]));
        assert_eq!(kept(&deleted, 3, 3), Some(vec![true, false, true]));
        assert_eq!(kept(&deleted, 6, 3), Some(vec![true, false, true]));
        assert_eq!(kept(&deleted, 9, 3), None);
    }

    #[test]
    fn a_repeated_position_does_not_shift_the_mask() {
        assert_eq!(kept(&[1, 1, 1], 0, 3), Some(vec![true, false, true]));
    }

    fn key(values: &[(&str, Option<&str>)]) -> String {
        partition_key(
            &values
                .iter()
                .map(|(n, v)| (n.to_string(), v.map(str::to_string)))
                .collect(),
        )
    }

    #[test]
    fn partition_key_tells_apart_absent_null_and_valued() {
        let unpartitioned = key(&[]);
        let null_value = key(&[("dt", None)]);
        let valued = key(&[("dt", Some("x"))]);
        assert_ne!(unpartitioned, null_value);
        assert_ne!(null_value, valued);
        // A value holding the separator cannot forge another partition's key.
        assert_ne!(
            key(&[("a", Some("b/c")), ("d", Some("e"))]),
            key(&[("a", Some("b")), ("c/d", Some("e"))])
        );
    }

    #[test]
    fn a_null_is_not_the_string_that_spells_it() {
        assert_ne!(key(&[("dt", None)]), key(&[("dt", Some("null"))]));
        assert_ne!(key(&[("dt", None)]), key(&[("dt", Some(""))]));
    }

    #[test]
    fn partition_key_ignores_the_order_the_map_iterates() {
        assert_eq!(
            key(&[("a", Some("1")), ("b", Some("2"))]),
            key(&[("b", Some("2")), ("a", Some("1"))])
        );
    }

    #[test]
    fn partition_values_names_every_declared_column() {
        let declared = ["island".to_string(), "sex".to_string()];
        let from_scan = HashMap::from([("island".to_string(), "Biscoe".to_string())]);
        assert_eq!(
            partition_values(&declared, &from_scan),
            HashMap::from([
                ("island".to_string(), Some("Biscoe".to_string())),
                ("sex".to_string(), None),
            ])
        );
    }

    #[test]
    fn an_unpartitioned_table_names_no_columns() {
        let from_scan = HashMap::from([("stray".to_string(), "value".to_string())]);
        assert!(partition_values(&[], &from_scan).is_empty());
    }

    #[test]
    fn a_writer_accepts_batches_whose_schema_differs_only_in_metadata() {
        let plain = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let annotated = Arc::new(
            Schema::new(vec![Field::new("id", DataType::Int64, true).with_metadata(
                [("PARQUET:field_id".to_string(), "1".to_string())].into(),
            )])
            .with_metadata([("writer".to_string(), "spark".to_string())].into()),
        );
        assert_eq!(schema_key(&plain), schema_key(&annotated));

        let mut buf: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, plain.clone(), None).unwrap();
        for schema in [&plain, &annotated] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            writer
                .write(&batch)
                .expect("writer must accept a batch that differs only in metadata");
        }
        writer.close().unwrap();

        let metadata = ParquetMetaDataReader::new()
            .parse_and_finish(&Bytes::from(buf))
            .unwrap();
        assert_eq!(metadata.file_metadata().num_rows(), 6);
    }

    #[test]
    fn schema_key_ignores_metadata_but_not_nullability() {
        let plain = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let annotated = Arc::new(
            Schema::new(vec![Field::new("id", DataType::Int64, true).with_metadata(
                [("PARQUET:field_id".to_string(), "1".to_string())].into(),
            )])
            .with_metadata([("writer".to_string(), "spark".to_string())].into()),
        );
        let not_null = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        assert_eq!(schema_key(&plain), schema_key(&annotated));
        assert_ne!(schema_key(&plain), schema_key(&not_null));
    }

    #[test]
    fn files_pack_until_the_next_one_would_overshoot() {
        let bins = plan_bins(
            vec![
                candidate("a", 400, false, Some(1)),
                candidate("b", 400, false, Some(2)),
                candidate("c", 400, false, Some(3)),
            ],
            1000,
            &[],
        )
        .unwrap();
        assert_eq!(bin_paths(&bins), vec![vec!["a", "b"]]);
    }

    #[test]
    fn a_file_larger_than_the_target_is_not_split() {
        let bins = plan_bins(
            vec![
                candidate("big", 5000, true, Some(1)),
                candidate("a", 100, false, Some(2)),
            ],
            1000,
            &[],
        )
        .unwrap();
        assert_eq!(bin_paths(&bins), vec![vec!["big"]]);
    }

    #[test]
    fn bins_pack_in_order_of_the_order_column() {
        let bins = plan_bins(
            vec![
                candidate("third", 400, false, Some(30)),
                candidate("first", 400, false, Some(10)),
                candidate("second", 400, false, Some(20)),
            ],
            900,
            &[],
        )
        .unwrap();
        assert_eq!(bin_paths(&bins), vec![vec!["first", "second"]]);
    }

    #[test]
    fn files_without_statistics_pack_behind_the_ones_that_have_them() {
        let bins = plan_bins(
            vec![
                candidate("unknown", 400, false, None),
                candidate("known", 400, false, Some(10)),
            ],
            1000,
            &[],
        )
        .unwrap();
        assert_eq!(bin_paths(&bins), vec![vec!["known", "unknown"]]);
    }

    #[test]
    fn partitions_never_share_a_bin() {
        let bins = plan_bins(
            vec![
                partitioned(candidate("us/a", 100, false, Some(1)), &[("region", "us")]),
                partitioned(candidate("eu/b", 100, false, Some(2)), &[("region", "eu")]),
                partitioned(candidate("us/c", 100, false, Some(3)), &[("region", "us")]),
            ],
            1000,
            &["region".to_string()],
        )
        .unwrap();
        // eu sorts before us, and its lone file is dropped for having no deletion vector.
        assert_eq!(bin_paths(&bins), vec![vec!["us/a", "us/c"]]);
    }

    #[test]
    fn a_bin_carries_the_directory_its_sources_live_in() {
        let bins = plan_bins(
            vec![
                partitioned(
                    candidate("dt=100%2525/a.parquet", 100, false, Some(1)),
                    &[("dt", "100%")],
                ),
                partitioned(
                    candidate("dt=100%2525/b.parquet", 100, false, Some(2)),
                    &[("dt", "100%")],
                ),
            ],
            1000,
            &["dt".to_string()],
        )
        .unwrap();
        assert!(
            bins[0].output.starts_with("dt=100%2525/") && bins[0].output.ends_with(".parquet"),
            "got: {}",
            bins[0].output
        );
        assert!(
            bins[0].store_output.starts_with("dt=100%25/"),
            "got: {}",
            bins[0].store_output
        );
        // Both spellings must name the same file.
        assert_eq!(
            store_path_from_uri(&bins[0].output)
                .unwrap()
                .map(String::from),
            Some(bins[0].store_output.clone())
        );
        assert_eq!(
            bins[0].partition_values.get("dt"),
            Some(&Some("100%".to_string()))
        );
    }

    #[test]
    fn an_absolute_source_path_refuses_the_plan() {
        let err = plan_bins(
            vec![
                candidate("s3://elsewhere/t/a.parquet", 100, false, Some(1)),
                candidate("s3://elsewhere/t/b.parquet", 100, false, Some(2)),
            ],
            1000,
            &[],
        )
        .unwrap_err();
        assert!(err.to_string().contains("absolute path"), "got: {err}");
    }

    #[test]
    fn a_rewrite_preserves_the_features_ldrs_writes() {
        // `appendOnly` included: `dataChange: false` compaction is the recognized carve-out.
        let features = [
            TableFeature::TimestampWithoutTimezone,
            TableFeature::DeletionVectors,
            TableFeature::ChangeDataFeed,
            TableFeature::AppendOnly,
        ];
        assert_eq!(hides_files(&features), NONE);
        assert_eq!(blocks_rewrite(&features), NONE);
        assert!(refuse_features(&features).is_ok());
    }

    #[test]
    fn iceberg_metadata_hides_files_but_rewrites_fine() {
        let features = [TableFeature::IcebergCompatV2];
        assert_eq!(hides_files(&features), vec![&TableFeature::IcebergCompatV2]);
        assert_eq!(blocks_rewrite(&features), NONE);
    }

    #[test]
    fn row_tracking_falls_through_to_the_rewrite_check() {
        let features = [TableFeature::RowTracking];
        assert_eq!(hides_files(&features), NONE);
        assert_eq!(blocks_rewrite(&features), vec![&TableFeature::RowTracking]);
    }

    #[test]
    fn clustering_falls_through_to_the_rewrite_check() {
        let features = [TableFeature::ClusteredTable];
        assert_eq!(hides_files(&features), NONE);
        assert_eq!(
            blocks_rewrite(&features),
            vec![&TableFeature::ClusteredTable]
        );
    }

    #[test]
    fn an_unknown_feature_is_caught_by_the_first_check() {
        let unknown = TableFeature::Unknown("someFutureFeature".to_string());
        let features = [unknown.clone()];
        assert_eq!(hides_files(&features), vec![&unknown]);
        assert_eq!(blocks_rewrite(&features), vec![&unknown]);
    }

    #[test]
    fn only_the_refused_features_are_reported() {
        let features = [
            TableFeature::TimestampWithoutTimezone,
            TableFeature::RowTracking,
            TableFeature::ClusteredTable,
        ];
        let message = format!("{:#}", refuse_features(&features).unwrap_err());
        assert!(message.contains("rowTracking"), "got: {message}");
        assert!(message.contains("clustering"), "got: {message}");
        assert!(
            !message.contains("timestampNtz"),
            "a supported feature must not be named: {message}"
        );
    }
}
