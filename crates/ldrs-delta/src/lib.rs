use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

use anyhow::Context;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::schema::{DataType as DeltaDataType, StructField, StructType};
use delta_kernel::snapshot::CheckpointWriteResult;
use delta_kernel::table_features::TableFeature;
use delta_kernel::{Engine, Snapshot, SnapshotRef, Version};
use delta_kernel_default_engine::executor::tokio::TokioMultiThreadExecutor;
use delta_kernel_default_engine::DefaultEngineBuilder;
use futures::{Stream, StreamExt};
use ldrs_storage::{
    base_or_relative_path, build_store, join_store_path, kernel_url, store_path_from_uri,
};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::Serialize;
use tokio::runtime::Handle;
use uuid::Uuid;

mod dv;
mod features;
mod merge;
mod optimize;
mod overwrite;
mod stats;
mod vacuum;

pub use features::refuse_non_micros_timestamps;
pub use merge::*;
pub use optimize::*;
pub use overwrite::*;
pub use stats::*;
pub use vacuum::*;

const CHECKPOINT_INTERVAL: u64 = 10;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeltaCommitInfo {
    timestamp: i64,
    operation: String,
    operation_parameters: HashMap<String, String>,
    engine_info: String,
    /// Free-form per-operation counts, as `DESCRIBE HISTORY` reports them. Spelled the way Spark spells them so the same tooling reads both.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    operation_metrics: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_commit_timestamp: Option<i64>,
}

impl DeltaCommitInfo {
    /// The commit that creates the table, at version 0.
    fn for_create(operation: &Operation, now_ms: i64) -> Self {
        DeltaCommitInfo {
            timestamp: now_ms,
            operation: operation.name().to_string(),
            operation_parameters: operation.parameters(),
            engine_info: engine_info(),
            operation_metrics: HashMap::new(),
            in_commit_timestamp: Some(now_ms),
        }
    }

    /// A commit appended to an existing table.
    fn for_commit(
        operation: &Operation,
        snapshot: &Snapshot,
        engine: &dyn Engine,
        now_ms: i64,
    ) -> Result<Self, anyhow::Error> {
        Ok(DeltaCommitInfo {
            timestamp: now_ms,
            operation: operation.name().to_string(),
            operation_parameters: operation.parameters(),
            engine_info: engine_info(),
            operation_metrics: HashMap::new(),
            in_commit_timestamp: in_commit_timestamp(snapshot, engine, now_ms)?,
        })
    }
}

fn engine_info() -> String {
    format!("ldrs-{}", env!("CARGO_PKG_VERSION"))
}

/// The `inCommitTimestamp` for a commit built on `snapshot`, or `None` when the table does not
/// have in-commit timestamps enabled.
fn in_commit_timestamp(
    snapshot: &Snapshot,
    engine: &dyn Engine,
    now_ms: i64,
) -> Result<Option<i64>, anyhow::Error> {
    match (
        features::writer_features(snapshot).contains(&TableFeature::InCommitTimestamp),
        snapshot.table_properties().enable_in_commit_timestamps,
    ) {
        (true, Some(true)) => {
            let previous = snapshot.get_timestamp(engine).with_context(|| {
                let file = version_to_log_filename(snapshot.version());
                format!("no 'inCommitTimestamp' in '_delta_log/{file}'")
            })?;
            Ok(Some(now_ms.max(previous + 1)))
        }
        _ => Ok(None),
    }
}

#[derive(Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct DeltaProtocol {
    min_reader_version: i32,
    min_writer_version: i32,
    reader_features: Vec<TableFeature>,
    writer_features: Vec<TableFeature>,
}

impl DeltaProtocol {
    /// What a table that declares nothing looks like, for a table being created.
    fn none() -> Self {
        DeltaProtocol {
            min_reader_version: 0,
            min_writer_version: 0,
            reader_features: Vec::new(),
            writer_features: Vec::new(),
        }
    }

    fn from_snapshot(snapshot: &Snapshot) -> Self {
        let protocol = snapshot.table_configuration().protocol();
        DeltaProtocol {
            min_reader_version: protocol.min_reader_version(),
            min_writer_version: protocol.min_writer_version(),
            reader_features: protocol.reader_features().unwrap_or_default().to_vec(),
            writer_features: protocol.writer_features().unwrap_or_default().to_vec(),
        }
    }

    fn with_reader_features(mut self, required: &[TableFeature]) -> Self {
        append_missing(&mut self.reader_features, required);
        if !self.reader_features.is_empty() {
            self.min_reader_version = MIN_READER_VERSION;
        }
        self
    }

    fn with_writer_features(mut self, required: &[TableFeature]) -> Self {
        append_missing(&mut self.writer_features, required);
        if !self.writer_features.is_empty() {
            self.min_writer_version = MIN_WRITER_VERSION;
        }
        self
    }
}

/// Append the required features the list does not already hold.
fn append_missing(features: &mut Vec<TableFeature>, required: &[TableFeature]) {
    for feature in required {
        if !features.contains(feature) {
            features.push(feature.clone());
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeltaFormat {
    provider: String,
    options: HashMap<String, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeltaMetadata {
    id: String,
    format: DeltaFormat,
    schema_string: String,
    partition_columns: Vec<String>,
    created_time: i64,
    configuration: HashMap<String, String>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct DeltaAdd {
    path: String,
    /// One entry per partition column the table declares, `None` where the value is null.
    partition_values: HashMap<String, Option<String>>,
    size: i64,
    modification_time: i64,
    data_change: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deletion_vector: Option<dv::DeletionVectorDescriptor>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeltaRemove {
    path: String,
    deletion_timestamp: i64,
    data_change: bool,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    deletion_vector: Option<dv::DeletionVectorDescriptor>,
}

impl DeltaRemove {
    /// Tombstones the logical file `(path, deletionVector.uniqueId)`
    fn from_scan_file(
        ts: i64,
        existing_dvs: &HashMap<String, dv::DeletionVectorDescriptor>,
    ) -> impl Fn(ScanFile) -> Result<Self, anyhow::Error> + use<'_> {
        move |file| {
            let deletion_vector = existing_dvs.get(&file.path).cloned();
            if file.dv_info.has_vector() && deletion_vector.is_none() {
                anyhow::bail!(
                    "file {} has a deletion vector but its descriptor could not be recovered from \
                     the scan output; refusing to commit a remove that would leave it live",
                    file.path
                );
            }
            Ok(DeltaRemove {
                path: file.path,
                deletion_timestamp: ts,
                data_change: true,
                size: file.size,
                deletion_vector,
            })
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeltaTxn {
    app_id: String,
    version: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_updated: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
enum DeltaAction<'a> {
    CommitInfo(&'a DeltaCommitInfo),
    Protocol(&'a DeltaProtocol),
    MetaData(&'a DeltaMetadata),
    Add(&'a DeltaAdd),
    Remove(&'a DeltaRemove),
    Txn(&'a DeltaTxn),
}

fn should_checkpoint(version: Version, last_checkpoint: Option<Version>, interval: u64) -> bool {
    version.saturating_sub(last_checkpoint.unwrap_or(0)) >= interval
}

async fn write_checkpoint(
    engine: Arc<dyn Engine>,
    snapshot: SnapshotRef,
) -> Result<(CheckpointWriteResult, Arc<Snapshot>), anyhow::Error> {
    let res = tokio::task::spawn_blocking(move || snapshot.checkpoint(engine.as_ref(), None)).await;
    // flatten the Result to a single anyhow Result
    match res {
        Ok(Ok(t)) => Ok(t),
        Ok(Err(e)) => Err(anyhow::anyhow!("Checkpoint failed: {}", e)),
        Err(e) => Err(anyhow::anyhow!("Checkpoint task failed: {}", e)),
    }
}

/// What a checkpoint request did.
#[derive(Debug, Clone, Copy)]
pub struct CheckpointOutcome {
    pub version: Version,
    /// `false` when the version already had a checkpoint
    pub written: bool,
}

/// Consolidate the log at the table's current version
pub async fn checkpoint(
    table_path: &str,
    cloud_io: &Handle,
) -> Result<CheckpointOutcome, anyhow::Error> {
    let url = base_or_relative_path(table_path)?;
    let (store, _, _) = build_store(&url)?;
    let engine = build_engine(store, cloud_io);
    let snapshot = Snapshot::builder_for(kernel_url(&url)?).build(engine.as_ref())?;
    let version = snapshot.version();

    let (result, _) = write_checkpoint(engine, snapshot).await?;
    Ok(CheckpointOutcome {
        version,
        written: matches!(result, CheckpointWriteResult::Written),
    })
}

fn arrow_schema_to_delta_struct(schema: &SchemaRef) -> Result<StructType, anyhow::Error> {
    let fields: Vec<StructField> = schema
        .fields()
        .iter()
        .map(|f| {
            let delta_type = arrow_type_to_delta_type(f.data_type())?;
            Ok(StructField::new(f.name(), delta_type, f.is_nullable()))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    StructType::try_new(fields).map_err(|e| anyhow::anyhow!("Failed to build delta schema: {}", e))
}

fn arrow_type_to_delta_type(dt: &arrow_schema::DataType) -> Result<DeltaDataType, anyhow::Error> {
    use arrow_schema::DataType::*;
    match dt {
        Boolean => Ok(DeltaDataType::BOOLEAN),
        Int8 => Ok(DeltaDataType::Primitive(
            delta_kernel::schema::PrimitiveType::Byte,
        )),
        Int16 => Ok(DeltaDataType::SHORT),
        Int32 => Ok(DeltaDataType::INTEGER),
        Int64 => Ok(DeltaDataType::LONG),
        Float16 | Float32 => Ok(DeltaDataType::FLOAT),
        Float64 => Ok(DeltaDataType::DOUBLE),
        Utf8 | LargeUtf8 | Utf8View => Ok(DeltaDataType::STRING),
        Binary | LargeBinary | BinaryView => Ok(DeltaDataType::BINARY),
        Date32 | Date64 => Ok(DeltaDataType::DATE),
        Timestamp(_unit, tz) => {
            if tz.is_some() {
                Ok(DeltaDataType::TIMESTAMP)
            } else {
                Ok(DeltaDataType::TIMESTAMP_NTZ)
            }
        }
        Decimal32(precision, scale) => Ok(DeltaDataType::decimal(*precision, (*scale) as u8)
            .map_err(|e| anyhow::anyhow!("Invalid decimal type: {}", e))?),
        Decimal64(precision, scale) => Ok(DeltaDataType::decimal(*precision, (*scale) as u8)
            .map_err(|e| anyhow::anyhow!("Invalid decimal type: {}", e))?),
        Decimal128(precision, scale) => Ok(DeltaDataType::decimal(*precision, (*scale) as u8)
            .map_err(|e| anyhow::anyhow!("Invalid decimal type: {}", e))?),
        Decimal256(precision, scale) => Ok(DeltaDataType::decimal(*precision, (*scale) as u8)
            .map_err(|e| anyhow::anyhow!("Invalid decimal type: {}", e))?),
        FixedSizeBinary(_) => Ok(DeltaDataType::BINARY),
        other => Err(anyhow::anyhow!(
            "Unsupported Arrow type for Delta: {:?}",
            other
        )),
    }
}

fn build_commit_jsonl(actions: &[DeltaAction]) -> Result<String, anyhow::Error> {
    actions
        .iter()
        .map(|a| serde_json::to_string(a).map_err(Into::into))
        .collect::<Result<Vec<_>, anyhow::Error>>()
        .map(|lines| lines.join("\n"))
}

/// Feature lists exist only at reader version 3 / writer version 7, so any commit that declares
/// one sets both.
const MIN_READER_VERSION: i32 = 3;
const MIN_WRITER_VERSION: i32 = 7;

enum Operation {
    CreateTable,
    Write,
    Merge,
    Optimize,
}

impl Operation {
    fn name(&self) -> &'static str {
        match self {
            Operation::CreateTable => "CREATE TABLE",
            Operation::Write => "WRITE",
            Operation::Merge => "MERGE",
            Operation::Optimize => "OPTIMIZE",
        }
    }

    fn parameters(&self) -> HashMap<String, String> {
        match self {
            Operation::CreateTable => HashMap::new(),
            Operation::Write => HashMap::from([("mode".into(), "Overwrite".into())]),
            Operation::Merge => HashMap::from([("mode".into(), "Merge".into())]),
            Operation::Optimize => HashMap::new(),
        }
    }

    fn reader_features(&self) -> &'static [TableFeature] {
        match self {
            Operation::CreateTable | Operation::Write => &[TableFeature::TimestampWithoutTimezone],
            Operation::Merge => &[
                TableFeature::TimestampWithoutTimezone,
                TableFeature::DeletionVectors,
            ],
            Operation::Optimize => &[],
        }
    }

    /// `metaData.configuration` entries ldrs owns for this operation
    fn configuration(&self) -> &'static [(&'static str, &'static str)] {
        match self {
            Operation::CreateTable => &[("delta.enableInCommitTimestamps", "true")],
            Operation::Merge => &[("delta.enableDeletionVectors", "true")],
            Operation::Write | Operation::Optimize => &[],
        }
    }

    fn writer_features(&self) -> &'static [TableFeature] {
        match self {
            Operation::CreateTable => &[
                TableFeature::TimestampWithoutTimezone,
                TableFeature::InCommitTimestamp,
            ],
            Operation::Write => &[TableFeature::TimestampWithoutTimezone],
            Operation::Merge => &[
                TableFeature::TimestampWithoutTimezone,
                TableFeature::DeletionVectors,
            ],
            Operation::Optimize => &[],
        }
    }

    fn data_change(&self) -> bool {
        match self {
            Operation::CreateTable | Operation::Write | Operation::Merge => true,
            Operation::Optimize => false,
        }
    }
}

/// The protocol action a commit must write, or `None`
fn protocol_upgrade(current: DeltaProtocol, target: DeltaProtocol) -> Option<DeltaProtocol> {
    (target != current).then_some(target)
}

/// Refuse a table that partitions its data
fn refuse_partitioned(partition_columns: &[String]) -> Result<(), anyhow::Error> {
    match partition_columns {
        [] => Ok(()),
        columns => Err(anyhow::anyhow!(
            "cannot write to a table partitioned by '{}'",
            columns.join("', '")
        )),
    }
}

#[cfg(test)]
mod partition_refusal_tests {
    use super::refuse_partitioned;

    #[test]
    fn an_unpartitioned_table_is_accepted() {
        assert!(refuse_partitioned(&[]).is_ok());
    }

    #[test]
    fn the_refusal_names_every_partition_column() {
        let columns = ["island".to_string(), "sex".to_string()];
        let message = refuse_partitioned(&columns).unwrap_err().to_string();
        assert!(message.contains("'island', 'sex'"), "got: {message}");
    }
}

/// The `metaData` action a commit must write, or `None` when nothing in it moved.
fn metadata_upgrade(snapshot: &Snapshot, target: DeltaMetadata) -> Option<DeltaMetadata> {
    let current = snapshot.table_configuration().metadata();

    if current.schema_string() != &target.schema_string {
        tracing::warn!(
            "the schema being written differs from the table's; the table's schema will be \
             replaced with it"
        );
    }

    match metadata_moved(
        current.schema_string(),
        current.partition_columns(),
        current.configuration(),
        &target,
    ) {
        true => Some(target),
        false => None,
    }
}

/// Whether `target` differs from the table in any field a `metaData` action records.
fn metadata_moved(
    schema_string: &str,
    partition_columns: &[String],
    configuration: &HashMap<String, String>,
    target: &DeltaMetadata,
) -> bool {
    schema_string != target.schema_string
        || partition_columns != target.partition_columns.as_slice()
        || configuration != &target.configuration
}

struct Commit {
    data_change: bool,
    commit_info: DeltaCommitInfo,
    txn: Option<DeltaTxn>,
    protocol: Option<DeltaProtocol>,
    metadata: Option<DeltaMetadata>,
    removes: Vec<DeltaRemove>,
    adds: Vec<DeltaAdd>,
}

impl Commit {
    /// The commit that creates the table, at version 0.
    fn for_create(operation: Operation, now_ms: i64) -> Self {
        let target = DeltaProtocol::none()
            .with_reader_features(operation.reader_features())
            .with_writer_features(operation.writer_features());
        Commit {
            data_change: operation.data_change(),
            commit_info: DeltaCommitInfo::for_create(&operation, now_ms),
            txn: None,
            protocol: protocol_upgrade(DeltaProtocol::none(), target),
            metadata: None,
            removes: Vec::new(),
            adds: Vec::new(),
        }
    }

    /// A commit that rearranges which files the table holds and changes nothing about the table
    /// itself. No `metaData` target is built, so one cannot be written.
    fn for_maintenance(
        operation: Operation,
        snapshot: &Snapshot,
        engine: &dyn Engine,
        now_ms: i64,
    ) -> Result<Self, anyhow::Error> {
        let current = DeltaProtocol::from_snapshot(snapshot);
        let target = current
            .clone()
            .with_reader_features(operation.reader_features())
            .with_writer_features(operation.writer_features());
        Ok(Commit {
            data_change: operation.data_change(),
            commit_info: DeltaCommitInfo::for_commit(&operation, snapshot, engine, now_ms)?,
            txn: None,
            protocol: protocol_upgrade(current, target),
            metadata: None,
            removes: Vec::new(),
            adds: Vec::new(),
        })
    }

    /// A commit appended to an existing table. Has the diffs to properly set what this needs.
    fn for_table(
        operation: Operation,
        snapshot: &Snapshot,
        schema: &SchemaRef,
        table_config: &TableConfig,
        engine: &dyn Engine,
        now_ms: i64,
    ) -> Result<Self, anyhow::Error> {
        let current = DeltaProtocol::from_snapshot(snapshot);
        let target = current
            .clone()
            .with_reader_features(operation.reader_features())
            .with_writer_features(operation.writer_features())
            .with_writer_features(table_config.writer_features());

        let metadata = snapshot.table_configuration().metadata();
        refuse_partitioned(metadata.partition_columns())?;

        let desired = configuration_with(
            metadata.configuration(),
            table_config,
            operation.configuration(),
        );
        let metadata = build_metadata(
            schema,
            Some(metadata.id()),
            metadata.created_time(),
            desired,
            metadata.partition_columns().to_vec(),
        )?;

        Ok(Commit {
            data_change: operation.data_change(),
            commit_info: DeltaCommitInfo::for_commit(&operation, snapshot, engine, now_ms)?,
            txn: None,
            protocol: protocol_upgrade(current, target),
            metadata: metadata_upgrade(snapshot, metadata),
            removes: Vec::new(),
            adds: Vec::new(),
        })
    }

    fn with_txn(mut self, txn: DeltaTxn) -> Self {
        self.txn = Some(txn);
        self
    }

    fn with_metrics(mut self, metrics: HashMap<String, String>) -> Self {
        self.commit_info.operation_metrics = metrics;
        self
    }

    fn with_metadata(mut self, metadata: DeltaMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Appends, so a caller can contribute file actions in more than one batch.
    fn with_removes(mut self, removes: Vec<DeltaRemove>) -> Self {
        self.removes.extend(removes.into_iter().map(|mut remove| {
            remove.data_change = self.data_change;
            remove
        }));
        self
    }

    /// Appends
    fn with_adds(mut self, adds: Vec<DeltaAdd>) -> Self {
        self.adds.extend(adds.into_iter().map(|mut add| {
            add.data_change = self.data_change;
            add
        }));
        self
    }

    fn to_jsonl(&self) -> Result<String, anyhow::Error> {
        let mut actions = vec![DeltaAction::CommitInfo(&self.commit_info)];
        actions.extend(self.txn.as_ref().map(DeltaAction::Txn));
        actions.extend(self.protocol.as_ref().map(DeltaAction::Protocol));
        actions.extend(self.metadata.as_ref().map(DeltaAction::MetaData));
        actions.extend(self.removes.iter().map(DeltaAction::Remove));
        actions.extend(self.adds.iter().map(DeltaAction::Add));
        build_commit_jsonl(&actions)
    }
}

fn build_metadata(
    schema: &SchemaRef,
    table_id: Option<&str>,
    created_time: Option<i64>,
    configuration: HashMap<String, String>,
    partition_columns: Vec<String>,
) -> Result<DeltaMetadata, anyhow::Error> {
    let delta_schema = arrow_schema_to_delta_struct(schema)?;
    let schema_string = serde_json::to_string(&delta_schema)?;

    Ok(DeltaMetadata {
        id: table_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string()),
        format: DeltaFormat {
            provider: "parquet".to_string(),
            options: HashMap::new(),
        },
        schema_string,
        partition_columns,
        created_time: created_time.unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
        configuration,
    })
}

/// What a table should be, as far as a caller gets to say. Applied to every commit: entries that
/// differ from what the table carries are written into `metaData`, and any feature they require is
/// declared in `protocol` by the same commit.
///
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableConfig {
    pub target_file_size: Option<NonZeroU64>,
    pub checkpoint_interval: Option<NonZeroU64>,
}

impl TableConfig {
    pub fn set(&mut self, key: &str, value: &str) -> Result<(), anyhow::Error> {
        let positive = || {
            value
                .parse::<NonZeroU64>()
                .with_context(|| format!("'{key}' needs a positive integer, got '{value}'"))
        };
        match key {
            "delta.targetFileSize" => self.target_file_size = Some(positive()?),
            "delta.checkpointInterval" => self.checkpoint_interval = Some(positive()?),
            "delta.enableDeletionVectors" | "delta.enableInCommitTimestamps" => {
                anyhow::bail!("'{key}' is managed by ldrs and cannot be set")
            }
            _ => anyhow::bail!("'{key}' is not a table property ldrs can set"),
        }
        Ok(())
    }

    fn entries(&self) -> Vec<(String, String)> {
        [
            self.target_file_size
                .map(|size| ("delta.targetFileSize", size)),
            self.checkpoint_interval
                .map(|every| ("delta.checkpointInterval", every)),
        ]
        .into_iter()
        .flatten()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
    }

    fn writer_features(&self) -> &'static [TableFeature] {
        &[]
    }
}

fn configuration_with(
    current: &HashMap<String, String>,
    table_config: &TableConfig,
    ldrs_own: &[(&str, &str)],
) -> HashMap<String, String> {
    let mut configuration = current.clone();
    configuration.extend(table_config.entries());
    configuration.extend(
        ldrs_own
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string())),
    );
    configuration
}

pub async fn ensure_table(table_path: &str, schema: &SchemaRef) -> Result<(), anyhow::Error> {
    refuse_non_micros_timestamps(schema)?;
    let url = base_or_relative_path(table_path)?;
    let (store, base_path, _) = build_store(&url)?;

    let now_ms = chrono::Utc::now().timestamp_millis();

    let configuration = HashMap::from([(
        "delta.enableInCommitTimestamps".to_string(),
        "true".to_string(),
    )]);

    let commit_body = Commit::for_create(Operation::CreateTable, now_ms)
        .with_metadata(build_metadata(
            schema,
            None,
            None,
            configuration,
            Vec::new(),
        )?)
        .to_jsonl()?;
    let log_path = base_path
        .clone()
        .join("_delta_log")
        .clone()
        .join("00000000000000000000.json");

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
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn build_engine(store: Arc<dyn ObjectStore>, cloud_io: &Handle) -> Arc<dyn Engine> {
    Arc::new(
        DefaultEngineBuilder::new(store)
            .with_task_executor(Arc::new(TokioMultiThreadExecutor::new(cloud_io.clone())))
            .build(),
    )
}

struct TableState {
    version: Version,
    active_files: Vec<ScanFile>,
    existing_dvs: HashMap<String, dv::DeletionVectorDescriptor>,
    snapshot: SnapshotRef,
}

fn snapshot_table_state(
    engine: &dyn Engine,
    table_url: &url::Url,
) -> Result<TableState, anyhow::Error> {
    let snapshot = Snapshot::builder_for(kernel_url(table_url)?).build(engine)?;
    let version = snapshot.version();
    let scan = snapshot
        .clone()
        .scan_builder()
        .without_row_transforms()
        .build()?;

    let mut active_files = Vec::new();
    let mut existing_dvs = HashMap::new();

    for scan_metadata in scan.scan_metadata(engine)? {
        let scan_metadata = scan_metadata?;
        fn collect_file(files: &mut Vec<ScanFile>, scan_file: ScanFile) {
            files.push(scan_file);
        }
        existing_dvs.extend(dv::read_existing_dvs(&scan_metadata.scan_files)?);
        active_files = scan_metadata.visit_scan_files(active_files, collect_file)?;
    }

    Ok(TableState {
        version,
        active_files,
        existing_dvs,
        snapshot,
    })
}

fn partition_values(
    declared: &[String],
    from_scan: &HashMap<String, String>,
) -> HashMap<String, Option<String>> {
    declared
        .iter()
        .map(|column| (column.clone(), from_scan.get(column).cloned()))
        .collect()
}

fn build_add(
    filename: &str,
    metadata: &parquet::file::metadata::ParquetMetaData,
    size: u64,
    modification_time: i64,
    schema: &SchemaRef,
    partition_values: HashMap<String, Option<String>>,
) -> Result<DeltaAdd, anyhow::Error> {
    let file_stats = parquet_metadata_to_delta_stats(metadata, schema);
    let stats_json = delta_stats_to_json(&file_stats, schema)?;
    Ok(DeltaAdd {
        path: filename.to_string(),
        partition_values,
        size: size as i64,
        modification_time,
        data_change: true,
        stats: Some(stats_json),
        deletion_vector: None,
    })
}

fn file_path(
    base_path: &object_store::path::Path,
    log_path: &str,
) -> Result<object_store::path::Path, anyhow::Error> {
    let relative = store_path_from_uri(log_path)?.ok_or_else(|| {
        anyhow::anyhow!("references the absolute path {log_path}, which is outside the table root")
    })?;
    Ok(join_store_path(base_path, &relative))
}

fn version_to_log_filename(version: Version) -> String {
    format!("{:020}.json", version)
}

async fn cleanup_source_files(
    store: &Arc<dyn ObjectStore>,
    base_path: &object_store::path::Path,
    source_files: &[(String, parquet::file::metadata::ParquetMetaData, u64)],
) {
    for (filename, _, _) in source_files {
        let path = base_path.clone().join(filename.as_str());
        let _ = store.delete(&path).await;
    }
}

const MAX_COMMIT_RETRIES: usize = 10;
const MERGE_MAX_RETRIES: usize = 3;

fn build_overwrite_commit(
    table_state: &TableState,
    schema: &SchemaRef,
    adds: &[DeltaAdd],
    table_config: &TableConfig,
    engine: &dyn Engine,
) -> Result<(String, Version), anyhow::Error> {
    let next_version = table_state.version + 1;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let remove = DeltaRemove::from_scan_file(now_ms, &table_state.existing_dvs);
    let removes: Vec<_> = table_state
        .active_files
        .iter()
        .cloned()
        .map(remove)
        .collect::<Result<Vec<_>, _>>()?;

    let commit_body = Commit::for_table(
        Operation::Write,
        &table_state.snapshot,
        schema,
        table_config,
        engine,
        now_ms,
    )?
    .with_removes(removes)
    .with_adds(adds.to_vec())
    .to_jsonl()?;
    Ok((commit_body, next_version))
}

pub async fn overwrite_delta<S>(
    table_path: &str,
    schema: SchemaRef,
    stream: S,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
    table_config: &TableConfig,
    cloud_io: &Handle,
) -> Result<(), anyhow::Error>
where
    S: Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + 'static,
{
    ensure_table(table_path, &schema).await?;
    let mut sink = DeltaOverwriteSink::new(
        table_path,
        schema,
        max_rows,
        max_bytes,
        table_config,
        cloud_io,
    )?;
    let mut stream = std::pin::pin!(stream);
    while let Some(batch) = stream.next().await {
        sink.write_batch(&batch?).await?;
    }
    sink.finish().await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nonzero(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).unwrap()
    }

    fn carrying(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn a_property_the_table_lacks_is_added() {
        let config = TableConfig {
            target_file_size: Some(nonzero(1024)),
            ..Default::default()
        };
        let desired = configuration_with(&carrying(&[]), &config, &[]);
        assert_eq!(desired, carrying(&[("delta.targetFileSize", "1024")]));
    }

    #[test]
    fn a_property_whose_value_changed_is_rewritten() {
        let config = TableConfig {
            target_file_size: Some(nonzero(2048)),
            ..Default::default()
        };
        let current = carrying(&[("delta.targetFileSize", "1024")]);
        let desired = configuration_with(&current, &config, &[]);
        assert_eq!(desired, carrying(&[("delta.targetFileSize", "2048")]));
    }

    #[test]
    fn a_property_nobody_names_is_left_alone() {
        let current = carrying(&[("delta.appendOnly", "true")]);
        let desired = configuration_with(&current, &TableConfig::default(), &[]);
        assert_eq!(desired, current, "reconciling never removes");
    }

    #[test]
    fn ldrs_has_the_last_word_over_the_caller() {
        let config = TableConfig {
            target_file_size: Some(nonzero(1024)),
            ..Default::default()
        };
        let desired = configuration_with(
            &carrying(&[("delta.enableDeletionVectors", "false")]),
            &config,
            &[("delta.enableDeletionVectors", "true")],
        );
        assert_eq!(desired["delta.enableDeletionVectors"], "true");
        assert_eq!(desired["delta.targetFileSize"], "1024");
    }

    #[test]
    fn a_settable_property_is_parsed_into_its_field() {
        let mut config = TableConfig::default();
        config.set("delta.targetFileSize", "268435456").unwrap();
        config.set("delta.checkpointInterval", "100").unwrap();
        assert_eq!(config.target_file_size, Some(nonzero(268435456)));
        assert_eq!(config.checkpoint_interval, Some(nonzero(100)));
    }

    #[test]
    fn a_property_ldrs_manages_is_refused_by_name() {
        let error = TableConfig::default()
            .set("delta.enableDeletionVectors", "true")
            .unwrap_err()
            .to_string();
        assert!(error.contains("managed by ldrs"), "{error}");
    }

    #[test]
    fn a_property_ldrs_does_not_know_is_refused_as_unknown() {
        let error = TableConfig::default()
            .set("delta.enableChangeDataFeed", "true")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("not a table property ldrs can set"),
            "{error}"
        );
    }

    #[test]
    fn a_value_kernel_would_silently_drop_is_refused_here() {
        for value in ["banana", "0", "-1", "1.5"] {
            assert!(
                TableConfig::default()
                    .set("delta.targetFileSize", value)
                    .is_err(),
                "'{value}' should not be accepted"
            );
        }
    }

    fn recording(
        schema_string: &str,
        partition_columns: &[&str],
        configuration: &[(&str, &str)],
    ) -> DeltaMetadata {
        DeltaMetadata {
            id: "the-table".to_string(),
            format: DeltaFormat {
                provider: "parquet".to_string(),
                options: HashMap::new(),
            },
            schema_string: schema_string.to_string(),
            partition_columns: partition_columns.iter().map(|c| c.to_string()).collect(),
            created_time: 0,
            configuration: carrying(configuration),
        }
    }

    #[test]
    fn metadata_that_restates_the_table_has_not_moved() {
        let target = recording("{schema}", &["dt"], &[("delta.targetFileSize", "1024")]);
        assert!(!metadata_moved(
            "{schema}",
            &["dt".to_string()],
            &carrying(&[("delta.targetFileSize", "1024")]),
            &target
        ));
    }

    #[test]
    fn a_different_id_or_created_time_is_not_a_move() {
        let mut target = recording("{schema}", &[], &[]);
        target.id = "someone-elses".to_string();
        target.created_time = 999;
        assert!(!metadata_moved("{schema}", &[], &HashMap::new(), &target));
    }

    #[test]
    fn each_field_metadata_records_moves_it_on_its_own() {
        let current = ("{schema}", vec!["dt".to_string()], carrying(&[("a", "1")]));
        let unchanged = recording("{schema}", &["dt"], &[("a", "1")]);

        let cases = [
            ("schema", recording("{other}", &["dt"], &[("a", "1")])),
            ("partitions", recording("{schema}", &[], &[("a", "1")])),
            (
                "configuration",
                recording("{schema}", &["dt"], &[("a", "2")]),
            ),
        ];
        for (field, target) in cases {
            assert!(
                metadata_moved(current.0, &current.1, &current.2, &target),
                "a changed {field} should write metaData"
            );
        }
        assert!(!metadata_moved(
            current.0, &current.1, &current.2, &unchanged
        ));
    }

    fn declaring(reader: &[TableFeature], writer: &[TableFeature]) -> DeltaProtocol {
        DeltaProtocol {
            min_reader_version: MIN_READER_VERSION,
            min_writer_version: MIN_WRITER_VERSION,
            reader_features: reader.to_vec(),
            writer_features: writer.to_vec(),
        }
    }

    fn upgrade(
        current: DeltaProtocol,
        reader: &[TableFeature],
        writer: &[TableFeature],
    ) -> Option<DeltaProtocol> {
        let target = current
            .clone()
            .with_reader_features(reader)
            .with_writer_features(writer);
        protocol_upgrade(current, target)
    }

    #[test]
    fn protocol_upgrade_writes_nothing_when_the_table_already_declares_it() {
        let write = Operation::Write;
        let current = declaring(write.reader_features(), write.writer_features());
        assert_eq!(
            upgrade(current, write.reader_features(), write.writer_features()),
            None
        );
    }

    #[test]
    fn protocol_upgrade_declares_everything_for_a_new_table() {
        let merge = Operation::Merge;
        let target = upgrade(
            DeltaProtocol::none(),
            merge.reader_features(),
            merge.writer_features(),
        )
        .unwrap();
        assert_eq!(target.min_reader_version, MIN_READER_VERSION);
        assert_eq!(target.min_writer_version, MIN_WRITER_VERSION);
        assert_eq!(target.writer_features, merge.writer_features().to_vec());
    }

    #[test]
    fn protocol_upgrade_keeps_features_it_does_not_require() {
        let rowtracking = TableFeature::RowTracking;
        let current = declaring(
            &[TableFeature::TimestampWithoutTimezone],
            &[TableFeature::TimestampWithoutTimezone, rowtracking.clone()],
        );
        let merge = Operation::Merge;
        let target = upgrade(current, merge.reader_features(), merge.writer_features()).unwrap();
        assert_eq!(
            target.writer_features,
            vec![
                TableFeature::TimestampWithoutTimezone,
                rowtracking,
                TableFeature::DeletionVectors,
            ]
        );
    }

    #[test]
    fn protocol_upgrade_preserves_an_unknown_feature_verbatim() {
        let unknown = TableFeature::Unknown("someFutureFeature".to_string());
        let current = declaring(&[], &[unknown.clone()]);
        let target = upgrade(current, &[], Operation::Merge.writer_features()).unwrap();
        assert_eq!(target.writer_features[0], unknown);
        assert!(serde_json::to_string(&target)
            .unwrap()
            .contains("someFutureFeature"));
    }

    #[test]
    fn protocol_upgrade_ignores_the_order_the_table_declared() {
        let merge = Operation::Merge;
        let current = declaring(
            merge.reader_features(),
            &[
                TableFeature::DeletionVectors,
                TableFeature::TimestampWithoutTimezone,
            ],
        );
        assert_eq!(
            upgrade(current, merge.reader_features(), merge.writer_features()),
            None
        );
    }

    #[test]
    fn should_checkpoint_waits_for_the_first_interval() {
        assert!(!should_checkpoint(0, None, 10));
        assert!(!should_checkpoint(9, None, 10));
        assert!(should_checkpoint(10, None, 10));
    }

    #[test]
    fn should_checkpoint_measures_the_gap_from_the_last_one() {
        assert!(!should_checkpoint(15, Some(10), 10));
        assert!(should_checkpoint(20, Some(10), 10));
    }

    #[test]
    fn should_checkpoint_catches_up_after_skipped_versions() {
        assert!(should_checkpoint(25, Some(10), 10));
    }

    #[test]
    fn should_checkpoint_saturates_when_the_checkpoint_leads() {
        assert!(!should_checkpoint(5, Some(10), 10));
    }
}
