mod stats;

pub use stats::{delta_stats_to_json, parquet_metadata_to_delta_stats, ColumnStats, DeltaStats};

use crate::types::ColumnSpec;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::schema::{DataType as DeltaDataType, StructField, StructType};
use delta_kernel::{Engine, Snapshot, Version};
use futures::Stream;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use uuid::Uuid;

use crate::arrow_access::arrow_transforms::ArrowColumnTransformStrategy;
use crate::ldrs_parquet::{default_writer_props, write_parquet_split};
use crate::storage::StorageProvider;

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct DeltaDestination {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub max_rows: Option<usize>,
    pub max_bytes: Option<usize>,
}

impl TryFrom<&Value> for DeltaDestination {
    type Error = anyhow::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let name = value
            .get("name")
            .and_then(|v| String::deserialize(v).ok())
            .ok_or(anyhow::anyhow!("Missing name"))?;
        let columns = value
            .get("delta.columns")
            .or(value.get("columns"))
            .and_then(|c| Vec::<ColumnSpec>::deserialize(c).ok())
            .unwrap_or_default();
        let max_rows = value
            .get("delta.max_rows")
            .or(value.get("max_rows"))
            .and_then(|v| usize::deserialize(v).ok());
        let max_bytes = value
            .get("delta.max_bytes")
            .or(value.get("max_bytes"))
            .and_then(|v| usize::deserialize(v).ok());

        Ok(DeltaDestination {
            name,
            columns,
            max_rows,
            max_bytes,
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeltaCommitInfo {
    timestamp: i64,
    operation: String,
    operation_parameters: HashMap<String, String>,
    engine_info: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeltaProtocol {
    min_reader_version: i32,
    min_writer_version: i32,
    reader_features: Vec<String>,
    writer_features: Vec<String>,
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeltaAdd {
    path: String,
    partition_values: HashMap<String, String>,
    size: i64,
    modification_time: i64,
    data_change: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeltaRemove {
    path: String,
    deletion_timestamp: i64,
    data_change: bool,
    size: i64,
}

impl DeltaRemove {
    fn from_scan_file(ts: i64) -> impl Fn(ScanFile) -> Self {
        move |file| DeltaRemove {
            path: file.path,
            deletion_timestamp: ts,
            data_change: true,
            size: file.size,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
enum DeltaAction<'a> {
    CommitInfo(&'a DeltaCommitInfo),
    Protocol(&'a DeltaProtocol),
    MetaData(&'a DeltaMetadata),
    Add(&'a DeltaAdd),
    Remove(&'a DeltaRemove),
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

fn default_protocol() -> DeltaProtocol {
    DeltaProtocol {
        min_reader_version: 3,
        min_writer_version: 7,
        reader_features: vec!["timestampNtz".to_string()],
        writer_features: vec!["timestampNtz".to_string()],
    }
}

fn build_metadata(
    schema: &SchemaRef,
    table_id: Option<&str>,
    created_time: Option<i64>,
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
        partition_columns: vec![],
        created_time: created_time.unwrap_or_else(|| chrono::Utc::now().timestamp_millis()),
        configuration: HashMap::new(),
    })
}

pub async fn ensure_table(table_path: &str, schema: &SchemaRef) -> Result<(), anyhow::Error> {
    let storage = StorageProvider::try_from_string(table_path)?;
    let (store, base_path) = storage.get_store_and_path()?;

    let now_ms = chrono::Utc::now().timestamp_millis();

    let commit_info = DeltaCommitInfo {
        timestamp: now_ms,
        operation: "CREATE TABLE".to_string(),
        operation_parameters: HashMap::new(),
        engine_info: format!("ldrs-{}", env!("CARGO_PKG_VERSION")),
    };

    let protocol = default_protocol();
    let metadata = build_metadata(schema, None, None)?;

    let actions = vec![
        DeltaAction::CommitInfo(&commit_info),
        DeltaAction::Protocol(&protocol),
        DeltaAction::MetaData(&metadata),
    ];

    let commit_body = build_commit_jsonl(&actions)?;
    let log_path = base_path
        .child("_delta_log")
        .child("00000000000000000000.json");

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

fn build_engine(store: Arc<dyn ObjectStore>) -> Arc<dyn Engine> {
    Arc::new(DefaultEngineBuilder::new(store).build())
}

struct TableState {
    version: Version,
    active_files: Vec<ScanFile>,
    table_id: String,
    created_time: Option<i64>,
}

fn snapshot_table_state(
    engine: &dyn Engine,
    table_url: &url::Url,
) -> Result<TableState, anyhow::Error> {
    let snapshot = Snapshot::builder_for(table_url.clone()).build(engine)?;
    let version = snapshot.version();
    let metadata = snapshot.table_configuration().metadata();
    let table_id = metadata.id().to_string();
    let created_time = metadata.created_time();

    let scan = snapshot.clone().scan_builder().build()?;

    let mut active_files = Vec::new();

    for scan_metadata in scan.scan_metadata(engine)? {
        let scan_metadata = scan_metadata?;
        fn collect_file(files: &mut Vec<ScanFile>, scan_file: ScanFile) {
            files.push(scan_file);
        }
        active_files = scan_metadata.visit_scan_files(active_files, collect_file)?;
    }

    Ok(TableState {
        version,
        active_files,
        table_id,
        created_time,
    })
}

fn version_to_log_filename(version: Version) -> String {
    format!("{:020}.json", version)
}

const DEFAULT_MAX_ROWS: usize = 1_000_000;
const DEFAULT_MAX_BYTES: usize = 128 * 1024 * 1024; // 128MB
const MAX_COMMIT_RETRIES: usize = 10;

fn build_overwrite_commit(
    table_state: &TableState,
    schema: &SchemaRef,
    adds: &[DeltaAdd],
) -> Result<(String, Version), anyhow::Error> {
    let next_version = table_state.version + 1;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let remove = DeltaRemove::from_scan_file(now_ms);
    let removes: Vec<_> = table_state.active_files.iter().cloned().map(remove).collect();

    let commit_info = DeltaCommitInfo {
        timestamp: now_ms,
        operation: "WRITE".to_string(),
        operation_parameters: HashMap::from([("mode".to_string(), "Overwrite".to_string())]),
        engine_info: format!("ldrs-{}", env!("CARGO_PKG_VERSION")),
    };

    let protocol = default_protocol();
    let metadata = build_metadata(
        schema,
        Some(&table_state.table_id),
        table_state.created_time,
    )?;

    let mut actions: Vec<DeltaAction> = vec![
        DeltaAction::CommitInfo(&commit_info),
        DeltaAction::Protocol(&protocol),
        DeltaAction::MetaData(&metadata),
    ];
    actions.extend(removes.iter().map(DeltaAction::Remove));
    actions.extend(adds.iter().map(DeltaAction::Add));

    let commit_body = build_commit_jsonl(&actions)?;
    Ok((commit_body, next_version))
}

pub async fn overwrite_delta<S>(
    table_path: &str,
    schema: SchemaRef,
    transforms: Vec<Option<ArrowColumnTransformStrategy>>,
    stream: S,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
) -> Result<(), anyhow::Error>
where
    S: Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + 'static,
{
    ensure_table(table_path, &schema).await?;

    let storage = StorageProvider::try_from_string(table_path)?;
    let (store, base_path) = storage.get_store_and_path()?;
    let table_url = storage.get_url();
    let engine = build_engine(store.clone());

    let files = write_parquet_split(
        table_path,
        schema.clone(),
        transforms,
        stream,
        Some(max_rows.unwrap_or(DEFAULT_MAX_ROWS)),
        Some(max_bytes.unwrap_or(DEFAULT_MAX_BYTES)),
        |_| format!("{}.parquet", Uuid::new_v4()),
        Some(default_writer_props()),
    )
    .await?;

    let mut adds = Vec::with_capacity(files.len());
    for (filename, parquet_metadata) in files {
        let file_path = base_path.child(filename.as_str());
        let obj_meta = store.head(&file_path).await?;
        let file_stats = parquet_metadata_to_delta_stats(&parquet_metadata, &schema);
        let stats_json = delta_stats_to_json(&file_stats, &schema)?;
        adds.push(DeltaAdd {
            path: filename,
            partition_values: HashMap::new(),
            size: obj_meta.size as i64,
            modification_time: obj_meta.last_modified.timestamp_millis(),
            data_change: true,
            stats: Some(stats_json),
        });
    }

    for _attempt in 0..MAX_COMMIT_RETRIES {
        let table_state = snapshot_table_state(engine.as_ref(), &table_url)?;
        let (commit_body, next_version) = build_overwrite_commit(&table_state, &schema, &adds)?;
        let log_path = base_path
            .child("_delta_log")
            .child(version_to_log_filename(next_version));

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
            Ok(_) => return Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }
    anyhow::bail!("failed to commit delta overwrite after {MAX_COMMIT_RETRIES} attempts")
}
