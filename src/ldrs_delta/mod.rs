mod dv;
mod merge;
mod stats;

pub use merge::{merge_delta, MergeConfig, MergeStats, TxnConfig};
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
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
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
    pub mode: DeltaMode,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum DeltaMode {
    Overwrite,
    Merge {
        merge_keys: Vec<String>,
        allow_null_keys: bool,
        txn: Option<MergeTxnConfig>,
    },
}

/// Raw (unrendered) txn config from YAML. String templates are rendered at dispatch
/// time against the execution context before constructing the runtime `TxnConfig`.
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum MergeTxnConfig {
    SourceWatermark {
        app_id: String,
        watermark_column: String,
    },
    ProcessingTime {
        app_id: String,
        batch_version: Option<String>,
    },
}

/// Look up a key under a namespaced form first (`prefix.key`), then fall back to bare `key`.
fn get_ns<'a>(value: &'a Value, prefix: &str, key: &str) -> Option<&'a Value> {
    let ns_key = format!("{}.{}", prefix, key);
    value.get(&ns_key).or(value.get(key))
}

impl TryFrom<&Value> for DeltaDestination {
    type Error = anyhow::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let name = value
            .get("name")
            .and_then(|v| String::deserialize(v).ok())
            .ok_or(anyhow::anyhow!("Missing name"))?;
        let columns = get_ns(value, "delta", "columns")
            .and_then(|c| Vec::<ColumnSpec>::deserialize(c).ok())
            .unwrap_or_default();
        let max_rows = get_ns(value, "delta", "max_rows")
            .and_then(|v| usize::deserialize(v).ok());
        let max_bytes = get_ns(value, "delta", "max_bytes")
            .and_then(|v| usize::deserialize(v).ok());

        // Merge mode detected by presence of merge_keys (namespaced or bare)
        let mode = if let Some(keys_value) = get_ns(value, "delta", "merge_keys") {
            let merge_keys = Vec::<String>::deserialize(keys_value)
                .map_err(|e| anyhow::anyhow!("merge_keys must be a list of strings: {}", e))?;
            if merge_keys.is_empty() {
                anyhow::bail!("merge_keys cannot be empty");
            }

            let allow_null_keys = get_ns(value, "delta", "allow_null_keys")
                .and_then(|v| bool::deserialize(v).ok())
                .unwrap_or(false);

            let app_id = get_ns(value, "delta", "app_id")
                .and_then(|v| String::deserialize(v).ok())
                .unwrap_or_else(|| "ldrs-merge-{{ name }}".to_string());

            let txn_mode = get_ns(value, "delta", "txn_mode")
                .and_then(|v| String::deserialize(v).ok());

            let txn = match txn_mode.as_deref() {
                Some("source_watermark") => {
                    let watermark_column = get_ns(value, "delta", "watermark_column")
                        .and_then(|v| String::deserialize(v).ok())
                        .ok_or(anyhow::anyhow!(
                            "source_watermark requires watermark_column"
                        ))?;
                    Some(MergeTxnConfig::SourceWatermark {
                        app_id,
                        watermark_column,
                    })
                }
                Some("processing_time") => {
                    let batch_version = get_ns(value, "delta", "batch_version")
                        .and_then(|v| String::deserialize(v).ok());
                    Some(MergeTxnConfig::ProcessingTime {
                        app_id,
                        batch_version,
                    })
                }
                Some(other) => anyhow::bail!("unknown txn_mode: {}", other),
                None => None,
            };

            DeltaMode::Merge {
                merge_keys,
                allow_null_keys,
                txn,
            }
        } else {
            DeltaMode::Overwrite
        };

        Ok(DeltaDestination {
            name,
            columns,
            max_rows,
            max_bytes,
            mode,
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
    fn from_scan_file(ts: i64) -> impl Fn(ScanFile) -> Self {
        move |file| DeltaRemove {
            path: file.path,
            deletion_timestamp: ts,
            data_change: true,
            size: file.size,
            deletion_vector: None,
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

fn merge_protocol() -> DeltaProtocol {
    DeltaProtocol {
        min_reader_version: 3,
        min_writer_version: 7,
        reader_features: vec!["timestampNtz".to_string(), "deletionVectors".to_string()],
        writer_features: vec!["timestampNtz".to_string(), "deletionVectors".to_string()],
    }
}

fn build_metadata(
    schema: &SchemaRef,
    table_id: Option<&str>,
    created_time: Option<i64>,
    configuration: HashMap<String, String>,
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
        configuration,
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
    let metadata = build_metadata(schema, None, None, HashMap::new())?;

    let actions = vec![
        DeltaAction::CommitInfo(&commit_info),
        DeltaAction::Protocol(&protocol),
        DeltaAction::MetaData(&metadata),
    ];

    let commit_body = build_commit_jsonl(&actions)?;
    let log_path = base_path
        .clone().join("_delta_log")
        .clone().join("00000000000000000000.json");

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
    has_deletion_vectors: bool,
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
    // we can read the property if dvs have been added
    // this either does not exist or has a value of true or false
    let has_deletion_vectors = snapshot
        .table_properties()
        .enable_deletion_vectors
        .unwrap_or(false);

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
        has_deletion_vectors,
    })
}

fn build_add(
    filename: &str,
    metadata: &parquet::file::metadata::ParquetMetaData,
    obj_meta: &object_store::ObjectMeta,
    schema: &SchemaRef,
) -> Result<DeltaAdd, anyhow::Error> {
    let file_stats = parquet_metadata_to_delta_stats(metadata, schema);
    let stats_json = delta_stats_to_json(&file_stats, schema)?;
    Ok(DeltaAdd {
        path: filename.to_string(),
        partition_values: HashMap::new(),
        size: obj_meta.size as i64,
        modification_time: obj_meta.last_modified.timestamp_millis(),
        data_change: true,
        stats: Some(stats_json),
        deletion_vector: None,
    })
}

fn version_to_log_filename(version: Version) -> String {
    format!("{:020}.json", version)
}

const DEFAULT_MAX_ROWS: usize = 1_000_000;
const DEFAULT_MAX_BYTES: usize = 128 * 1024 * 1024; // 128MB
const MAX_COMMIT_RETRIES: usize = 10;
const MERGE_MAX_RETRIES: usize = 3;

fn build_overwrite_commit(
    table_state: &TableState,
    schema: &SchemaRef,
    adds: &[DeltaAdd],
) -> Result<(String, Version), anyhow::Error> {
    let next_version = table_state.version + 1;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let remove = DeltaRemove::from_scan_file(now_ms);
    let removes: Vec<_> = table_state
        .active_files
        .iter()
        .cloned()
        .map(remove)
        .collect();

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
        HashMap::new(),
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

    let file_paths: Vec<_> = files
        .iter()
        .map(|(filename, _)| base_path.clone().join(filename.as_str()))
        .collect();

    let obj_metas = futures::future::try_join_all(
        file_paths.iter().map(|path| store.head(path)),
    )
    .await?;

    let adds = files
        .iter()
        .zip(obj_metas.iter())
        .map(|((filename, metadata), obj_meta)| build_add(filename, metadata, obj_meta, &schema))
        .collect::<Result<Vec<_>, _>>()?;

    for _attempt in 0..MAX_COMMIT_RETRIES {
        let table_state = snapshot_table_state(engine.as_ref(), &table_url)?;
        let (commit_body, next_version) = build_overwrite_commit(&table_state, &schema, &adds)?;
        let log_path = base_path
            .clone().join("_delta_log")
            .clone().join(version_to_log_filename(next_version));

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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> DeltaDestination {
        let value: Value = serde_yaml::from_str(yaml).unwrap();
        DeltaDestination::try_from(&value).unwrap()
    }

    #[test]
    fn overwrite_default_no_merge_keys() {
        let dest = parse("name: public.users");
        assert_eq!(dest.name, "public.users");
        assert_eq!(dest.mode, DeltaMode::Overwrite);
    }

    #[test]
    fn merge_mode_from_namespaced_keys() {
        let dest = parse(
            r#"
name: public.users
delta.merge_keys: [id]
"#,
        );
        match dest.mode {
            DeltaMode::Merge {
                merge_keys,
                allow_null_keys,
                txn,
            } => {
                assert_eq!(merge_keys, vec!["id".to_string()]);
                assert!(!allow_null_keys);
                assert!(txn.is_none());
            }
            _ => panic!("expected Merge mode"),
        }
    }

    #[test]
    fn merge_mode_from_bare_keys() {
        let dest = parse(
            r#"
name: public.users
merge_keys: [id, name]
"#,
        );
        match dest.mode {
            DeltaMode::Merge { merge_keys, .. } => {
                assert_eq!(merge_keys, vec!["id".to_string(), "name".to_string()]);
            }
            _ => panic!("expected Merge mode"),
        }
    }

    #[test]
    fn merge_source_watermark_parses() {
        let dest = parse(
            r#"
name: public.users
delta.merge_keys: [id]
delta.txn_mode: source_watermark
delta.watermark_column: updated_at
delta.app_id: "my-app"
"#,
        );
        match dest.mode {
            DeltaMode::Merge {
                txn: Some(MergeTxnConfig::SourceWatermark { app_id, watermark_column }),
                ..
            } => {
                assert_eq!(app_id, "my-app");
                assert_eq!(watermark_column, "updated_at");
            }
            _ => panic!("expected SourceWatermark txn"),
        }
    }

    #[test]
    fn merge_processing_time_parses() {
        let dest = parse(
            r#"
name: public.users
delta.merge_keys: [id]
delta.txn_mode: processing_time
delta.batch_version: "{{ run_id }}"
"#,
        );
        match dest.mode {
            DeltaMode::Merge {
                txn: Some(MergeTxnConfig::ProcessingTime { app_id, batch_version }),
                ..
            } => {
                // app_id defaulted to ldrs-merge-{{ name }} template (rendered later)
                assert_eq!(app_id, "ldrs-merge-{{ name }}");
                assert_eq!(batch_version.as_deref(), Some("{{ run_id }}"));
            }
            _ => panic!("expected ProcessingTime txn"),
        }
    }

    #[test]
    fn merge_allow_null_keys_parses() {
        let dest = parse(
            r#"
name: public.users
delta.merge_keys: [id]
delta.allow_null_keys: true
"#,
        );
        match dest.mode {
            DeltaMode::Merge { allow_null_keys, .. } => assert!(allow_null_keys),
            _ => panic!("expected Merge mode"),
        }
    }

    #[test]
    fn unknown_txn_mode_errors() {
        let value: Value = serde_yaml::from_str(
            r#"
name: public.users
delta.merge_keys: [id]
delta.txn_mode: bogus
"#,
        )
        .unwrap();
        assert!(DeltaDestination::try_from(&value).is_err());
    }

    #[test]
    fn empty_merge_keys_errors() {
        let value: Value = serde_yaml::from_str(
            r#"
name: public.users
delta.merge_keys: []
"#,
        )
        .unwrap();
        assert!(DeltaDestination::try_from(&value).is_err());
    }
}
