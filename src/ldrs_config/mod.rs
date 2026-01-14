pub mod config;

use std::pin::Pin;

use anyhow::Context;
use arrow_array::RecordBatch;
use futures::Stream;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStream};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};
use url::Url;

use crate::{
    ldrs_arrow::build_final_schema,
    ldrs_config::config::{get_parsed_config, LdrsConfig, LdrsDestination, LdrsSource},
    ldrs_postgres::postgres_execution::{collect_params, load_to_postgres},
    ldrs_snowflake::{sf_arrow_stream, snowflake_source::SFSource, SnowflakeConnection},
    ldrs_storage::is_object_store_url,
    parquet_provider::builder_from_url,
    pq::get_fields,
    storage::base_or_relative_path,
    types::{ColumnSchema, ColumnSpec},
};

enum StreamType {
    Parquet(ParquetRecordBatchStream<ParquetObjectReader>),
    Receiver(ReceiverStream<Result<RecordBatch, anyhow::Error>>),
}

impl Stream for StreamType {
    type Item = Result<RecordBatch, anyhow::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.get_mut() {
            StreamType::Parquet(stream) => Pin::new(stream)
                .poll_next(cx)
                .map(|opt| opt.map(|res| res.map_err(Into::into))),
            StreamType::Receiver(stream) => Pin::new(stream).poll_next(cx),
        }
    }
}

struct LdrsSrcStream {
    schema: arrow_schema::SchemaRef,
    stream_type: StreamType,
    source_cols: Vec<ColumnSpec>,
    cleanup_handle: Option<JoinHandle<Result<(), anyhow::Error>>>,
}

pub fn get_all_ldrs_env_vars() -> Vec<(String, String)> {
    std::env::vars()
        .filter(|(key, _)| key.starts_with("LDRS_"))
        .collect()
}

pub fn get_env_url<'a>(
    vars: &'a [(String, String)],
    name: &str,
    prefix: &str,
    ldrs_type: &str,
) -> Result<&'a (String, String), anyhow::Error> {
    let fqn = format!("{}_{}", ldrs_type, name);
    let prefix_key = format!("{}_{}", ldrs_type, prefix);
    let src = vars
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(&fqn))
        .or_else(|| {
            vars.iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(&prefix_key))
        })
        .or_else(|| {
            vars.iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(ldrs_type))
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "No env var found for {} or {} or {}",
                fqn,
                prefix_key,
                ldrs_type
            )
        })?;

    Ok(src)
}

pub fn get_src_url<'a>(
    vars: &'a [(String, String)],
    name: &str,
    prefix: &str,
) -> Result<&'a (String, String), anyhow::Error> {
    get_env_url(vars, name, prefix, "LDRS_SRC")
}

pub fn get_dest_url<'a>(
    vars: &'a [(String, String)],
    name: &str,
    prefix: &str,
) -> Result<&'a (String, String), anyhow::Error> {
    get_env_url(vars, name, prefix, "LDRS_DEST")
}

pub fn infer_env_type(env_type: &str, vars: &[(String, String)]) -> Option<String> {
    let src = vars
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(env_type));
    let url = src.and_then(|(_, value)| Url::parse(value).ok());
    match url {
        Some(u) => match u {
            u if is_object_store_url(&u) => Some("file".to_string()),
            u if u.scheme() == "postgres" => Some("pg".to_string()),
            _ => None,
        },
        None => None,
    }
}

pub async fn create_ldrs_exec(
    config_string: &str,
    ldrs_env: &[(String, String)],
    cloud_io_rt: &tokio::runtime::Handle,
) -> Result<(), anyhow::Error> {
    let config: LdrsConfig =
        serde_yaml::from_str(config_string).with_context(|| "Could not parse the config")?;

    let src_default = config.src.or(infer_env_type("LDRS_SRC", ldrs_env));
    let dest_default = config.dest.or(infer_env_type("LDRS_DEST", ldrs_env));

    let table_tasks = config
        .tables
        .into_iter()
        .map(|t| get_parsed_config(&src_default, &dest_default, t))
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    let total_tasks = table_tasks.len();
    for (i, task) in table_tasks.into_iter().enumerate() {
        let task_start = std::time::Instant::now();
        debug!("Task: {:?}", task);
        info!("Running task: {}/{}", i + 1, total_tasks);
        let src = match task.src {
            LdrsSource::File(file) => {
                // if the filename is not provided, use the name as the filename
                let file_name = file.filename.as_deref().unwrap_or(file.name.as_str());
                let src_value = get_src_url(ldrs_env, &file.name, "file")?;
                let joined = format!("{}{}", src_value.1.as_str(), file_name);
                let src_url = base_or_relative_path(&joined)?;
                let builder = builder_from_url(src_url.clone(), cloud_io_rt.clone()).await?;

                let schema = builder.schema().clone();

                let file_md = builder.metadata().clone();
                let fields = get_fields(file_md.file_metadata())?;

                let stream = builder.with_batch_size(1024).build()?;
                let dest_cols = fields
                    .iter()
                    .filter_map(|pq| ColumnSpec::try_from(pq).ok())
                    .collect::<Vec<_>>();
                LdrsSrcStream {
                    schema,
                    stream_type: StreamType::Parquet(stream),
                    source_cols: dest_cols,
                    cleanup_handle: None,
                }
            }
            LdrsSource::SF(sf) => {
                let sf_sql = match &sf {
                    SFSource::Query(sql) => sql.sql.clone(),
                    SFSource::Table(table) => format!("SELECT * FROM {}", table.name),
                };
                let sf_src = get_src_url(ldrs_env, &sf.get_name(), "sf")?;
                let arrow_stream = sf_arrow_stream(sf_src.1.as_str(), &sf_sql).await?;
                let schema = arrow_stream.schema_stream.await.ok_or_else(|| {
                    anyhow::Error::msg("Failed to load schema. Most likely ldrs-sf failed")
                })?;
                LdrsSrcStream {
                    schema,
                    stream_type: StreamType::Receiver(arrow_stream.batch_stream),
                    source_cols: vec![],
                    cleanup_handle: Some(arrow_stream.command_handle),
                }
            }
            _ => unreachable!(),
        };

        let dest = match task.dest {
            LdrsDestination::Pg(pg_dest) => {
                let dest_value = get_dest_url(ldrs_env, pg_dest.get_name(), "pg")?;
                let pg_url = base_or_relative_path(dest_value.1.as_str())?;
                let source_cols = src
                    .source_cols
                    .iter()
                    .map(ColumnSchema::from)
                    .collect::<Vec<_>>();
                let (final_cols, transforms) =
                    build_final_schema(&src.schema, vec![source_cols, pg_dest.get_columns()])?;
                info!("Final columns: {:?}", final_cols);
                info!("Transforms: {:?}", transforms);
                // collect the params that can be bound
                let env_params = collect_params(ldrs_env);
                let pg_commands = pg_dest.to_pg_commands();
                let exec = load_to_postgres(
                    &pg_url,
                    pg_dest.get_name(),
                    &pg_commands,
                    &final_cols,
                    &transforms,
                    &env_params,
                    src.stream_type,
                )
                .await?;
            }
        };

        if let Some(handle) = src.cleanup_handle {
            match handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(anyhow::anyhow!("ldrs-sf failed: {}", e)),
                Err(e) => Err(anyhow::anyhow!("ldrs-sf task panicked: {}", e)),
            }?
        }
        let task_end = std::time::Instant::now();
        info!("Task time: {:?}", task_end - task_start);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_src_url() {
        let vars = vec![
            ("LDRS_SRC".to_string(), "https://example.com".to_string()),
            (
                "LDRS_SRC_PREFIX".to_string(),
                "https://prefix.com".to_string(),
            ),
            ("LDRS_SRC_NAME".to_string(), "https://name.com".to_string()),
        ];

        assert_eq!(
            get_src_url(&vars, "NAME", "PREFIX").unwrap(),
            &("LDRS_SRC_NAME".to_string(), "https://name.com".to_string())
        );
        assert_eq!(
            get_src_url(&vars, "NO_MATCH", "PREFIX").unwrap(),
            &(
                "LDRS_SRC_PREFIX".to_string(),
                "https://prefix.com".to_string()
            )
        );
        assert_eq!(
            get_src_url(&vars, "NO_MATCH", "NO_MATCH").unwrap(),
            &("LDRS_SRC".to_string(), "https://example.com".to_string())
        );
    }
}
