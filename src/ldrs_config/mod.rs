pub mod config;

use std::pin::pin;

use anyhow::Context;
use arrow_array::RecordBatch;
use futures::Stream;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStream};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};
use url::Url;

use crate::{
    arrow_access::arrow_transforms::{
        build_arrow_transform_strategy, ArrowColumnTransformStrategy,
    },
    ldrs_arrow::build_source_and_target_schema,
    ldrs_config::config::{get_parsed_config, LdrsConfig, LdrsDestination, LdrsSource},
    ldrs_env::{collect_params, LdrsExecutionContext},
    ldrs_postgres::{client::check_for_role, postgres_execution::load_to_postgres},
    ldrs_snowflake::{sf_arrow_stream, snowflake_source::SFSource},
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
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.get_mut() {
            StreamType::Parquet(stream) => pin!(stream)
                .poll_next(cx)
                .map(|opt| opt.map(|res| res.map_err(Into::into))),
            StreamType::Receiver(stream) => pin!(stream).poll_next(cx),
        }
    }
}

struct LdrsSrcStream {
    schema: Option<arrow_schema::SchemaRef>,
    stream_type: StreamType,
    source_cols: Vec<ColumnSpec>,
    cleanup_handle: Option<JoinHandle<Result<(), anyhow::Error>>>,
}

pub fn get_env_value<'a>(
    vars: &'a [(String, String)],
    keys: &[&str],
) -> Option<&'a (String, String)> {
    keys.iter()
        .find_map(|key| vars.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)))
}

pub fn get_src_url<'a>(
    vars: &'a [(String, String)],
    name: &str,
    prefix: &str,
) -> Result<&'a (String, String), anyhow::Error> {
    let fqn = format!("LDRS_SRC_{}", name);
    let prefix_key = format!("LDRS_SRC_{}", prefix);
    get_env_value(vars, &[&fqn, &prefix_key, "LDRS_SRC"]).ok_or_else(|| {
        anyhow::anyhow!("No env var found for {} or {} or LDRS_SRC", fqn, prefix_key)
    })
}

pub fn get_dest_url<'a>(
    vars: &'a [(String, String)],
    name: &str,
    prefix: &str,
) -> Result<&'a (String, String), anyhow::Error> {
    let fqn = format!("LDRS_DEST_{}", name);
    let prefix_key = format!("LDRS_DEST_{}", prefix);
    get_env_value(vars, &[&fqn, &prefix_key, "LDRS_DEST"]).ok_or_else(|| {
        anyhow::anyhow!(
            "No env var found for {} or {} or LDRS_DEST",
            fqn,
            prefix_key
        )
    })
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

    // get all possible environment params
    let env_params = collect_params(ldrs_env);
    debug!("Environment Params: {:?}", env_params);
    let handlebars = handlebars::Handlebars::new();

    let total_tasks = table_tasks.len();
    for (i, task) in table_tasks.into_iter().enumerate() {
        let task_start = std::time::Instant::now();
        debug!("Task: {:?}", task);
        info!("Running task: {}/{}", i + 1, total_tasks);
        let (src, context) = match task.src {
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
                let ldrs_context = LdrsExecutionContext::try_new(&file.name, &handlebars)?;
                (
                    LdrsSrcStream {
                        schema: Some(schema),
                        stream_type: StreamType::Parquet(stream),
                        source_cols: dest_cols,
                        cleanup_handle: None,
                    },
                    ldrs_context,
                )
            }
            LdrsSource::SF(sf) => {
                let sf_sql = match &sf {
                    SFSource::Query(sql) => sql.sql.clone(),
                    SFSource::Table(table) => format!("SELECT * FROM {}", table.name),
                };
                let name = sf.get_name();
                let sf_src = get_src_url(ldrs_env, &name, "sf")?;
                let ldrs_context = LdrsExecutionContext::try_new(&name, &handlebars)?;
                let handled_name = ldrs_context
                    .handlebars
                    .render_template("{{ shoutySnakeCase name }}", &ldrs_context.context)?;
                let sf_params = sf.try_get_env_params(&handled_name, &env_params)?;
                debug!("Snowflake Params: {:?}", sf_params);
                let arrow_stream = sf_arrow_stream(sf_src.1.as_str(), &sf_sql, sf_params).await?;
                let schema = arrow_stream.schema_stream.await;
                (
                    LdrsSrcStream {
                        schema,
                        stream_type: StreamType::Receiver(arrow_stream.batch_stream),
                        source_cols: vec![],
                        cleanup_handle: Some(arrow_stream.command_handle),
                    },
                    ldrs_context,
                )
            }
        };

        let _ = match src.schema {
            Some(schema) => match task.dest {
                LdrsDestination::Pg(pg_dest) => {
                    let dest_value = get_dest_url(ldrs_env, pg_dest.get_name(), "pg")?;
                    let (pg_url, role) = check_for_role(dest_value.1.as_str())?;
                    // check the environment for a pg role
                    let role = role.or(get_env_value(
                        ldrs_env,
                        &[
                            &format!("LDRS_PG_ROLE_{}", pg_dest.get_name()),
                            "LDRS_PG_ROLE",
                        ],
                    )
                    .map(|(_, v)| v.to_string()));
                    // custom overrides from the source
                    let source_cols = src
                        .source_cols
                        .iter()
                        .map(ColumnSchema::from)
                        .collect::<Vec<_>>();

                    let (src_cols, target_cols) = build_source_and_target_schema(
                        &schema,
                        source_cols,
                        vec![pg_dest.get_columns()],
                    )?;
                    let strategies: Vec<Option<ArrowColumnTransformStrategy>> = src_cols
                        .iter()
                        .zip(target_cols.iter())
                        .zip(schema.fields().iter())
                        .map(|((source, target), field)| {
                            build_arrow_transform_strategy(source, target, field.data_type())
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    info!("Target columns: {:?}", target_cols);
                    info!("Arrow Transforms: {:?}", strategies);
                    // collect the params that can be bound
                    let env_params = collect_params(ldrs_env);
                    let pg_commands = pg_dest.to_pg_commands();
                    load_to_postgres(
                        &pg_url,
                        &pg_commands,
                        &target_cols,
                        &strategies,
                        &env_params,
                        role,
                        &context,
                        src.stream_type,
                    )
                    .await
                }
            },
            None => {
                info!("No schema found, most likely the load failed");
                Ok(())
            }
        }?;

        if let Some(handle) = src.cleanup_handle {
            match handle.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e),
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
    use crate::types::ColumnType;

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

    #[test]
    fn test_collect_params() {
        let simple_env = vec![
            ("LDRS_PARAM_P1".to_string(), "value1".to_string()),
            ("LDRS_PARAM_P2".to_string(), "value2".to_string()),
        ];
        let params = collect_params(&simple_env);
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], ("P1".to_string(), "value1".to_string(), None));

        let with_type = vec![
            ("LDRS_PARAM_P1_UUID".to_string(), "value1".to_string()),
            (
                "LDRS_PARAM_P2_TIMESTAMP".to_string(),
                "2023-01-01T00:00:00Z".to_string(),
            ),
        ];
        let params = collect_params(&with_type);
        assert_eq!(params.len(), 2);
        assert_eq!(
            params[0],
            (
                "P1".to_string(),
                "value1".to_string(),
                Some(ColumnType::Uuid)
            )
        );
        assert_eq!(
            params[1],
            (
                "P2".to_string(),
                "2023-01-01T00:00:00Z".to_string(),
                Some(ColumnType::Timestamp(crate::types::TimeUnit::Micros))
            )
        );
    }
}
