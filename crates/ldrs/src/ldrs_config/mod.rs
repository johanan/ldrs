pub mod config;
pub mod field_validation;

use std::{
    collections::HashMap,
    io::{self, IsTerminal},
    pin::pin,
    sync::Arc,
};

use anyhow::Context;
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use deadpool_postgres::Pool;
use futures::{future::join_all, Stream};
use ldrs_arrow::{
    build_arrow_transform_strategy, build_source_and_target_schema, ArrowColumnTransformStrategy,
    ColumnSpec, ColumnType,
};
use ldrs_delta::{ensure_table, DeltaMergeSink, DeltaOverwriteSink, MergeConfig, TxnConfig};
use ldrs_parquet::{
    builder_from_url, columnspec_from_parquet, default_writer_props, get_fields,
    with_bloom_filters, FileNamer, ParquetSink,
};
use ldrs_postgres::{build_pg_pool, check_for_role};
use ldrs_storage::join_into_url;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStream};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::{
    delta::{DeltaDestination, DeltaMerge, MergeTxnConfig, TxnMode},
    finalize::{call_finalize, run_sf, FinalizeItem, SfCommand},
    ldrs_config::config::{
        parse_table, validate_configs, LdrsConfig, LdrsDestination, LdrsParsedConfig, LdrsSource,
    },
    ldrs_env::{
        collect_params, collect_vars_by_prefix, setup_handlebars, shouty, LdrsExecutionContext,
    },
    ldrs_snowflake::{sf_arrow_stream, snowflake_source::SFSource, SnowflakeConnection},
    phase::{DeltaStrategy, DestinationOutcome, FileWritten, PhaseOutput},
    postgres::{execute::PgSink, postgres_destination::split_pg_plan},
    sink::{drive, ArrowStdoutSink, BatchTransform, Sink, Transforms},
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
    source_files: Option<Vec<String>>,
}

struct ExecutionEnv<'a> {
    ldrs_env: &'a [(String, String)],
    handlebars: handlebars::Handlebars<'a>,
    handlebars_vars: Vec<(String, String)>,
    typed_params: Vec<(String, String, Option<ColumnType>)>,
}

impl<'a> ExecutionEnv<'a> {
    fn create(ldrs_env: &'a [(String, String)]) -> Self {
        let env_params = collect_params(ldrs_env);
        debug!("Environment Params: {:?}", env_params);
        let mut handlebars = handlebars::Handlebars::new();
        setup_handlebars(&mut handlebars);
        let handlebars_vars = collect_vars_by_prefix(ldrs_env, "TEMPL");
        debug!("Handlebars Vars: {:?}", handlebars_vars);
        ExecutionEnv {
            ldrs_env,
            handlebars,
            handlebars_vars,
            typed_params: env_params,
        }
    }
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
    ident: &str,
    prefix: &str,
) -> Result<&'a (String, String), anyhow::Error> {
    // identity (raw, then screaming-snake for OS env), then kind, then the bare fallback
    let raw = format!("LDRS_SRC_{}", ident);
    let snake = format!("LDRS_SRC_{}", shouty(ident));
    let prefix_key = format!("LDRS_SRC_{}", prefix);
    get_env_value(vars, &[&raw, &snake, &prefix_key, "LDRS_SRC"]).ok_or_else(|| {
        anyhow::anyhow!(
            "No env var found for {}, {}, {}, or LDRS_SRC",
            raw,
            snake,
            prefix_key
        )
    })
}

pub fn get_dest_url<'a>(
    vars: &'a [(String, String)],
    ident: &str,
    prefix: &str,
) -> Result<&'a (String, String), anyhow::Error> {
    // identity (raw, then screaming-snake for OS env), then kind, then the bare fallback
    let raw = format!("LDRS_DEST_{}", ident);
    let snake = format!("LDRS_DEST_{}", shouty(ident));
    let prefix_key = format!("LDRS_DEST_{}", prefix);
    get_env_value(vars, &[&raw, &snake, &prefix_key, "LDRS_DEST"]).ok_or_else(|| {
        anyhow::anyhow!(
            "No env var found for {}, {}, {}, or LDRS_DEST",
            raw,
            snake,
            prefix_key
        )
    })
}

fn is_object_store_url(url: &Url) -> bool {
    matches!(
        url.scheme(),
        "file" | "az" | "adl" | "azure" | "abfs" | "abfss" | "https"
    )
}

pub fn infer_env_type(env_type: &str, vars: &[(String, String)]) -> Option<String> {
    let src = vars
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(env_type));
    let url = src.and_then(|(_, value)| Url::parse(value).ok());
    match url {
        Some(u) => match u {
            u if u.scheme().starts_with("snowflake") => Some("sf".to_string()),
            u if u.scheme().starts_with("delta+") => Some("delta".to_string()),
            u if is_object_store_url(&u) => Some("file".to_string()),
            u if u.scheme() == "postgres" => Some("pg".to_string()),
            _ => None,
        },
        None => None,
    }
}

fn resolve_txn_config(
    yaml: Option<MergeTxnConfig>,
    context: &LdrsExecutionContext,
) -> Result<TxnConfig, anyhow::Error> {
    match yaml {
        None => Ok(TxnConfig::None),
        Some(MergeTxnConfig::SourceWatermark {
            app_id,
            watermark_column,
        }) => Ok(TxnConfig::SourceWatermark {
            app_id: context.render_template(&app_id)?,
            watermark_column,
        }),
        Some(MergeTxnConfig::ProcessingTime {
            app_id,
            batch_version,
        }) => {
            let batch_version = batch_version
                .map(|s| context.render_template(&s))
                .transpose()?
                .map(|s| {
                    s.parse::<i64>()
                        .map_err(|e| anyhow::anyhow!("batch_version must parse as i64: {}", e))
                })
                .transpose()?;
            Ok(TxnConfig::ProcessingTime {
                app_id: context.render_template(&app_id)?,
                batch_version,
            })
        }
    }
}

fn column_helper(
    source_cols: Vec<ColumnSpec>,
    dest_cols: Vec<ColumnSpec>,
    schema: &SchemaRef,
) -> Result<
    (
        Vec<ColumnSpec>,
        Vec<std::option::Option<ArrowColumnTransformStrategy>>,
    ),
    anyhow::Error,
> {
    let (src_cols, target_cols) =
        build_source_and_target_schema(schema, source_cols, vec![dest_cols])?;
    let strategies: Vec<Option<ArrowColumnTransformStrategy>> = src_cols
        .iter()
        .zip(target_cols.iter())
        .zip(schema.fields().iter())
        .map(|((source, target), field)| {
            build_arrow_transform_strategy(source, target, field.data_type())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((target_cols, strategies))
}

pub fn parse_yaml_config(
    config_string: &str,
    ldrs_env: &[(String, String)],
) -> Result<Vec<LdrsParsedConfig>, anyhow::Error> {
    let config: LdrsConfig =
        serde_yaml::from_str(config_string).with_context(|| "Could not parse the config")?;

    let src_default = config.src.clone().or(infer_env_type("LDRS_SRC", ldrs_env));
    let dest_default = config
        .dest
        .clone()
        .or(infer_env_type("LDRS_DEST", ldrs_env));

    config
        .tables
        .iter()
        .map(|t| parse_table(t.clone(), &config, &src_default, &dest_default))
        .collect::<Result<Vec<_>, anyhow::Error>>()
}

pub async fn execute_configs(
    tasks: Vec<LdrsParsedConfig>,
    select: Option<Vec<String>>,
    ldrs_env: &[(String, String)],
    cloud_io_rt: &tokio::runtime::Handle,
) -> Result<(), anyhow::Error> {
    let filtered_tasks: Vec<_> = match select {
        Some(selected_tables) => tasks
            .into_iter()
            .filter(|t| {
                selected_tables
                    .iter()
                    .any(|s| s.eq_ignore_ascii_case(t.src.name()))
            })
            .collect(),
        None => tasks,
    };
    debug!("Tasks to be run {:?}", filtered_tasks);

    validate_configs(&filtered_tasks)?;

    let exec_env = ExecutionEnv::create(ldrs_env);
    // One connection pool per database URL, shared across every task for the whole run.
    let mut pg_pools: HashMap<String, Pool> = HashMap::new();
    let total_tasks = filtered_tasks.len();
    for (i, task) in filtered_tasks.into_iter().enumerate() {
        let task_start = std::time::Instant::now();
        debug!("Task: {:?}", task);
        let task_name = task.src.name();
        for u in &task.unknown_keys {
            match u.suggestions.as_slice() {
                [] => warn!(
                    "table '{}': '{}' is not a known field (see `ldrs schema`)",
                    task_name, u.key
                ),
                [one] => warn!(
                    "table '{}': '{}' is not a known field (did you mean '{}'? see `ldrs schema`)",
                    task_name, u.key, one
                ),
                many => {
                    let joined = many
                        .iter()
                        .map(|s| format!("'{}'", s))
                        .collect::<Vec<_>>()
                        .join(", ");
                    warn!(
                        "table '{}': '{}' is not a known field (did you mean one of {}? see `ldrs schema`)",
                        task_name, u.key, joined
                    );
                }
            }
        }
        info!("Running task: {}/{}", i + 1, total_tasks);
        let rows = execute_task(
            task,
            exec_env.ldrs_env,
            &exec_env.handlebars,
            &exec_env.handlebars_vars,
            &exec_env.typed_params,
            cloud_io_rt,
            &mut pg_pools,
        )
        .await?;
        let task_end = std::time::Instant::now();
        info!(
            rows,
            elapsed_ms = (task_end - task_start).as_millis(),
            "Task completed"
        );
    }

    Ok(())
}

/// Run every finalize item against its resolved target. Items are independent so they run concurrently and their failures are collected, never short-circuited.
async fn run_finalize(
    items: &[FinalizeItem],
    phase: &PhaseOutput,
    ldrs_env: &[(String, String)],
    context: &LdrsExecutionContext<'_>,
) -> Vec<String> {
    let runs = items.iter().map(|item| async move {
        match item {
            FinalizeItem::Sf(sf) => {
                // `target` defaults to the source name; it is both the rendered identity the Lua
                // builds on and the connection-lookup key.
                let target = sf.target.as_deref().unwrap_or(&phase.name);
                let resolved = context
                    .render_template(target)
                    .map_err(|e| format!("finalize target render failed: {e:#}"))?;
                let url = get_dest_url(ldrs_env, &resolved, "sf")
                    .map_err(|e| format!("{e:#}"))?
                    .1
                    .clone();
                let conn =
                    SnowflakeConnection::create_connection(&url).map_err(|e| format!("{e:#}"))?;
                let commands = call_finalize::<SfCommand>(&sf.lua, phase, context)
                    .map_err(|e| format!("{e:#}"))?;
                tokio::task::spawn_blocking(move || run_sf(&conn, commands))
                    .await
                    .map_err(|e| format!("finalize task panicked: {e}"))?
            }
        }
    });
    // Keep the failures (`Err`); a success is `Ok(())` with nothing to report.
    join_all(runs)
        .await
        .into_iter()
        .filter_map(Result::err)
        .collect()
}

pub async fn execute_task(
    task: LdrsParsedConfig,
    ldrs_env: &[(String, String)],
    handlebars: &handlebars::Handlebars<'_>,
    handlebars_vars: &[(String, String)],
    env_params: &[(String, String, Option<ColumnType>)],
    cloud_io_rt: &tokio::runtime::Handle,
    pg_pools: &mut HashMap<String, Pool>,
) -> Result<Option<u64>, anyhow::Error> {
    let name = task.src.name().to_string();
    let finalize_items = task.finalize;
    let (src, context) = match task.src {
        LdrsSource::File(file) => {
            let ldrs_context =
                LdrsExecutionContext::try_new(&file.name, &handlebars, &handlebars_vars)?;
            // if the filename is not provided, use the name as the filename
            let file_name = file.filename.as_deref().unwrap_or(file.name.as_str());
            let src_value = get_src_url(ldrs_env, &file.name, "file")?;
            // render the filename template, then join it onto the source base
            let file_name = ldrs_context.render_template(file_name)?;
            let src_url = join_into_url(src_value.1.as_str(), &file_name)?;
            let builder = builder_from_url(src_url.clone(), cloud_io_rt.clone()).await?;

            let schema = builder.schema().clone();

            let file_md = builder.metadata().clone();
            let fields = get_fields(file_md.file_metadata())?;

            // matches DuckDB STANDARD_VECTOR_SIZE
            let stream = builder.with_batch_size(2048).build()?;
            let source_cols = fields
                .iter()
                .filter_map(|pq| columnspec_from_parquet(pq).ok())
                .collect::<Vec<_>>();
            (
                LdrsSrcStream {
                    schema: Some(schema),
                    stream_type: StreamType::Parquet(stream),
                    source_cols,
                    cleanup_handle: None,
                    source_files: Some(vec![src_url.to_string()]),
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
            let ldrs_context = LdrsExecutionContext::try_new(&name, &handlebars, &handlebars_vars)?;
            let handled_name = ldrs_context.render_template("{{ shoutySnakeCase name }}")?;
            let sf_params = sf.try_get_env_params(&handled_name, &env_params)?;
            debug!("Snowflake Params: {:?}", sf_params);
            let rendered_sql = ldrs_context.render_template(&sf_sql)?;
            let conn = SnowflakeConnection::create_connection(&sf_src.1)?;
            let arrow_stream = sf_arrow_stream(&conn, &rendered_sql, sf_params).await?;
            let schema = arrow_stream.schema_stream.await;
            (
                LdrsSrcStream {
                    schema,
                    stream_type: StreamType::Receiver(arrow_stream.batch_stream),
                    source_cols: vec![],
                    cleanup_handle: Some(arrow_stream.command_handle),
                    source_files: None,
                },
                ldrs_context,
            )
        }
    };

    let rows = match src.schema {
        Some(schema) => {
            // Matching destinations share one transform; otherwise each casts its own.
            let shared = all_columns_match(&task.dests);
            let built = build_sinks(
                task.dests,
                &src.source_cols,
                &schema,
                &context,
                ldrs_env,
                env_params,
                pg_pools,
            )
            .await?;
            let (mut sinks, per_dest): (Vec<Sink>, Vec<Option<BatchTransform>>) =
                built.into_iter().unzip();
            let transforms = if shared {
                Transforms::Shared(per_dest.into_iter().next().flatten())
            } else {
                Transforms::PerDest(per_dest)
            };
            match drive(src.stream_type, &mut sinks, &transforms).await {
                Ok(rows) => {
                    let destinations = finish_all(sinks, env_params).await?;
                    let phase = PhaseOutput {
                        name,
                        source_files: src.source_files,
                        success: destinations.iter().all(|d| d.succeeded()),
                        rows,
                        destinations,
                    };
                    debug!("finalize phase output: {:?}", phase);
                    let finalize_failures =
                        run_finalize(&finalize_items, &phase, ldrs_env, &context).await;
                    let load_failures: Vec<String> = phase
                        .destinations
                        .iter()
                        .filter_map(|d| d.status().err().cloned())
                        .collect();
                    // Load and finalize are distinct phases (data didn't land vs. post-load work
                    // didn't run). Each failure is its own structured log line; the returned error
                    // is a summary, since the detail lives in the logs.
                    for f in &load_failures {
                        error!(phase = "load", "{f}");
                    }
                    for f in &finalize_failures {
                        error!(phase = "finalize", "{f}");
                    }
                    let mut summary = Vec::new();
                    if !load_failures.is_empty() {
                        summary.push(format!("{} load", load_failures.len()));
                    }
                    if !finalize_failures.is_empty() {
                        summary.push(format!("{} finalize", finalize_failures.len()));
                    }
                    if summary.is_empty() {
                        Ok(Some(rows))
                    } else {
                        Err(anyhow::anyhow!(
                            "task '{}' failed (see logs): {} error(s)",
                            phase.name,
                            summary.join(", ")
                        ))
                    }
                }
                Err(e) => {
                    abort_all(sinks).await;
                    Err(e)
                }
            }
        }
        None => {
            warn!("No schema found, most likely the load failed or no Arrow Record Batches were produced.");
            Ok(None)
        }
    }?;

    if let Some(handle) = src.cleanup_handle {
        match handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(anyhow::anyhow!("ldrs-sf task panicked: {}", e)),
        }?
    }
    Ok(rows)
}

/// Whether every destination resolved to the same column specs.
/// If they all match it is the same transforms as the source columns are the same as well.
fn all_columns_match(dests: &[LdrsDestination]) -> bool {
    dests.len() > 1 && dests.windows(2).all(|w| w[0].columns() == w[1].columns())
}

/// Get the pool for `url` from the registry, building and caching it on first use. Returns a
/// clone of the cached pool.
fn pool_for(pools: &mut HashMap<String, Pool>, url: &str) -> Result<Pool, anyhow::Error> {
    if let Some(pool) = pools.get(url) {
        return Ok(pool.clone());
    }
    let pool = build_pg_pool(url)?;
    pools.insert(url.to_string(), pool.clone());
    Ok(pool)
}

/// Build one sink per destination, paired with the transform it needs (the executor owns
/// the cast; sinks are passthrough writers). Aborts already-built sinks if a later one
/// fails to construct.
pub async fn build_sinks(
    dests: Vec<LdrsDestination>,
    source_cols: &[ColumnSpec],
    schema: &SchemaRef,
    context: &LdrsExecutionContext<'_>,
    ldrs_env: &[(String, String)],
    env_params: &[(String, String, Option<ColumnType>)],
    pg_pools: &mut HashMap<String, Pool>,
) -> Result<Vec<(Sink, Option<BatchTransform>)>, anyhow::Error> {
    let mut built = Vec::with_capacity(dests.len());
    for dest in dests {
        match build_one(
            dest,
            source_cols,
            schema,
            context,
            ldrs_env,
            env_params,
            pg_pools,
        )
        .await
        {
            Ok(pair) => built.push(pair),
            Err(e) => {
                abort_all(built.into_iter().map(|(sink, _)| sink).collect()).await;
                return Err(e);
            }
        }
    }
    Ok(built)
}

/// Resolve a destination's column specs into its target columns, output schema, and the
/// transform the executor will run (`None` when no cast is needed).
fn resolve_transform(
    source_cols: &[ColumnSpec],
    columns: Vec<ColumnSpec>,
    schema: &SchemaRef,
) -> Result<(Vec<ColumnSpec>, SchemaRef, Option<BatchTransform>), anyhow::Error> {
    let (target_cols, strategies) = column_helper(source_cols.to_vec(), columns, schema)?;
    if strategies.iter().any(|s| s.is_some()) {
        let out_schema = Arc::new(Schema::new(
            target_cols
                .iter()
                .map(|col| col.to_arrow_field())
                .collect::<Vec<_>>(),
        ));
        Ok((
            target_cols,
            out_schema.clone(),
            Some((strategies, out_schema)),
        ))
    } else {
        Ok((target_cols, schema.clone(), None))
    }
}

async fn build_one(
    dest: LdrsDestination,
    source_cols: &[ColumnSpec],
    schema: &SchemaRef,
    context: &LdrsExecutionContext<'_>,
    ldrs_env: &[(String, String)],
    env_params: &[(String, String, Option<ColumnType>)],
    pg_pools: &mut HashMap<String, Pool>,
) -> Result<(Sink, Option<BatchTransform>), anyhow::Error> {
    match dest {
        LdrsDestination::Pg(pg_dest) => {
            let resolved_target =
                context.render_template(pg_dest.get_target().unwrap_or(pg_dest.get_name()))?;
            // PG needs a schema-qualified identity for its staging/rename flow.
            let (pg_schema, pg_table) = resolved_target.split_once('.').ok_or_else(|| {
                anyhow::anyhow!(
                    "pg destination requires a schema-qualified target, got '{resolved_target}'"
                )
            })?;
            let load_table_name = format!("{}_{}", pg_table, uuid::Uuid::new_v4().simple());
            let load_table = format!("{pg_schema}.{load_table_name}");
            let pg_ctx = context.with_vars(&[
                ("target", &resolved_target),
                ("schema", pg_schema),
                ("table", pg_table),
                ("load_table", &load_table),
                ("load_table_name", &load_table_name),
            ]);
            let dest_value = get_dest_url(ldrs_env, &resolved_target, "pg")?;
            let (pg_url, role) = check_for_role(dest_value.1.as_str())?;
            let role = role.or(get_env_value(
                ldrs_env,
                &[
                    &format!("LDRS_PG_ROLE_{}", shouty(&resolved_target)),
                    "LDRS_PG_ROLE",
                ],
            )
            .map(|(_, v)| v.to_string()));
            let plan = split_pg_plan(pg_dest.to_pg_commands())?;
            // PG keeps `target_cols` for COPY encoding; the cast runs in the executor.
            let (target_cols, _out_schema, transform) =
                resolve_transform(source_cols, pg_dest.get_columns(), schema)?;
            let pool = pool_for(pg_pools, &pg_url)?;
            let pg = PgSink::open(
                pool,
                role,
                plan,
                target_cols,
                resolved_target,
                &pg_ctx,
                env_params,
            )
            .await?;
            Ok((Sink::Pg(pg), transform))
        }
        LdrsDestination::Pq(parq) => {
            let resolved_target =
                context.render_template(parq.target.as_deref().unwrap_or(&parq.name))?;
            let dest_ctx = context.with_vars(&[("target", &resolved_target)]);
            let dest_value = get_dest_url(ldrs_env, &resolved_target, "pq")?;
            let (_target_cols, out_schema, transform) =
                resolve_transform(source_cols, parq.columns, schema)?;
            let props = with_bloom_filters(default_writer_props(), parq.bloom_filters);
            let mut namer_hb = handlebars::Handlebars::new();
            setup_handlebars(&mut namer_hb);
            let namer = build_index_namer(namer_hb, &dest_ctx.context, &parq.filename);
            let sink = ParquetSink::new(
                &dest_value.1,
                out_schema,
                parq.max_rows,
                parq.max_bytes,
                namer,
                Some(props),
            )?;
            Ok((Sink::Pq(sink), transform))
        }
        LdrsDestination::Delta(delta_dest) => {
            let (resolved_target, columns) = match &delta_dest {
                DeltaDestination::Overwrite(c) => (
                    context.render_template(c.target.as_deref().unwrap_or(&c.name))?,
                    c.columns.clone(),
                ),
                DeltaDestination::Merge(m) => (
                    context
                        .render_template(m.common.target.as_deref().unwrap_or(&m.common.name))?,
                    m.common.columns.clone(),
                ),
            };
            let dest_value = get_dest_url(ldrs_env, &resolved_target, "delta")?;
            let storage_url = dest_value.1.strip_prefix("delta+").unwrap_or(&dest_value.1);
            let table_path = join_into_url(storage_url, &resolved_target)?.to_string();
            let (_target_cols, out_schema, transform) =
                resolve_transform(source_cols, columns, schema)?;

            ensure_table(&table_path, &out_schema).await?;
            match delta_dest {
                DeltaDestination::Overwrite(o) => {
                    let sink =
                        DeltaOverwriteSink::new(&table_path, out_schema, o.max_rows, o.max_bytes)?;
                    Ok((Sink::DeltaOverwrite(sink), transform))
                }
                DeltaDestination::Merge(m) => {
                    let DeltaMerge {
                        merge_keys,
                        allow_null_keys,
                        txn_mode,
                        watermark_column,
                        batch_version,
                        app_id,
                        ..
                    } = m;
                    let txn = match txn_mode {
                        None => None,
                        Some(TxnMode::SourceWatermark) => Some(MergeTxnConfig::SourceWatermark {
                            app_id,
                            watermark_column: watermark_column
                                .expect("validate ensures watermark_column for source_watermark"),
                        }),
                        Some(TxnMode::ProcessingTime) => Some(MergeTxnConfig::ProcessingTime {
                            app_id,
                            batch_version,
                        }),
                    };
                    let dest_ctx = context.with_vars(&[("target", &resolved_target)]);
                    let txn_config = resolve_txn_config(txn, &dest_ctx)?;
                    let merge_config = MergeConfig {
                        merge_keys,
                        allow_null_keys,
                        max_rows: m.common.max_rows,
                        max_bytes: m.common.max_bytes,
                        txn_config,
                    };
                    let sink = DeltaMergeSink::new(&table_path, out_schema, merge_config)?;
                    Ok((Sink::DeltaMerge(sink), transform))
                }
            }
        }
        LdrsDestination::Arrow(arrow_dest) => {
            if io::stdout().is_terminal() {
                return Err(anyhow::Error::msg("Outputting Arrow IPC Stream to stdout is not supported in a terminal. Please redirect the output to a file or pipe it to another command."));
            }
            let (_target_cols, out_schema, transform) =
                resolve_transform(source_cols, arrow_dest.columns, schema)?;
            let sink = ArrowStdoutSink::new(io::stdout(), out_schema)?;
            Ok((Sink::Arrow(sink), transform))
        }
    }
}

/// Finish every sink after a successful stream, recording each one's outcome. Sinks finish
/// concurrently; each commit is independent, so one failure does not stop the others.
pub async fn finish_all(
    sinks: Vec<Sink>,
    env_params: &[(String, String, Option<ColumnType>)],
) -> Result<Vec<DestinationOutcome>, anyhow::Error> {
    let destinations: Vec<DestinationOutcome> = join_all(sinks.into_iter().map(|sink| {
        async move {
            match sink {
                Sink::Pg(pg) => {
                    let table = pg.target().to_string();
                    let result = pg
                        .commit(env_params)
                        .await
                        .with_context(|| format!("postgres load to {table} failed"))
                        .map_err(|e| format!("{e:#}"));
                    Some(DestinationOutcome::Pg { table, result })
                }
                Sink::Pq(s) => {
                    let location = s.base_path().to_string();
                    Some(DestinationOutcome::Parquet {
                        location: location.clone(),
                        result: s
                            .finish()
                            .await
                            .map(|metas| {
                                metas
                                    .into_iter()
                                    .map(|(path, md)| FileWritten {
                                        path,
                                        rows: md.file_metadata().num_rows().max(0) as u64,
                                    })
                                    .collect()
                            })
                            .with_context(|| format!("parquet write to {location} failed"))
                            .map_err(|e| format!("{e:#}")),
                    })
                }
                Sink::DeltaOverwrite(s) => {
                    let location = s.base_path().to_string();
                    Some(DestinationOutcome::Delta {
                        location: location.clone(),
                        strategy: DeltaStrategy::Overwrite,
                        result: s
                            .finish()
                            .await
                            .with_context(|| format!("delta overwrite at {location} failed"))
                            .map_err(|e| format!("{e:#}")),
                    })
                }
                Sink::DeltaMerge(s) => {
                    let location = s.base_path().to_string();
                    Some(DestinationOutcome::Delta {
                        location: location.clone(),
                        strategy: DeltaStrategy::Merge,
                        result: s
                            .finish()
                            .await
                            .map(|_| ())
                            .with_context(|| format!("delta merge at {location} failed"))
                            .map_err(|e| format!("{e:#}")),
                    })
                }
                // arrow: terminal, no outcome entry; a finish error (e.g. closed pipe) is logged, not fatal
                Sink::Arrow(s) => {
                    if let Err(e) = s.finish() {
                        warn!("arrow sink finish failed: {e}");
                    }
                    None
                }
            }
        }
    }))
    .await
    .into_iter()
    .flatten()
    .collect();
    Ok(destinations)
}

/// Failure-path cleanup. Each sink undoes what it can: Postgres rolls back, the file
/// sinks delete their incomplete output, Arrow is a no-op. Best-effort.
pub async fn abort_all(sinks: Vec<Sink>) {
    for sink in sinks {
        match sink {
            Sink::Pg(pg) => pg.rollback().await,
            Sink::Pq(s) => s.abort().await,
            Sink::DeltaOverwrite(s) => s.abort().await,
            Sink::DeltaMerge(s) => s.abort().await,
            Sink::Arrow(s) => s.abort(),
        }
    }
}

/// Build a Parquet file namer that renders `template` for each rotation with the rotation
/// `index` bound into the context. Owns the handlebars and a copy of the context.
fn build_index_namer(
    handlebars: handlebars::Handlebars<'static>,
    context: &serde_json::Value,
    template: &str,
) -> FileNamer {
    let context = context.clone();
    let template = template.to_string();
    Box::new(move |index| {
        let mut ctx = context.clone();
        ctx["index"] = serde_json::json!(index);
        handlebars
            .render_template(&template, &ctx)
            .map_err(Into::into)
    })
}

#[cfg(test)]
mod tests {

    use ldrs_arrow::{ColumnType, TimeUnit};

    use super::*;

    #[test]
    fn pool_for_caches_pools_by_url() {
        // build_pg_pool is lazy (no connection), so this exercises the registry offline.
        let mut pools = HashMap::new();
        let _ = pool_for(&mut pools, "postgresql://localhost/db1").unwrap();
        let _ = pool_for(&mut pools, "postgresql://localhost/db1").unwrap();
        assert_eq!(pools.len(), 1, "same URL reuses one pool");
        let _ = pool_for(&mut pools, "postgresql://localhost/db2").unwrap();
        assert_eq!(pools.len(), 2, "a different URL gets its own pool");
    }

    #[test]
    fn index_namer_renders_padded_distinct_names() {
        let mut hb = handlebars::Handlebars::new();
        setup_handlebars(&mut hb);
        let context = serde_json::json!({ "name": "public.users" });
        let namer = build_index_namer(hb, &context, "out/{{ name }}_{{ pad index 5 }}.parquet");
        assert_eq!(namer(0).unwrap(), "out/public.users_00000.parquet");
        assert_eq!(namer(1).unwrap(), "out/public.users_00001.parquet");
        assert_eq!(namer(42).unwrap(), "out/public.users_00042.parquet");
    }

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
    fn test_get_dest_url_shouty_form() {
        // a dotted identity ("public.users") can't be set as an OS env var; the screaming-snake
        // form resolves it. raw still wins when present (e.g. a programmatically-injected key).
        let snake_only = vec![(
            "LDRS_DEST_PUBLIC_USERS".to_string(),
            "delta+az://lake".to_string(),
        )];
        assert_eq!(
            get_dest_url(&snake_only, "public.users", "delta")
                .unwrap()
                .1,
            "delta+az://lake"
        );

        let raw_present = vec![
            (
                "LDRS_DEST_public.users".to_string(),
                "raw://wins".to_string(),
            ),
            (
                "LDRS_DEST_PUBLIC_USERS".to_string(),
                "delta+az://lake".to_string(),
            ),
        ];
        assert_eq!(
            get_dest_url(&raw_present, "public.users", "delta")
                .unwrap()
                .1,
            "raw://wins"
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
                Some(ColumnType::Timestamp(TimeUnit::Micros))
            )
        );
    }
}
