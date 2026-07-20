pub mod config;
pub mod field_validation;

use std::collections::HashMap;

use anyhow::Context;
use deadpool_postgres::Pool;
use futures::future::join_all;
use ldrs_arrow::ColumnType;
use ldrs_core::execute::run_task;
use ldrs_core::phase::PhaseOutput;
use ldrs_core::plan::{
    ArrowDest, DeltaDest, DeltaMode, DestSpec, PgDest, PqDest, SourceSpec, Task,
};
use ldrs_delta::{MergeConfig, TxnConfig};
use ldrs_parquet::FileNamer;
use ldrs_postgres::check_for_role;
use ldrs_storage::join_into_url;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::{
    delta::{DeltaDestination, DeltaMerge, MergeTxnConfig, TxnMode},
    finalize::{call_finalize, run_sf, FinalizeItem, SfCommand},
    ldrs_config::config::{
        parse_table, validate_configs, LdrsConfig, LdrsDestination, LdrsParsedConfig, LdrsSource,
    },
    ldrs_env::{
        collect_params, collect_vars_by_prefix, explain_render_error, setup_handlebars, shouty,
        LdrsExecutionContext,
    },
    ldrs_snowflake::{
        resolve_conn_creds, sf_spawned, snowflake_source::SFSource, SnowflakeConnection,
    },
    postgres::{
        postgres_destination::{split_pg_plan, PgPlan},
        resolve::resolve_command,
    },
};

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

/// Resolve a connection URL against the tier chain for `base` (`LDRS_SRC` or `LDRS_DEST`), most
/// specific first: the exact `base_<ident>` (the canonical key, highest precedence), then its
/// screaming-snake `base_<SHOUTY>` as the env-var-safe fallback (a name's dots and the like aren't
/// valid in a shell env name), then the kind `base_<KIND>`, then the bare `base`. Logs each
/// candidate and which one landed, so `RUST_LOG=ldrs=debug` shows exactly which key to set (and
/// the error names them all on a miss).
fn resolve_scoped_url<'a>(
    vars: &'a [(String, String)],
    base: &str,
    ident: &str,
    prefix: &str,
) -> Result<&'a (String, String), anyhow::Error> {
    let raw = format!("{base}_{ident}");
    let snake = format!("{base}_{}", shouty(ident));
    let prefix_key = format!("{base}_{prefix}");
    let candidates = [raw.as_str(), snake.as_str(), prefix_key.as_str(), base];
    let resolved = get_env_value(vars, &candidates);
    debug!(
        "{base} for '{ident}': tried {candidates:?} resolved {}",
        resolved
            .map(|(k, _)| k.as_str())
            .unwrap_or("(none matched)")
    );
    resolved.ok_or_else(|| anyhow::anyhow!("No env var found; set one of {candidates:?}"))
}

pub fn get_src_url<'a>(
    vars: &'a [(String, String)],
    ident: &str,
    prefix: &str,
) -> Result<&'a (String, String), anyhow::Error> {
    resolve_scoped_url(vars, "LDRS_SRC", ident, prefix)
}

pub fn get_dest_url<'a>(
    vars: &'a [(String, String)],
    ident: &str,
    prefix: &str,
) -> Result<&'a (String, String), anyhow::Error> {
    resolve_scoped_url(vars, "LDRS_DEST", ident, prefix)
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
                let dest = get_dest_url(ldrs_env, &resolved, "SF").map_err(|e| format!("{e:#}"))?;
                let (pem_key, pem_file) = resolve_conn_creds(ldrs_env, &dest.0);
                let conn = SnowflakeConnection::create_connection(&dest.1, pem_key, pem_file)
                    .map_err(|e| format!("{e:#}"))?;
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
    let context = LdrsExecutionContext::try_new(&name, &handlebars, &handlebars_vars)?;
    let source = resolve_source(task.src, &context, env_params, ldrs_env)?;
    let dests = task
        .dests
        .into_iter()
        .map(|dest| resolve_dest(dest, &context, ldrs_env))
        .collect::<Result<Vec<_>, _>>()?;
    let task = Task {
        name,
        source,
        dests,
    };

    let phase = match run_task(cloud_io_rt, pg_pools, task).await? {
        Some(phase) => phase,
        None => return Ok(None),
    };
    debug!("finalize phase output: {:?}", phase);
    let finalize_failures = run_finalize(&finalize_items, &phase, ldrs_env, &context).await;
    let load_failures: Vec<String> = phase
        .destinations
        .iter()
        .filter_map(|d| d.status().err().cloned())
        .collect();
    // Load and finalize are distinct phases. Each
    // failure is its own structured log line; the returned error is a summary, since the detail
    // lives in the logs.
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
        Ok(Some(phase.rows))
    } else {
        Err(anyhow::anyhow!(
            "task '{}' failed (see logs): {} error(s)",
            phase.name,
            summary.join(", ")
        ))
    }
}

/// Resolve a source DTO into its plan spec: render templates and resolve `LDRS_*` env against the
/// shared context.
fn resolve_source(
    src: LdrsSource,
    context: &LdrsExecutionContext<'_>,
    env_params: &[(String, String, Option<ColumnType>)],
    ldrs_env: &[(String, String)],
) -> Result<SourceSpec, anyhow::Error> {
    match src {
        LdrsSource::File(file) => {
            // if the filename is not provided, use the name as the filename
            let file_name = file.filename.as_deref().unwrap_or(file.name.as_str());
            let src_value = get_src_url(ldrs_env, &file.name, "FILE")?;
            // render the filename template, then join it onto the source base
            let file_name = context.render_template(file_name)?;
            let src_url = join_into_url(src_value.1.as_str(), &file_name)?;
            Ok(SourceSpec::File {
                url: src_url.to_string(),
            })
        }
        LdrsSource::SF(sf) => {
            let sf_sql = match &sf {
                SFSource::Query(sql) => sql.sql.clone(),
                SFSource::Table(table) => format!("SELECT * FROM {}", table.name),
            };
            let name = sf.get_name();
            let sf_src = get_src_url(ldrs_env, &name, "SF")?;
            let handled_name = context.render_template("{{ shoutySnakeCase name }}")?;
            let sf_params = sf.try_get_env_params(&handled_name, env_params)?;
            debug!("Snowflake Params: {:?}", sf_params);
            let rendered_sql = context.render_template(&sf_sql)?;
            let (pem_key, pem_file) = resolve_conn_creds(ldrs_env, &sf_src.0);
            let conn = SnowflakeConnection::create_connection(&sf_src.1, pem_key, pem_file)?;
            let spawned = sf_spawned(&conn, &rendered_sql, sf_params);
            Ok(SourceSpec::Spawned(spawned))
        }
    }
}

/// Resolve a destination DTO into its plan spec: render templates, resolve `LDRS_*` env, and expand
/// the PG strategy into command sequences.
fn resolve_dest(
    dest: LdrsDestination,
    context: &LdrsExecutionContext<'_>,
    ldrs_env: &[(String, String)],
) -> Result<DestSpec, anyhow::Error> {
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
            let dest_value = get_dest_url(ldrs_env, &resolved_target, "PG")?;
            let (pg_url, role) = check_for_role(dest_value.1.as_str())?;
            let role = role.or(get_env_value(
                ldrs_env,
                &[
                    &format!("LDRS_PG_ROLE_{}", shouty(&resolved_target)),
                    "LDRS_PG_ROLE",
                ],
            )
            .map(|(_, v)| v.to_string()));
            let PgPlan {
                before,
                load_table: load_table_tmpl,
                after,
            } = split_pg_plan(pg_dest.to_pg_commands())?;
            let before = before
                .iter()
                .map(|c| resolve_command(c, &pg_ctx, ldrs_env, &resolved_target))
                .collect::<Result<Vec<_>, _>>()?;
            let after = after
                .iter()
                .map(|c| resolve_command(c, &pg_ctx, ldrs_env, &resolved_target))
                .collect::<Result<Vec<_>, _>>()?;
            let load_table = pg_ctx.render_template(&load_table_tmpl)?;
            Ok(DestSpec::Pg(PgDest {
                conn_url: pg_url,
                role,
                before,
                load_table,
                after,
                target: resolved_target,
                columns: pg_dest.get_columns(),
            }))
        }
        LdrsDestination::Pq(parq) => {
            let resolved_target =
                context.render_template(parq.target.as_deref().unwrap_or(&parq.name))?;
            let dest_ctx = context.with_vars(&[("target", &resolved_target)]);
            let dest_value = get_dest_url(ldrs_env, &resolved_target, "PQ")?;
            let mut namer_hb = handlebars::Handlebars::new();
            setup_handlebars(&mut namer_hb);
            let namer = build_index_namer(namer_hb, &dest_ctx.context, &parq.filename);
            Ok(DestSpec::Pq(PqDest {
                url: dest_value.1.clone(),
                namer,
                bloom_filters: parq.bloom_filters,
                max_rows: parq.max_rows,
                max_bytes: parq.max_bytes,
                columns: parq.columns,
            }))
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
            let dest_value = get_dest_url(ldrs_env, &resolved_target, "DELTA")?;
            let storage_url = dest_value.1.strip_prefix("delta+").unwrap_or(&dest_value.1);
            let table_path = join_into_url(storage_url, &resolved_target)?.to_string();
            let mode = match delta_dest {
                DeltaDestination::Overwrite(o) => DeltaMode::Overwrite {
                    max_rows: o.max_rows,
                    max_bytes: o.max_bytes,
                },
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
                    DeltaMode::Merge(MergeConfig {
                        merge_keys,
                        allow_null_keys,
                        max_rows: m.common.max_rows,
                        max_bytes: m.common.max_bytes,
                        txn_config,
                    })
                }
            };
            Ok(DestSpec::Delta(DeltaDest {
                table_path,
                mode,
                columns,
            }))
        }
        LdrsDestination::Arrow(arrow_dest) => Ok(DestSpec::Arrow(ArrowDest {
            columns: arrow_dest.columns,
        })),
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
            .map_err(|e| explain_render_error(&ctx, e))
    })
}

#[cfg(test)]
mod tests {

    use ldrs_arrow::{ColumnType, TimeUnit};

    use super::*;

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
    fn index_namer_render_error_is_enriched() {
        let mut hb = handlebars::Handlebars::new();
        setup_handlebars(&mut hb);
        let context = serde_json::json!({ "name": "public.users" });
        let namer = build_index_namer(hb, &context, "out/{{ missing }}.parquet");
        let err = namer(0).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("template render failed") && msg.contains("bound variables"),
            "render error should carry the enriched context, got: {msg}"
        );
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
