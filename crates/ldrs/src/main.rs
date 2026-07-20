use std::{fs, io};

use anyhow::Context;
use clap::{Args, CommandFactory, Parser, Subcommand};
use dotenvy::dotenv;
use ldrs::cli_schema;
use ldrs::ldrs_config::config::{find_unknown_block_keys, parse_dest, parse_src, LdrsParsedConfig};
use ldrs::ldrs_config::{execute_configs, infer_env_type, parse_yaml_config};
use ldrs::ldrs_env::get_all_ldrs_env_vars;
use ldrs::lua_logic::lua_args::{modules_from_args, LuaArgs, SnowflakeResult, SnowflakeStrategy};
use ldrs::lua_logic::{LuaFunctionLoader, StorageData, UrlData};
use ldrs::path_pattern;
use ldrs_storage::build_store;
use serde::Deserialize;
use serde_yaml::{Mapping, Value};
use tracing::{debug, info};
use tracing_subscriber::{fmt, EnvFilter};

// maintaining this so clients do not break
#[derive(Args, Deserialize, Debug)]
pub struct DeltaLoad {
    #[arg(short, long)]
    pub delta_root: String,
    #[arg(short, long)]
    pub file: String,
    #[arg(short, long)]
    pub table_name: String,
}

#[derive(Args)]
struct ConfigArgs {
    #[arg(short, long)]
    config: String,
    /// Run only these tables, by `name`, comma-separated (e.g. --select public.users,public.orders). Omit to run every table in the config.
    #[arg(long, value_delimiter = ',')]
    select: Option<Vec<String>>,
}

#[derive(Subcommand)]
pub enum SnowflakeCommands {
    Ingest {
        #[arg(long, short)]
        file_url: String,

        #[arg(long, short)]
        pattern: String,

        #[clap(flatten)]
        lua_args: LuaArgs,
    },
}

#[derive(Args)]
#[command(
    after_help = "Tip: run `ldrs schema` to list the available kinds. `ldrs schema <kind>` (e.g. `ldrs schema pq`) dumps one kind's fields; `ldrs schema columns` the column-transform vocabulary; `ldrs schema usage` env vars, templating, and examples."
)]
struct RunArgs {
    /// Base config blob (YAML or JSON). A single table block.
    /// All other config args will override values in this.
    #[arg(long)]
    config_inline: Option<String>,

    /// Source kind (file, sf, etc). Optional: can be inferred from LDRS_SRC
    #[arg(long)]
    src: Option<String>,

    /// Destination kind (pg.merge, pq, delta.overwrite, etc). Optional: can be inferred from LDRS_DEST
    #[arg(long)]
    dest: Option<String>,

    /// Table identifier.
    #[arg(long)]
    name: Option<String>,

    /// SQL for query-shaped sources. Shortcut for --opt sql=
    #[arg(long)]
    sql: Option<String>,

    /// Per-kind options as key=value. Keys may be namespaced (pg.merge_keys, file.partition_cols).
    #[arg(long = "opt", value_parser = parse_kv)]
    opt: Vec<(String, String)>,
}

#[derive(Subcommand)]
enum Destination {
    /// Load from a config file. All sources and destinations
    Ld(ConfigArgs),
    /// Snowflake destination
    Sf {
        #[command(subcommand)]
        command: SnowflakeCommands,
    },
    /// Singular load from inline config and/or cli args
    Run(RunArgs),
    /// Discover available sources, destinations, and config options. Start here for one-off runs.
    Schema {
        #[command(subcommand)]
        command: Option<cli_schema::SchemaCommands>,
    },
}

fn parse_kv(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("expected KEY=VALUE, got `{}`", s))
        .and_then(|(k, v)| {
            if k.is_empty() {
                Err(format!("key cannot be empty in `{}`", s))
            } else {
                Ok((k, v))
            }
        })
}

fn build_run_block(args: &RunArgs) -> Result<Value, anyhow::Error> {
    let mut block: Mapping = match args.config_inline.as_deref() {
        Some(s) => serde_yaml::from_str::<Mapping>(s)?,
        None => Mapping::new(),
    };
    for (k, v) in &args.opt {
        block.insert(Value::String(k.clone()), Value::String(v.clone()));
    }
    for (k, v) in [
        ("src", args.src.as_ref()),
        ("dest", args.dest.as_ref()),
        ("name", args.name.as_ref()),
        ("sql", args.sql.as_ref()),
    ] {
        if let Some(v) = v {
            block.insert(Value::String(k.to_string()), Value::String(v.clone()));
        }
    }
    Ok(Value::Mapping(block))
}

const BANNER: &str = r#"

 ████      █████
░░███     ░░███
 ░███   ███████  ████████   █████
 ░███  ███░░███ ░░███░░███ ███░░
 ░███ ░███ ░███  ░███ ░░░ ░░█████
 ░███ ░███ ░███  ░███      ░░░░███
 █████░░████████ █████     ██████
░░░░░  ░░░░░░░░ ░░░░░     ░░░░░░
"#;

#[derive(Parser)]
#[command(author, version, about, long_about = None, before_help = BANNER)]
struct Cli {
    #[command(subcommand)]
    destination: Option<Destination>,
}

fn main() -> Result<(), anyhow::Error> {
    let _ = dotenv();
    fmt::Subscriber::builder()
        .with_writer(io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let cli = Cli::parse();

    let Some(destination) = cli.destination else {
        Cli::command().print_help()?;
        println!();
        return Ok(());
    };

    let is_data_command = !matches!(destination, Destination::Schema { .. });
    let start = std::time::Instant::now();

    let main_rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("main")
        .enable_all()
        .build()
        .with_context(|| "Unable to create main runtime")?;

    // Create the cloud I/O runtime outside the async context
    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("cloud-io")
        .enable_time()
        .enable_io()
        .build()
        .with_context(|| "Unable to create cloud io tokio runtime")?;

    let command_exec = main_rt.block_on(async {
        match destination {
            Destination::Ld(args) => {
                let config_string = fs::read_to_string(&args.config)
                    .with_context(|| format!("Failed to read config file: {}", args.config))?;
                let ldrs_env = get_all_ldrs_env_vars();
                let configs = parse_yaml_config(&config_string, &ldrs_env)?;
                execute_configs(configs, args.select, &ldrs_env, &rt.handle()).await
            }
            Destination::Run(args) => {
                let ldrs_env = get_all_ldrs_env_vars();
                let config = build_run_block(&args)?;
                let src_default = infer_env_type("LDRS_SRC", &ldrs_env);
                let dest_default = infer_env_type("LDRS_DEST", &ldrs_env);
                let src = parse_src(config.clone(), &src_default)?;
                let dest = parse_dest(config.clone(), &dest_default)?;
                let unknown_keys = find_unknown_block_keys(&config, &src, &dest);

                execute_configs(
                    vec![LdrsParsedConfig {
                        src,
                        dests: vec![dest],
                        finalize: Vec::new(),
                        unknown_keys,
                    }],
                    None,
                    &ldrs_env,
                    &rt.handle(),
                )
                .await
            }
            Destination::Schema { command } => match command {
                None => {
                    // bare `ldrs schema` → list the subcommands
                    let mut cmd = Cli::command();
                    if let Some(sub) = cmd.find_subcommand_mut("schema") {
                        sub.print_help()?;
                        println!();
                    }
                    Ok(())
                }
                Some(cmd) => {
                    let output = cli_schema::build(&cmd);
                    println!("{}", serde_json::to_string_pretty(&output)?);
                    Ok(())
                }
            },
            Destination::Sf { command } => match command {
                SnowflakeCommands::Ingest {
                    file_url,
                    pattern,
                    lua_args,
                } => match std::env::var("LDRS_URL").with_context(|| "LDRS_URL not set") {
                    Ok(sf_url) => {
                        let (pattern, url, modules) =
                            modules_from_args(lua_args, file_url.as_str(), pattern.as_str())?;

                        let (_, file_path, scheme) = build_store(&url)?;
                        let url_data: UrlData = url.clone().into();
                        let storage_data = StorageData::from_parts(&url, &file_path, scheme);

                        let file_path_str = file_path.to_string();
                        let extracted = pattern.parse_path(&file_path_str)?;
                        let segments_value = path_pattern::extracted_segments_to_value(&extracted);

                        let context = serde_json::json!({});

                        let mut loader = LuaFunctionLoader::new().unwrap();
                        let process_result = loader.call_process::<SnowflakeResult>(
                            &modules,
                            &url_data,
                            &storage_data,
                            &segments_value,
                            None,
                            &context,
                        )?;
                        let conn = ldrs::ldrs_snowflake::SnowflakeConnection::create_connection(
                            &sf_url, None, None,
                        )?;

                        if matches!(process_result.strategy, SnowflakeStrategy::Ingest) {
                            Err(anyhow::anyhow!("Ingest is not implemented"))?
                        }

                        let pre_sql = conn.exec(&process_result.pre_sql)?;
                        debug!("Pre SQL {:?} executed successfully", pre_sql);
                        let _sql = match process_result.strategy {
                            SnowflakeStrategy::Sql(sql) => {
                                let sql_result = conn.exec(&sql)?;
                                debug!("SQL {:?} executed successfully", sql_result);
                                Ok(())
                            }
                            SnowflakeStrategy::Ingest => {
                                Err(anyhow::anyhow!("Ingest is not implemented"))
                            }
                        }?;
                        let post_sql = conn.exec(&process_result.post_sql)?;
                        debug!("Post SQL {:?} executed successfully", post_sql);
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
            },
        }
    });

    drop(main_rt);
    drop(rt);

    let end = std::time::Instant::now();
    if is_data_command {
        info!("Time to load: {:?}", end - start);
    }
    command_exec
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_args() -> RunArgs {
        RunArgs {
            config_inline: None,
            src: None,
            dest: None,
            name: None,
            sql: None,
            opt: vec![],
        }
    }

    fn get_str<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
        v.get(key).and_then(|x| x.as_str())
    }

    #[test]
    fn parse_kv_simple() {
        assert_eq!(parse_kv("k=v").unwrap(), ("k".to_string(), "v".to_string()));
    }

    #[test]
    fn parse_kv_empty_key_rejected() {
        assert!(parse_kv("=v").is_err());
    }

    #[test]
    fn parse_kv_no_equals_rejected() {
        assert!(parse_kv("kv").is_err());
    }

    #[test]
    fn parse_kv_splits_on_first_equals() {
        assert_eq!(
            parse_kv("k=v=v2").unwrap(),
            ("k".to_string(), "v=v2".to_string())
        );
    }

    #[test]
    fn parse_kv_empty_value_allowed() {
        assert_eq!(parse_kv("k=").unwrap(), ("k".to_string(), "".to_string()));
    }

    #[test]
    fn build_run_block_inline_only() {
        let args = RunArgs {
            config_inline: Some("src: file".to_string()),
            ..empty_args()
        };
        let v = build_run_block(&args).unwrap();
        assert_eq!(get_str(&v, "src"), Some("file"));
    }

    #[test]
    fn build_run_block_flag_only() {
        let args = RunArgs {
            src: Some("sf".to_string()),
            ..empty_args()
        };
        let v = build_run_block(&args).unwrap();
        assert_eq!(get_str(&v, "src"), Some("sf"));
    }

    #[test]
    fn build_run_block_opt_only() {
        let args = RunArgs {
            opt: vec![("merge_keys".to_string(), "id".to_string())],
            ..empty_args()
        };
        let v = build_run_block(&args).unwrap();
        assert_eq!(get_str(&v, "merge_keys"), Some("id"));
    }

    #[test]
    fn build_run_block_flag_beats_inline() {
        let args = RunArgs {
            config_inline: Some("src: file".to_string()),
            src: Some("sf".to_string()),
            ..empty_args()
        };
        let v = build_run_block(&args).unwrap();
        assert_eq!(get_str(&v, "src"), Some("sf"));
    }

    #[test]
    fn build_run_block_opt_beats_inline() {
        let args = RunArgs {
            config_inline: Some("src: file".to_string()),
            opt: vec![("src".to_string(), "sf".to_string())],
            ..empty_args()
        };
        let v = build_run_block(&args).unwrap();
        assert_eq!(get_str(&v, "src"), Some("sf"));
    }

    #[test]
    fn build_run_block_flag_beats_opt() {
        let args = RunArgs {
            opt: vec![("src".to_string(), "file".to_string())],
            src: Some("sf".to_string()),
            ..empty_args()
        };
        let v = build_run_block(&args).unwrap();
        assert_eq!(get_str(&v, "src"), Some("sf"));
    }
}
