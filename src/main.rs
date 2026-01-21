use std::fs;

use anyhow::Context;
use clap::{Args, Parser, Subcommand};
use dotenvy::dotenv;
use ldrs::ldrs_config::{create_ldrs_exec, get_all_ldrs_env_vars};
use ldrs::ldrs_postgres::PgCommands;
use ldrs::ldrs_postgres::{load_from_file, load_postgres_cmd};
use ldrs::ldrs_snowflake::{SnowflakeResult, SnowflakeStrategy};
use ldrs::lua_logic::{LuaFunctionLoader, StorageData, UrlData};
use ldrs::path_pattern;
use ldrs::types::lua_args::modules_from_args;
use serde::Deserialize;
use tracing::info;

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

#[derive(Subcommand)]
pub enum DeltaCommands {
    /// Load data into Delta Lake
    Load(DeltaLoad),
}

#[derive(Args)]
struct ConfigArgs {
    #[arg(short, long)]
    config: String,
}

#[derive(Subcommand)]
enum Destination {
    /// Load from a config file. All sources and destinations
    Ld(ConfigArgs),
    /// PostgreSQL destination
    Pg {
        #[command(subcommand)]
        command: PgCommands,
    },
    /// Delta Lake destination
    Delta {
        #[command(subcommand)]
        command: DeltaCommands,
    },
    /// Snowflake destination
    Sf {
        #[command(subcommand)]
        command: ldrs::ldrs_snowflake::SnowflakeCommands,
    },
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    destination: Destination,
}

fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let _ = dotenv();

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
        match cli.destination {
            Destination::Ld(args) => {
                let config_string = fs::read_to_string(&args.config)
                    .with_context(|| format!("Failed to read config file: {}", args.config))?;
                let ldrs_env = get_all_ldrs_env_vars();
                create_ldrs_exec(&config_string, &ldrs_env, &rt.handle()).await
            }
            Destination::Pg { command } => match command {
                PgCommands::Load(args) => {
                    match std::env::var("LDRS_PG_URL").with_context(|| "LDRS_PG_URL not set") {
                        Ok(pg_url) => load_postgres_cmd(&args, pg_url, rt.handle().clone()).await,
                        Err(e) => Err(e),
                    }
                }
                PgCommands::Config(args) => {
                    match std::env::var("LDRS_PG_URL").with_context(|| "LDRS_PG_URL not set") {
                        Ok(pg_url) => load_from_file(args, pg_url, rt.handle().clone()).await,
                        Err(e) => Err(e),
                    }
                }
            },
            Destination::Delta { command } => match command {
                DeltaCommands::Load(args) => {
                    info!("delta load with {:?}", args);
                    Ok(())
                }
            },
            Destination::Sf { command } => match command {
                ldrs::ldrs_snowflake::SnowflakeCommands::Exec { sql } => {
                    match std::env::var("LDRS_URL").with_context(|| "LDRS_URL not set") {
                        Ok(sf_url) => {
                            let conn =
                                ldrs::ldrs_snowflake::SnowflakeConnection::create_connection(
                                    &sf_url,
                                )?;
                            let message = conn.exec(&sql)?;
                            info!("Snowflake exec result: {}", message);
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
                ldrs::ldrs_snowflake::SnowflakeCommands::Ingest {
                    file_url,
                    pattern,
                    lua_args,
                } => match std::env::var("LDRS_URL").with_context(|| "LDRS_URL not set") {
                    Ok(sf_url) => {
                        let (pattern, storage, modules) =
                            modules_from_args(lua_args, file_url.as_str(), pattern.as_str())?;

                        let (_, file_path) = storage.get_store_and_path()?;
                        let file_path = file_path.to_string();
                        let extracted = pattern.parse_path(&file_path)?;
                        let segments_value = path_pattern::extracted_segments_to_value(&extracted);

                        let url_data: UrlData = storage.get_url().into();
                        let storage_data: StorageData = storage.into();
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
                        let conn =
                            ldrs::ldrs_snowflake::SnowflakeConnection::create_connection(&sf_url)?;

                        if matches!(process_result.strategy, SnowflakeStrategy::Ingest) {
                            Err(anyhow::anyhow!("Ingest is not implemented"))?
                        }

                        let pre_sql = conn.exec_transaction(&process_result.pre_sql)?;
                        info!("Pre SQL {:?} executed successfully", pre_sql);
                        let _sql = match process_result.strategy {
                            SnowflakeStrategy::Sql(sql) => {
                                let sql_result = conn.exec_transaction(&sql)?;
                                info!("SQL {:?} executed successfully", sql_result);
                                Ok(())
                            }
                            SnowflakeStrategy::Ingest => {
                                Err(anyhow::anyhow!("Ingest is not implemented"))
                            }
                        }?;
                        let post_sql = conn.exec_transaction(&process_result.post_sql)?;
                        info!("Post SQL {:?} executed successfully", post_sql);
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
    info!("Time to load: {:?}", end - start);
    command_exec
}
