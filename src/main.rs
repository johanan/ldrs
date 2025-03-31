mod config;
mod delta;
mod parquet_provider;
mod postgres;
mod pq;
mod storage;

#[cfg(test)]
mod test_utils;

use anyhow::Context;
use clap::{Parser, Subcommand};
use config::{LoadArgs, PGFileLoad, PGFileLoadArgs, ProcessedPGFileLoad};
use delta::{DeltaLoad, DeltaMerge};
use parquet_provider::builder_from_string;
use postgres::load_postgres;
use pq::{get_fields, map_parquet_to_abstract, ParquetType};
use tracing::{debug, info};

#[derive(Subcommand)]
enum Commands {
    Load(LoadArgs),
    PGConfig(PGFileLoadArgs),
    Delta(DeltaLoad),
    Merge(DeltaMerge),
}

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

async fn load_postgres_cmd(
    args: &LoadArgs,
    pg_url: String,
    handle: tokio::runtime::Handle,
) -> Result<(), anyhow::Error> {
    let builder = builder_from_string(args.file.clone(), handle).await?;
    let file_md = builder.metadata().file_metadata().clone();
    let kv = pq::get_kv_fields(&file_md);
    debug!("kv: {:?}", kv);
    info!("num rows: {:?}", file_md.num_rows());

    let fields = get_fields(&file_md)?;
    let mapped = fields
        .iter()
        .filter_map(|pq| map_parquet_to_abstract(pq, &kv))
        .collect::<Vec<ParquetType>>();

    let stream = builder.with_batch_size(args.batch_size).build()?;
    load_postgres(
        &mapped,
        &args.table,
        args.post_sql.clone(),
        &pg_url,
        args.role.clone(),
        stream,
    )
    .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let start = std::time::Instant::now();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("cloud-io")
        .enable_time()
        .enable_io()
        .build()
        .with_context(|| "Unable to create cloud io tokio runtime")?;

    let command_exec = match cli.command {
        Commands::Load(args) => {
            match std::env::var("LDRS_PG_URL").with_context(|| "LDRS_PG_URL not set") {
                Ok(pg_url) => load_postgres_cmd(&args, pg_url, rt.handle().clone()).await,
                Err(e) => Err(e),
            }
        }
        Commands::PGConfig(args) => {
            // open the file and parse the yaml
            let pg_file_load: ProcessedPGFileLoad =
                std::fs::read_to_string(args.config_path.clone())
                    .with_context(|| "Unable to read file")
                    .and_then(|f| {
                        serde_yaml::from_str::<'_, PGFileLoad>(&f)
                            .with_context(|| "Unable to parse yaml")
                    })
                    .map(|pg_file_load| pg_file_load.merge_cli_args(args))
                    .and_then(|pg_file_load| pg_file_load.try_into())?;

            match std::env::var("LDRS_PG_URL").with_context(|| "LDRS_PG_URL not set") {
                Ok(pg_url) => {
                    let total_tasks = pg_file_load.tables.len();
                    for (i, pg_load) in pg_file_load.tables.iter().enumerate() {
                        let task_start = std::time::Instant::now();
                        info!("Running task: {}/{}", i + 1, total_tasks);
                        load_postgres_cmd(pg_load, pg_url.clone(), rt.handle().clone()).await?;
                        let task_end = std::time::Instant::now();
                        info!("Task time: {:?}", task_end - task_start);
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
        Commands::Delta(args) => delta::delta_run(&args, rt.handle().clone()).await,
        Commands::Merge(args) => delta::delta_merge(&args, rt.handle().clone()).await,
    };
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));

    let end = std::time::Instant::now();
    info!("Time to load: {:?}", end - start);
    info!("Debug source:\n{command_exec:#?}");
    command_exec
}
