mod config;
mod postgres;
mod pq;

use anyhow::Context;
use clap::{Parser, Subcommand};
use config::{LoadArgs, PGFileLoad, PGFileLoadArgs};
use postgres::load_postgres;
use pq::{get_fields, get_file_metadata, map_parquet_to_abstract, ParquetType};
use tracing::{debug, info};

#[derive(Subcommand)]
enum Commands {
    Load(LoadArgs),
    PGConfig(PGFileLoadArgs),
}

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

async fn load_postgres_cmd(args: &LoadArgs, pg_url: String) -> Result<(), anyhow::Error> {
    let builder = get_file_metadata(args.file.clone()).await?;
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
    load_postgres(&mapped, &args.table, args.post_sql.clone(), &pg_url, stream).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let start = std::time::Instant::now();

    let command_exec = match &cli.command {
        Commands::Load(args) => {
            match std::env::var("LDRS_PG_URL").with_context(|| "LDRS_PG_URL not set") {
                Ok(pg_url) => load_postgres_cmd(args, pg_url).await,
                Err(e) => Err(e),
            }
        }
        Commands::PGConfig(args) => {
            // open the file and parse the yaml
            let pg_file_load = std::fs::read_to_string(args.file_path.clone())
                .with_context(|| "Unable to read file")?;
            let pg_file_load: PGFileLoad =
                serde_yaml::from_str(&pg_file_load).with_context(|| "Unable to parse yaml")?;
            match std::env::var("LDRS_PG_URL").with_context(|| "LDRS_PG_URL not set") {
                Ok(pg_url) => {
                    let tasks = pg_file_load.parse_args()?;
                    let total_tasks = tasks.len();
                    for (i, pg_load) in tasks.iter().enumerate() {
                        let task_start = std::time::Instant::now();
                        info!("Running task: {}/{}", i + 1, total_tasks);
                        load_postgres_cmd(pg_load, pg_url.clone()).await?;
                        let task_end = std::time::Instant::now();
                        info!("Task time: {:?}", task_end - task_start);
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            }
        }
    };

    let end = std::time::Instant::now();
    info!("Time to load: {:?}", end - start);
    command_exec
}
