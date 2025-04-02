mod delta;
mod ldrs_postgres;
mod parquet_provider;
mod pq;
mod storage;

#[cfg(test)]
mod test_utils;

use anyhow::Context;
use clap::{Parser, Subcommand};
use delta::{DeltaLoad, DeltaMerge};
use dotenvy::dotenv;
use ldrs_postgres::{
    config::{LoadArgs, PGFileLoadArgs}, load_from_file, load_postgres_cmd
};
use tracing::info;

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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    let _ = dotenv();

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
            match std::env::var("LDRS_PG_URL").with_context(|| "LDRS_PG_URL not set") {
                Ok(pg_url) => load_from_file(args, pg_url, rt.handle().clone()).await,
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
