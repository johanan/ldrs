use anyhow::Context;
use clap::{Parser, Subcommand};
use dotenvy::dotenv;
use futures::FutureExt;
use ldrs::delta::{delta_merge, delta_run, DeltaLoad, DeltaMerge};
use ldrs::ldrs_arrow::{
    map_arrow_to_abstract, mvr_to_stream, print_arrow_ipc_batches, ArrowIpcStreamArgs,
};
use ldrs::ldrs_postgres::load_postgres_from_arrow;
use ldrs::ldrs_postgres::{
    config::{LoadArgs, PGFileLoadArgs},
    load_from_file, load_postgres_cmd,
};
use tokio_stream::StreamExt;
use tracing::info;

#[derive(Subcommand)]
enum Commands {
    Load(LoadArgs),
    PGConfig(PGFileLoadArgs),
    Delta(DeltaLoad),
    Merge(DeltaMerge),
    Mvr(ArrowIpcStreamArgs),
}

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
        match cli.command {
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
            Commands::Delta(args) => delta_run(&args, rt.handle().clone()).await,
            Commands::Merge(args) => delta_merge(&args, rt.handle().clone()).await,
            Commands::Mvr(args) => {
                let result = mvr_to_stream(&args.full_table).await?;

                let schema = result.schema_stream.await;
                match schema {
                    Some(schema) => {
                        let mapped = schema
                            .fields()
                            .iter()
                            .filter_map(|field| {
                                let md = field.metadata();
                                map_arrow_to_abstract(field, md)
                            })
                            .collect::<Vec<_>>();
                        info!("Schema: {:?}", schema);
                        info!("Mapped: {:?}", mapped);
                        load_postgres_from_arrow(mapped, result.batch_stream).await?;
                        //print_arrow_ipc_batches(result.batch_stream).await?;
                    }
                    None => {
                        info!("No schema found. Most likely mvr failed.");
                    }
                }

                match result.command_handle.await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(anyhow::anyhow!("Command failed: {}", e)),
                    Err(e) => Err(anyhow::anyhow!("Command task panicked: {}", e)),
                }
            }
        }
    });

    drop(main_rt);
    drop(rt);

    let end = std::time::Instant::now();
    info!("Time to load: {:?}", end - start);
    info!("Debug source:\n{command_exec:#?}");
    command_exec
}
