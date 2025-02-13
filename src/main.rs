mod postgres;
mod pq;

use anyhow::Context;
use clap::{arg, Args, Parser, Subcommand};
use pq::{get_fields, get_file_metadata, map_parquet_to_abstract, ParquetType};
use tracing::{debug, info};

#[derive(Subcommand)]
enum Commands {
    Load(LoadArgs),
}

#[derive(Args)]
struct LoadArgs {
    #[arg(short, long)]
    file: String,
    #[arg(short, long, default_value_t = 1024)]
    batch_size: usize,
    #[arg(short, long)]
    table: String,
    #[arg(short, long)]
    post_sql: Option<String>,
}

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let a: &LoadArgs = match &cli.command {
        Commands::Load(args) => args,
    };

    let builder = get_file_metadata(a.file.clone()).await.unwrap();
    let file_md = builder.metadata().file_metadata().clone();
    let kv = pq::get_kv_fields(&file_md);
    debug!("kv: {:?}", kv);
    info!("num rows: {:?}", file_md.num_rows());

    let fields = get_fields(&file_md)?;
    let mapped = fields
        .iter()
        .filter_map(|pq| map_parquet_to_abstract(pq, &kv))
        .collect::<Vec<ParquetType>>();

    let definition = postgres::build_definition(&a.table, &mapped, a.post_sql.clone());
    info!("PG definition: {:?}", definition);
    // get url from env var
    let pg_url = std::env::var("LDRS_PG_URL").with_context(|| "LDRS_PG_URL not set")?;

    let start = std::time::Instant::now();

    let mut client = postgres::create_connection(&pg_url).await?;
    postgres::prepare_to_copy(&mut client, &definition).await?;

    let stream = builder.with_batch_size(a.batch_size).build()?;
    postgres::execute_binary_copy(&mut client, &definition, &mapped, stream).await?;

    let end = std::time::Instant::now();
    info!("Time to load: {:?}", end - start);

    Ok(())
}
