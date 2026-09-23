use std::time::Duration;
use std::{fs, io};

use anyhow::Context;
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use dotenvy::dotenv;
use ldrs::cli_schema;
use ldrs::error::RunError;
use ldrs::ldrs_config::config::LdrsConfig;
use ldrs::ldrs_config::{
    execute_configs, infer_env_type, parse_tables, register_from_config, resolve_delta_targets,
    DeltaTarget,
};
use ldrs::ldrs_env::get_all_ldrs_env_vars;
use ldrs::results::Results;
use ldrs_delta::{execute_plan, plan_optimize, vacuum, OperationConfig, Retention};
use serde_yaml::{Mapping, Value};
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Subcommand)]
enum DeltaCommands {
    /// Vacuum every delta destination a config names
    Vacuum(VacuumLdArgs),
    /// Vacuum a single delta table
    VacuumTable(VacuumTableArgs),
    /// Compact the small files of every delta destination a config names
    Optimize(OptimizeLdArgs),
    /// Compact the small files of a single delta table
    OptimizeTable(OptimizeTableArgs),
    /// Optimize, then vacuum, then checkpoint every delta destination a config names
    Maintenance(MaintenanceLdArgs),
    /// Optimize, then vacuum, then checkpoint a single delta table
    MaintenanceTable(MaintenanceTableArgs),
    /// Ensure every delta destination a config declares a register block for is in its catalog
    Register(ConfigArgs),
}

#[derive(Args)]
struct VacuumLdArgs {
    #[command(flatten)]
    config: ConfigArgs,
    #[command(flatten)]
    vacuum: VacuumArgs,
    #[arg(short, long)]
    /// Just report what would be deleted.
    dry_run: bool,
}

#[derive(Args)]
struct VacuumTableArgs {
    /// Table root object_store URL
    #[arg(long)]
    url: String,
    #[command(flatten)]
    vacuum: VacuumArgs,
    #[arg(short, long)]
    /// Just report what would be deleted.
    dry_run: bool,
}

#[derive(Args)]
struct VacuumArgs {
    /// Retention period for vacuuming, e.g. "7 days". If not specified, the default retention period will be used. Cannot be used with --retention-unchecked.
    #[arg(long, value_parser = ldrs_delta::parse_retention)]
    retention: Option<Duration>,
    /// Retention period for vacuuming, e.g. "7 days". If specified, this will override the default retention period. Cannot be used with --retention.
    #[arg(long, conflicts_with = "retention", value_parser = ldrs_delta::parse_retention)]
    retention_unchecked: Option<Duration>,
}

#[derive(Args)]
struct OptimizeLdArgs {
    #[command(flatten)]
    config: ConfigArgs,
    #[command(flatten)]
    optimize: OptimizeArgs,
    #[arg(short, long)]
    /// Just report the bins that would be rewritten.
    dry_run: bool,
}

#[derive(Args)]
struct OptimizeTableArgs {
    /// Table root object_store URL
    #[arg(long)]
    url: String,
    #[command(flatten)]
    optimize: OptimizeArgs,
    #[arg(short, long)]
    /// Just report the bins that would be rewritten.
    dry_run: bool,
}

#[derive(Args)]
struct OptimizeArgs {
    /// Bytes to pack each output file toward. Overrides the table's `delta.targetFileSize`.
    #[arg(long)]
    target_size: Option<u64>,
    /// Column to pack each bin in order of, so the outputs keep tight min/max ranges. Overrides the
    /// first merge key, which a config's merge destination supplies on its own.
    #[arg(long)]
    order_column: Option<String>,
}

#[derive(Args)]
struct MaintenanceLdArgs {
    #[command(flatten)]
    config: ConfigArgs,
    #[command(flatten)]
    optimize: OptimizeArgs,
    #[command(flatten)]
    vacuum: VacuumArgs,
}

#[derive(Args)]
struct MaintenanceTableArgs {
    /// Table root object_store URL
    #[arg(long)]
    url: String,
    #[command(flatten)]
    optimize: OptimizeArgs,
    #[command(flatten)]
    vacuum: VacuumArgs,
}

impl VacuumArgs {
    fn retention(&self) -> Retention {
        match (self.retention, self.retention_unchecked) {
            (None, None) => Retention::TableDefault,
            (Some(d), None) => Retention::At(d),
            (None, Some(d)) => Retention::Unchecked(d),
            (Some(_), Some(_)) => unreachable!("clap rejects both retention flags"),
        }
    }
}

#[derive(Args)]
struct ConfigArgs {
    #[arg(short, long)]
    config: String,
    /// Run only these tables, by 'name', comma-separated (e.g. --select public.users,public.orders). Omit to run every table in the config.
    #[arg(long, value_delimiter = ',')]
    select: Option<Vec<String>>,
    /// Write a JSONL results file to this path. Run `ldrs schema results` for the line shapes.
    #[arg(long)]
    results: Option<String>,
}

#[derive(Args)]
#[command(
    after_help = "Tip: run `ldrs schema` to list the available kinds. `ldrs schema <kind>` (e.g. `ldrs schema pq`) dumps one kind's fields; `ldrs schema columns` the column-transform vocabulary; `ldrs schema usage` env vars, templating, and examples."
)]
struct RunArgs {
    /// Base config blob (YAML or JSON). A single table block, as in a config file's `tables:`.
    /// --src, --name and --sql override the same keys in this.
    #[arg(long)]
    config_inline: Option<String>,

    /// Source kind (file, sf, etc). Optional: can be inferred from LDRS_SRC
    #[arg(long)]
    src: Option<String>,

    /// Table identifier.
    #[arg(long)]
    name: Option<String>,

    /// SQL for query-shaped sources.
    #[arg(long)]
    sql: Option<String>,

    /// Append an Arrow IPC stdout destination to the block's `destinations:`.
    #[arg(long)]
    arrow: bool,

    /// Write a JSONL results file to this path. Run `ldrs schema results` for the line shapes.
    #[arg(long)]
    results: Option<String>,
}

#[derive(Subcommand)]
enum Destination {
    /// Load from a config file. All sources and destinations.
    Ld(ConfigArgs),
    /// Delta table maintenance
    Delta {
        #[command(subcommand)]
        command: DeltaCommands,
    },
    /// Singular load from inline config and/or cli args
    Run(RunArgs),
    /// Discover available sources, destinations, and config options. Start here for one-off runs.
    Schema {
        #[command(subcommand)]
        command: Option<cli_schema::SchemaCommands>,
    },
}

/// A single table named on the command line rather than resolved from a config.
fn target_from_url(url: &str) -> DeltaTarget {
    DeltaTarget {
        name: url.to_string(),
        target: url.to_string(),
        table_path: ldrs::delta::storage_url(url).to_string(),
        order_column: None,
    }
}

/// Read the config a maintenance verb was pointed at and resolve its delta destinations.
fn targets_from_config(args: &ConfigArgs) -> Result<Vec<DeltaTarget>, anyhow::Error> {
    let config_string = fs::read_to_string(&args.config)
        .with_context(|| format!("Failed to read config file: {}", args.config))?;
    let ldrs_env = get_all_ldrs_env_vars();
    let config: LdrsConfig =
        serde_yaml::from_str(&config_string).context("Could not parse the config")?;
    let configs = parse_tables(&config, infer_env_type("LDRS_SRC", &ldrs_env))?;
    resolve_delta_targets(configs, args.select.clone(), &ldrs_env)
}

async fn vacuum_target(
    target: &DeltaTarget,
    args: &VacuumArgs,
    dry_run: bool,
    cloud_io: &tokio::runtime::Handle,
) -> Result<(), anyhow::Error> {
    let outcome = vacuum(&target.table_path, args.retention(), dry_run, cloud_io).await?;
    info!(
        target = %target.target,
        listed = outcome.files_listed,
        kept = outcome.files_kept,
        selected = outcome.files_selected,
        deleted = outcome.files_deleted,
        retention_secs = outcome.retention_used.as_secs(),
        dry_run = outcome.dry_run,
        errors = outcome.delete_errors.len(),
        "vacuum complete"
    );
    Ok(())
}

async fn optimize_target(
    target: &DeltaTarget,
    args: &OptimizeArgs,
    dry_run: bool,
    cloud_io: &tokio::runtime::Handle,
) -> Result<(), anyhow::Error> {
    // The flag wins over the merge key the config supplied.
    let order_column = args
        .order_column
        .as_deref()
        .or(target.order_column.as_deref());
    let plan = plan_optimize(&target.table_path, args.target_size, order_column, cloud_io).await?;

    let bins = plan.bins().len();
    let input_files: usize = plan.bins().iter().map(|bin| bin.input_files()).sum();
    if dry_run {
        info!(
            target = %target.target,
            bins,
            input_files,
            "optimize plan (dry run, nothing written)"
        );
        return Ok(());
    }

    let config = OperationConfig::new(ldrs::ENGINE_INFO);
    let outcome = execute_plan(plan, &config, cloud_io).await?;
    info!(
        target = %target.target,
        version = ?outcome.version,
        files_added = outcome.files_added,
        files_removed = outcome.files_removed,
        bytes_added = outcome.bytes_added,
        bytes_removed = outcome.bytes_removed,
        deletion_vectors_removed = outcome.deletion_vectors_removed,
        skipped = outcome.skipped,
        "optimize complete"
    );
    Ok(())
}

async fn checkpoint_target(
    target: &DeltaTarget,
    cloud_io: &tokio::runtime::Handle,
) -> Result<(), anyhow::Error> {
    let outcome = ldrs_delta::checkpoint(&target.table_path, cloud_io).await?;
    info!(
        target = %target.target,
        version = outcome.version,
        written = outcome.written,
        "checkpoint complete"
    );
    Ok(())
}

/// Optimize, then vacuum, then checkpoint.
async fn maintain_target(
    target: &DeltaTarget,
    optimize: &OptimizeArgs,
    vacuum: &VacuumArgs,
    cloud_io: &tokio::runtime::Handle,
) -> Result<(), anyhow::Error> {
    let mut failed = Vec::new();
    if let Err(e) = optimize_target(target, optimize, false, cloud_io).await {
        error!(target = %target.target, "optimize failed: {e:#}");
        failed.push("optimize");
    }
    if let Err(e) = vacuum_target(target, vacuum, false, cloud_io).await {
        error!(target = %target.target, "vacuum failed: {e:#}");
        failed.push("vacuum");
    }
    if let Err(e) = checkpoint_target(target, cloud_io).await {
        error!(target = %target.target, "checkpoint failed: {e:#}");
        failed.push("checkpoint");
    }
    match failed.is_empty() {
        true => Ok(()),
        false => Err(anyhow::anyhow!("{}", failed.join(", "))),
    }
}

async fn run_vacuum(
    targets: Vec<DeltaTarget>,
    args: &VacuumArgs,
    dry_run: bool,
    cloud_io: &tokio::runtime::Handle,
    _results: &Results,
) -> Result<(), anyhow::Error> {
    let mut failed = Vec::new();
    for target in targets {
        info!(target = %target.target, url = %target.table_path, "vacuuming");
        if let Err(e) = vacuum_target(&target, args, dry_run, cloud_io).await {
            error!(target = %target.target, "vacuum failed: {e:#}");
            failed.push(target.target);
        }
    }
    report_failures("vacuum", failed)
}

async fn run_optimize(
    targets: Vec<DeltaTarget>,
    args: &OptimizeArgs,
    dry_run: bool,
    cloud_io: &tokio::runtime::Handle,
    _results: &Results,
) -> Result<(), anyhow::Error> {
    let mut failed = Vec::new();
    for target in targets {
        info!(target = %target.target, url = %target.table_path, "optimizing");
        if let Err(e) = optimize_target(&target, args, dry_run, cloud_io).await {
            error!(target = %target.target, "optimize failed: {e:#}");
            failed.push(target.target);
        }
    }
    report_failures("optimize", failed)
}

async fn run_maintenance(
    targets: Vec<DeltaTarget>,
    optimize: &OptimizeArgs,
    vacuum: &VacuumArgs,
    cloud_io: &tokio::runtime::Handle,
    _results: &Results,
) -> Result<(), anyhow::Error> {
    let mut failed = Vec::new();
    for target in targets {
        info!(target = %target.target, url = %target.table_path, "maintaining");
        if let Err(phases) = maintain_target(&target, optimize, vacuum, cloud_io).await {
            error!(target = %target.target, "maintenance incomplete: {phases}");
            failed.push(target.target);
        }
    }
    report_failures("maintenance", failed)
}

/// A run that could not finish every table exits non-zero naming them, whatever else it managed.
fn report_failures(verb: &str, failed: Vec<String>) -> Result<(), anyhow::Error> {
    match failed.is_empty() {
        true => Ok(()),
        false => Err(anyhow::anyhow!(
            "{verb} failed for {} table(s): {}",
            failed.len(),
            failed.join(", ")
        )),
    }
}

/// The one table `run` executes, as a single-table config.
fn run_config(args: &RunArgs) -> Result<LdrsConfig, anyhow::Error> {
    let mut block: Mapping = match args.config_inline.as_deref() {
        Some(s) => serde_yaml::from_str::<Mapping>(s)?,
        None => Mapping::new(),
    };
    for (k, v) in [("src", &args.src), ("name", &args.name), ("sql", &args.sql)] {
        if let Some(v) = v {
            block.insert(k.into(), v.as_str().into());
        }
    }
    if args.arrow {
        let arrow = Value::Mapping(Mapping::from_iter([("dest".into(), "arrow".into())]));
        match block.get_mut("destinations") {
            Some(Value::Sequence(destinations)) => destinations.push(arrow),
            None | Some(Value::Null) => {
                block.insert("destinations".into(), Value::Sequence(vec![arrow]));
            }
            // not a list: left for the parse to reject
            Some(_) => {}
        }
    }
    Ok(LdrsConfig {
        src: None,
        src_defaults: None,
        version: None,
        destinations: None,
        finalize: None,
        lua_modules: None,
        tables: vec![Value::Mapping(block)],
    })
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

#[derive(Clone, ValueEnum)]
enum LogFormat {
    /// Human-readable lines (default)
    Text,
    /// One JSON object per line
    Json,
}

#[derive(Parser)]
#[command(author, version, about, long_about = None, before_help = BANNER)]
struct Cli {
    /// Log output format on stderr
    #[arg(long, global = true, env = "LDRS_LOG_FORMAT", default_value = "text")]
    log_format: LogFormat,

    #[command(subcommand)]
    destination: Option<Destination>,
}

/// The `--results` path a command was given, if that command takes one.
fn results_path(destination: &Destination) -> Option<&str> {
    let args = match destination {
        Destination::Ld(config) => &config.results,
        Destination::Run(run) => &run.results,
        Destination::Delta { command } => match command {
            DeltaCommands::Vacuum(args) => &args.config.results,
            DeltaCommands::Optimize(args) => &args.config.results,
            DeltaCommands::Maintenance(args) => &args.config.results,
            DeltaCommands::Register(args) => &args.results,
            DeltaCommands::VacuumTable(_)
            | DeltaCommands::OptimizeTable(_)
            | DeltaCommands::MaintenanceTable(_) => &None,
        },
        Destination::Schema { .. } => &None,
    };
    args.as_deref()
}

/// Exits 1 when the task can be re-run, 3 when a destination committed and the retry is a repair.
fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => std::process::ExitCode::from(e.code()),
    }
}

fn run() -> Result<(), RunError> {
    let _ = dotenv();
    let cli = Cli::parse();
    let builder = fmt::Subscriber::builder()
        .with_writer(io::stderr)
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,delta_kernel=warn")),
        );
    match cli.log_format {
        LogFormat::Json => builder.json().init(),
        LogFormat::Text => builder.init(),
    }

    let Some(destination) = cli.destination else {
        Cli::command()
            .print_help()
            .with_context(|| "could not print help")?;
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
        // Opened before any command runs, so an unwritable path fails with nothing else done yet.
        let results = Results::create(results_path(&destination))?;
        match destination {
            Destination::Ld(args) => {
                let config_string = fs::read_to_string(&args.config)
                    .with_context(|| format!("Failed to read config file: {}", args.config))?;
                let ldrs_env = get_all_ldrs_env_vars();
                let config: LdrsConfig =
                    serde_yaml::from_str(&config_string).context("Could not parse the config")?;
                let configs = parse_tables(&config, infer_env_type("LDRS_SRC", &ldrs_env))?;
                execute_configs(configs, args.select, &ldrs_env, rt.handle(), &results).await
            }
            Destination::Delta { command } => match command {
                DeltaCommands::Vacuum(args) => {
                    let targets = targets_from_config(&args.config)?;
                    run_vacuum(targets, &args.vacuum, args.dry_run, rt.handle(), &results).await
                }
                DeltaCommands::VacuumTable(args) => {
                    let targets = vec![target_from_url(&args.url)];
                    run_vacuum(targets, &args.vacuum, args.dry_run, rt.handle(), &results).await
                }
                DeltaCommands::Optimize(args) => {
                    let targets = targets_from_config(&args.config)?;
                    run_optimize(targets, &args.optimize, args.dry_run, rt.handle(), &results).await
                }
                DeltaCommands::OptimizeTable(args) => {
                    let targets = vec![target_from_url(&args.url)];
                    run_optimize(targets, &args.optimize, args.dry_run, rt.handle(), &results).await
                }
                DeltaCommands::Maintenance(args) => {
                    let targets = targets_from_config(&args.config)?;
                    run_maintenance(targets, &args.optimize, &args.vacuum, rt.handle(), &results)
                        .await
                }
                DeltaCommands::MaintenanceTable(args) => {
                    let targets = vec![target_from_url(&args.url)];
                    run_maintenance(targets, &args.optimize, &args.vacuum, rt.handle(), &results)
                        .await
                }
                DeltaCommands::Register(args) => {
                    let config_string = fs::read_to_string(&args.config)
                        .with_context(|| format!("Failed to read config file: {}", args.config))?;
                    let ldrs_env = get_all_ldrs_env_vars();
                    let config: LdrsConfig = serde_yaml::from_str(&config_string)
                        .context("Could not parse the config")?;
                    let configs = parse_tables(&config, infer_env_type("LDRS_SRC", &ldrs_env))?;
                    let failed =
                        register_from_config(configs, args.select, &ldrs_env, &results).await?;
                    report_failures("register", failed)
                }
            }
            .map_err(RunError::from),
            Destination::Run(args) => {
                let ldrs_env = get_all_ldrs_env_vars();
                let configs =
                    parse_tables(&run_config(&args)?, infer_env_type("LDRS_SRC", &ldrs_env))?;
                execute_configs(configs, None, &ldrs_env, rt.handle(), &results).await
            }
            Destination::Schema { command } => match command {
                None => {
                    // bare `ldrs schema` list the subcommands
                    let mut cmd = Cli::command();
                    if let Some(sub) = cmd.find_subcommand_mut("schema") {
                        sub.print_help().with_context(|| "could not print help")?;
                        println!();
                    }
                    Ok(())
                }
                Some(cmd) => {
                    let output = cli_schema::build(&cmd);
                    let json = serde_json::to_string_pretty(&output)
                        .with_context(|| "could not render the schema")?;
                    println!("{json}");
                    Ok(())
                }
            },
        }
    });

    drop(main_rt);
    drop(rt);

    // The timing line is the last thing an operator watching the log sees, so it has to carry the verdict
    let end = std::time::Instant::now();
    if is_data_command {
        match &command_exec {
            Ok(()) => info!("Execution time: {:?}", end - start),
            Err(e) => error!("Failed after {:?}: {:#}", end - start, e),
        }
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
            name: None,
            sql: None,
            arrow: false,
            results: None,
        }
    }

    /// The single table block `run_config` builds.
    fn run_block(args: &RunArgs) -> Value {
        let config = run_config(args).unwrap();
        assert_eq!(config.tables.len(), 1);
        config.tables[0].clone()
    }

    fn get_str<'a>(v: &'a Value, key: &str) -> Option<&'a str> {
        v.get(key).and_then(|x| x.as_str())
    }

    fn dests(v: &Value) -> Vec<&str> {
        v.get("destinations")
            .and_then(Value::as_sequence)
            .map(|seq| seq.iter().filter_map(|d| get_str(d, "dest")).collect())
            .unwrap_or_default()
    }

    #[test]
    fn run_config_inline_only() {
        let args = RunArgs {
            config_inline: Some("src: file".to_string()),
            ..empty_args()
        };
        assert_eq!(get_str(&run_block(&args), "src"), Some("file"));
    }

    #[test]
    fn run_config_flag_only() {
        let args = RunArgs {
            src: Some("sf".to_string()),
            ..empty_args()
        };
        assert_eq!(get_str(&run_block(&args), "src"), Some("sf"));
    }

    #[test]
    fn run_config_flag_beats_inline() {
        let args = RunArgs {
            config_inline: Some("src: file".to_string()),
            src: Some("sf".to_string()),
            ..empty_args()
        };
        assert_eq!(get_str(&run_block(&args), "src"), Some("sf"));
    }

    #[test]
    fn run_config_without_arrow_adds_no_destination() {
        assert!(run_block(&empty_args()).get("destinations").is_none());
    }

    #[test]
    fn arrow_is_the_only_destination_when_the_block_has_none() {
        let args = RunArgs {
            arrow: true,
            ..empty_args()
        };
        assert_eq!(dests(&run_block(&args)), vec!["arrow"]);
    }

    #[test]
    fn arrow_fills_an_empty_destinations_key() {
        let args = RunArgs {
            config_inline: Some("destinations:".to_string()),
            arrow: true,
            ..empty_args()
        };
        assert_eq!(dests(&run_block(&args)), vec!["arrow"]);
    }

    #[test]
    fn arrow_appends_to_the_block_destinations() {
        let args = RunArgs {
            config_inline: Some("destinations: [{dest: pq, filename: out.parquet}]".to_string()),
            arrow: true,
            ..empty_args()
        };
        assert_eq!(dests(&run_block(&args)), vec!["pq", "arrow"]);
    }
}
