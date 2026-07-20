//! Resolved execution plan: what a shell hands core after rendering templates, resolving `LDRS_*`
//! env, and expanding strategies. Everything here is a resolved primitive or an already-resolved
//! engine type; no YAML, no Handlebars, no env vocabulary below this line.
//!
//! Not `derive(Debug/Clone)`: [`PqDest`] carries a [`FileNamer`] closure, so the enclosing types
//! can't derive either. Add a hand-written `Debug` (skipping the closure) if a consumer needs one.

use ldrs_arrow::ColumnSpec;
use ldrs_delta::MergeConfig;
use ldrs_parquet::FileNamer;
use ldrs_postgres::Command;

use crate::spawn::Spawned;

/// One source streamed into one or more destinations. `name` is the task identity carried into
/// `PhaseOutput`.
pub struct Task {
    pub name: String,
    pub source: SourceSpec,
    pub dests: Vec<DestSpec>,
}

/// A resolved source. Core opens it and produces the batch stream and schema.
pub enum SourceSpec {
    /// Object-store file (parquet today): the fully-joined, rendered URL. A single file for now;
    /// widens to a list when multi-file/glob sources land (`open_source` would concatenate them).
    File { url: String },
    /// External process emitting an Arrow IPC stream on stdout (ldrs-sf today, duckdb later).
    Spawned(Spawned),
}

/// A resolved destination. `columns` is the declared column set; the schema-derived target columns
/// and the cast are computed by the executor once the source schema is known.
pub enum DestSpec {
    Pg(PgDest),
    Delta(DeltaDest),
    Pq(PqDest),
    Arrow(ArrowDest),
}

impl DestSpec {
    /// The destination's declared column specs.
    pub fn columns(&self) -> &[ColumnSpec] {
        match self {
            DestSpec::Pg(pg) => &pg.columns,
            DestSpec::Delta(delta) => &delta.columns,
            DestSpec::Pq(pq) => &pq.columns,
            DestSpec::Arrow(arrow) => &arrow.columns,
        }
    }
}

/// A resolved Postgres load, minus the schema-derived columns: the strategy is already expanded
/// into `before`/`after` command sequences. The executor opens the connection from `conn_url`,
/// fills the columns from the source schema, and builds the execution `PgLoad`.
pub struct PgDest {
    pub conn_url: String,
    pub role: Option<String>,
    pub before: Vec<Command>,
    pub load_table: String,
    pub after: Vec<Command>,
    pub target: String,
    pub columns: Vec<ColumnSpec>,
}

/// A resolved Delta destination: the table URI and the operation.
pub struct DeltaDest {
    pub table_path: String,
    pub mode: DeltaMode,
    pub columns: Vec<ColumnSpec>,
}

pub enum DeltaMode {
    Overwrite {
        max_rows: Option<usize>,
        max_bytes: Option<usize>,
    },
    Merge(MergeConfig),
}

/// A resolved Parquet destination. `namer` is the resolved naming strategy (renders each rotation's
/// filename); the executor builds the writer props from `bloom_filters`.
pub struct PqDest {
    pub url: String,
    pub namer: FileNamer,
    pub bloom_filters: Vec<Vec<String>>,
    pub max_rows: Option<usize>,
    pub max_bytes: Option<usize>,
    pub columns: Vec<ColumnSpec>,
}

/// A resolved Arrow-IPC-to-stdout destination.
pub struct ArrowDest {
    pub columns: Vec<ColumnSpec>,
}
