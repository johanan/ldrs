//! Per-destination outcome of a completed load phase, assembled by the executor.

use ldrs_arrow::ColumnSpec;
use schemars::JsonSchema;
use serde::Serialize;

/// What a committed Delta write did. `Overwrite` carries nothing; `Merge` carries the merge stats
#[derive(Debug, Serialize, JsonSchema)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum DeltaCommit {
    Overwrite,
    Merge {
        skipped: bool,
        skipped_version: Option<i64>,
        source_rows: u64,
        matched_rows: u64,
        inserted_rows: u64,
        files_scanned: u64,
        files_written: u64,
    },
}

/// One parquet file written by the run. `full_url` is the file's fully-qualified URL; `path` is the
/// writer's path relative to the destination base (the namer output), portable across environments.
#[derive(Debug, Serialize, JsonSchema)]
pub struct FileWritten {
    pub full_url: String,
    pub path: String,
    pub rows: u64,
}

/// One destination's identity and its outcome (`result`). Identity is known regardless of outcome;
/// `result` carries the success output or the error. `target` is the resolved logical name
/// The URL-backed variants carry `full_url` (fully qualified: the Delta table, the
/// Parquet base directory). `columns` is the post-cast schema that actually landed. Parquet's files
/// are listed per-entry in `result`.
#[derive(Debug, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum DestinationOutcome {
    Pg {
        target: String,
        columns: Vec<ColumnSpec>,
        result: Result<(), String>,
    },
    Delta {
        target: String,
        full_url: String,
        columns: Vec<ColumnSpec>,
        result: Result<DeltaCommit, String>,
    },
    Parquet {
        target: String,
        full_url: String,
        columns: Vec<ColumnSpec>,
        result: Result<Vec<FileWritten>, String>,
    },
}

impl DestinationOutcome {
    /// The outcome with the success payload mapped out
    pub fn status(&self) -> Result<(), &String> {
        match self {
            DestinationOutcome::Pg { result, .. } => result.as_ref().map(|_| ()),
            DestinationOutcome::Delta { result, .. } => result.as_ref().map(|_| ()),
            DestinationOutcome::Parquet { result, .. } => result.as_ref().map(|_| ()),
        }
    }

    /// Whether this destination committed.
    pub fn succeeded(&self) -> bool {
        self.status().is_ok()
    }
}

/// What a completed load phase produced. `rows` is the total streamed; `success` is true when
/// every destination committed.
#[derive(Debug, Serialize, JsonSchema)]
pub struct PhaseOutput {
    pub name: String,
    /// Source file paths, or `None` for a stream source (SF, stdin).
    pub source_files: Option<Vec<String>>,
    pub success: bool,
    pub rows: u64,
    pub destinations: Vec<DestinationOutcome>,
}
