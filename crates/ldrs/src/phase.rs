//! Per-destination outcome of a completed load phase, assembled by the executor.

use schemars::JsonSchema;
use serde::Serialize;

#[derive(Debug, Serialize, JsonSchema)]
pub enum DeltaStrategy {
    Overwrite,
    Merge,
}

/// One parquet file written by the run.
#[derive(Debug, Serialize, JsonSchema)]
pub struct FileWritten {
    pub path: String,
    pub rows: u64,
}

/// One destination's identity (the variant and its fields) and its outcome (`result`). Identity
/// is known regardless of outcome; `result` carries the success output or the error.
#[derive(Debug, Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum DestinationOutcome {
    Pg {
        table: String,
        result: Result<(), String>,
    },
    Delta {
        location: String,
        strategy: DeltaStrategy,
        result: Result<(), String>,
    },
    Parquet {
        location: String,
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
