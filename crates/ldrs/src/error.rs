//! The run's exit code. `RunError` carries one variant per code.

/// A failed run, tagged with what a caller should do about it.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// Nothing durable landed, so re-running the task is the retry.
    #[error(transparent)]
    Fatal(#[from] anyhow::Error),
    /// A destination committed and something else failed, so the retry is a repair.
    #[error("{0}")]
    Partial(String),
}

impl RunError {
    /// The process exit code for this failure.
    pub fn code(&self) -> u8 {
        match self {
            RunError::Fatal(_) => 1,
            RunError::Partial(_) => 3,
        }
    }
}
