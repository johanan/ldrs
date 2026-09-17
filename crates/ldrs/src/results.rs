//! The results file: one JSONL record per outcome, written as soon as that outcome is known.

use std::io::Write;

use anyhow::Context;
use ldrs_core::phase::PhaseOutput;
use schemars::JsonSchema;
use serde::Serialize;

/// One line of the results file. `kind` names the record and its fields sit beside it.
#[derive(Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResultLine<'a> {
    /// One per task: its load, written before any post-load phase runs.
    Load(&'a PhaseOutput),
    /// One per registration that ran: `created`, `refreshed`, or `unchanged`, or why it failed.
    Register {
        name: &'a str,
        target: &'a str,
        catalog_table: &'a str,
        result: Result<&'a str, &'a str>,
    },
    /// A task's finalize failures. Written only when there are some.
    Finalize { name: &'a str, errors: &'a [String] },
}

/// The open results file, or nothing when `--results` was not given.
#[derive(Default)]
pub struct Results(Option<std::fs::File>);

impl Results {
    /// Create the file, truncating a prior run's.
    pub fn create(path: Option<&str>) -> Result<Self, anyhow::Error> {
        let file = path
            .map(std::fs::File::create)
            .transpose()
            .with_context(|| {
                format!(
                    "could not create results file '{}'",
                    path.unwrap_or_default()
                )
            })?;
        Ok(Self(file))
    }

    /// Append one record, built whole and written in one call.
    pub fn write(&self, line: ResultLine) -> Result<(), anyhow::Error> {
        let Some(mut file) = self.0.as_ref() else {
            return Ok(());
        };
        let mut bytes =
            serde_json::to_vec(&line).with_context(|| "could not serialize results line")?;
        bytes.push(b'\n');
        file.write_all(&bytes)
            .and_then(|_| file.flush())
            .with_context(|| "could not write results line")
    }
}
