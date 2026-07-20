//! Spawn an external process that emits an Arrow IPC stream on stdout, and pump it back as a
//! bounded batch stream. Everything here is resolved: the binary, args, an optional stdin script,
//! and the complete child environment are handed in. The child-env policy (which `LDRS_*` vars to
//! strip or keep) belongs to the shell.

use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

use anyhow::Context;
use arrow::ipc::reader::StreamReader;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{self, JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, trace};

/// A resolved external source: the binary, its args, an optional stdin script, and the complete
/// child environment.
pub struct Spawned {
    pub binary: PathBuf,
    pub args: Vec<String>,
    pub stdin: Option<String>,
    pub env: Vec<(String, String)>,
}

/// The pumped output of a spawned source: the batch stream, the schema (sent once the stream
/// opens), and the child's cleanup handle carrying the settlement verdict.
pub struct SpawnedStream {
    pub batch_stream: ReceiverStream<Result<RecordBatch, anyhow::Error>>,
    pub schema_rx: oneshot::Receiver<SchemaRef>,
    pub command_handle: JoinHandle<Result<(), anyhow::Error>>,
}

/// Spawn the process and pump its stdout Arrow IPC stream into a bounded channel. Settlement:
/// a truncated stream reads without error, so the child's exit status (and whether the stream
/// opened at all) is the integrity signal, reported through `command_handle`.
pub fn spawn_arrow_source(spec: Spawned) -> SpawnedStream {
    let (tx, rx) = mpsc::channel(16);
    let (schema_tx, schema_rx) = oneshot::channel();

    let command_handle = task::spawn_blocking(move || {
        let mut cmd = Command::new(&spec.binary);
        cmd.args(&spec.args)
            .env_clear()
            .envs(spec.env)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if spec.stdin.is_some() {
            cmd.stdin(Stdio::piped());
        }
        debug!("Running command: {:?}", cmd);
        let mut child = cmd
            .spawn()
            .with_context(|| format!("Failed to spawn {:?}", spec.binary))?;

        if let Some(script) = &spec.stdin {
            child
                .stdin
                .take()
                .ok_or_else(|| anyhow::anyhow!("Failed to capture stdin"))?
                .write_all(script.as_bytes())
                .with_context(|| "Failed to write stdin script")?;
            // The stdin handle drops here, closing the pipe so the child sees EOF.
        }

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout"))?;
        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stderr"))?;

        let buf_reader = BufReader::new(stdout);
        let stream_reader = StreamReader::try_new(buf_reader, None).ok();
        let stream_opened = stream_reader.is_some();

        if let Some(mut stream_reader) = stream_reader {
            let schema = stream_reader.schema();
            if schema_tx.send(schema).is_err() {
                return Err(anyhow::anyhow!("Failed to send schema: receiver dropped"));
            }
            while let Some(batch_result) = stream_reader.next() {
                trace!("Processing batch");
                if tx
                    .blocking_send(batch_result.map_err(anyhow::Error::from))
                    .is_err()
                {
                    return Err(anyhow::anyhow!("Failed to send batch"));
                }
            }
        }

        let status = child.wait()?;
        match (status.success(), stream_opened) {
            (true, true) => Ok(()),
            (true, false) => Err(anyhow::anyhow!(
                "Command succeeded but failed to open Arrow stream"
            )),
            (false, _) => {
                let mut stderr_output = String::new();
                stderr.read_to_string(&mut stderr_output)?;
                Err(anyhow::anyhow!(
                    "Spawned command failed with status: {}. Stderr: {}",
                    status,
                    stderr_output
                ))
            }
        }
    });

    SpawnedStream {
        batch_stream: ReceiverStream::new(rx),
        schema_rx,
        command_handle,
    }
}

/// Await the schema a spawned source sends once its stream opens. Empty fields (or a dropped
/// sender, i.e. the stream never opened) mean no schema.
pub async fn resolve_schema(rx: oneshot::Receiver<SchemaRef>) -> Option<SchemaRef> {
    match rx.await {
        Ok(schema) if !schema.fields().is_empty() => {
            debug!("Schema: {:?}", schema);
            Some(schema)
        }
        _ => None,
    }
}
