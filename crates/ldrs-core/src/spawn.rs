//! Spawn an external process that emits an Arrow IPC stream on stdout, and pump it back as a
//! bounded batch stream. Everything here is resolved: the binary, args, an optional stdin script,
//! and the complete child environment are handed in. The child-env policy (which `LDRS_*` vars to
//! strip or keep) belongs to the shell.

use std::ffi::OsString;
use std::io::{BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;

use anyhow::Context;
use arrow::ipc::reader::StreamReader;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{self, JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, trace};

/// A resolved external source: the binary, its args, an optional stdin script, and the complete
/// child environment. Variable names are UTF-8; values are not required to be.
pub struct Spawned {
    pub binary: PathBuf,
    pub args: Vec<String>,
    pub stdin: Option<String>,
    pub env: Vec<(String, OsString)>,
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
///
/// All three pipes are serviced concurrently. Each holds a fixed-size OS buffer, so a child that
/// fills stderr, or one still being fed stdin while its stdout backs up, blocks and never exits.
/// stdout stays on this thread (`StreamReader` is a blocking reader); stdin and stderr get one
/// thread each, joined before the task ends.
pub fn spawn_arrow_source(spec: Spawned) -> SpawnedStream {
    let (tx, rx) = mpsc::channel(16);
    let (schema_tx, schema_rx) = oneshot::channel();

    let command_handle = task::spawn_blocking(move || {
        // Names only. `Command`'s own Debug renders the environment with values
        let env_keys: Vec<&str> = spec.env.iter().map(|(key, _)| key.as_str()).collect();
        debug!(
            "Running {:?} {:?} with child env keys: {:?}",
            spec.binary, spec.args, env_keys
        );
        drop(env_keys);

        let mut cmd = Command::new(&spec.binary);
        cmd.args(&spec.args)
            .env_clear()
            .envs(spec.env)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if spec.stdin.is_some() {
            cmd.stdin(Stdio::piped());
        }
        let mut child = cmd
            .spawn()
            .with_context(|| format!("Failed to spawn {:?}", spec.binary))?;

        let stdin_writer = match spec.stdin {
            Some(script) => {
                let mut handle = child
                    .stdin
                    .take()
                    .ok_or_else(|| anyhow::anyhow!("Failed to capture stdin"))?;
                Some(thread::spawn(move || -> Result<(), anyhow::Error> {
                    let written = handle.write_all(script.as_bytes());
                    // Dropping the handle closes the pipe so the child sees EOF.
                    drop(handle);
                    written.map_err(anyhow::Error::from)
                }))
            }
            None => None,
        };

        let mut stderr_handle = child
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stderr"))?;
        let stderr_drain = thread::spawn(move || -> Result<String, anyhow::Error> {
            let mut buf = String::new();
            stderr_handle.read_to_string(&mut buf)?;
            Ok(buf)
        });

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout"))?;
        let mut stream_reader = StreamReader::try_new(BufReader::new(stdout), None).ok();
        let stream_opened = stream_reader.is_some();

        let stream_result: Result<(), anyhow::Error> = match stream_reader.as_mut() {
            None => Ok(()),
            Some(reader) => match schema_tx.send(reader.schema()) {
                Err(_) => Err(anyhow::anyhow!("Failed to send schema: receiver dropped")),
                Ok(()) => {
                    let mut forwarded = Ok(());
                    for batch_result in reader {
                        trace!("Processing batch");
                        if tx
                            .blocking_send(batch_result.map_err(anyhow::Error::from))
                            .is_err()
                        {
                            forwarded = Err(anyhow::anyhow!("Failed to send batch"));
                            break;
                        }
                    }
                    forwarded
                }
            },
        };
        // Close our end of stdout before waiting on the child.
        drop(stream_reader);

        // Abandoning the stream leaves the child writing into a pipe nobody drains, so `wait`
        // would never return.
        if stream_result.is_err() {
            let _ = child.kill();
        }

        let status = child.wait()?;
        let stderr_output = stderr_drain
            .join()
            .map_err(|_| anyhow::anyhow!("stderr drain panicked"))
            .flatten()?;
        let stdin_result = match stdin_writer {
            Some(handle) => handle
                .join()
                .map_err(|_| anyhow::anyhow!("stdin writer panicked"))
                .flatten(),
            None => Ok(()),
        };

        // The child's verdict outranks our plumbing: a script that fails early closes the stdin
        // pipe, so a broken-pipe write is a symptom of that failure rather than the cause.
        match (status.success(), stream_opened, stream_result) {
            (false, _, _) => Err(anyhow::anyhow!(
                "Spawned command failed with status: {}. Stderr: {}",
                status,
                stderr_output
            )),
            (true, _, Err(e)) => Err(e),
            (true, false, Ok(())) => Err(anyhow::anyhow!(
                "Command succeeded but failed to open Arrow stream. Stderr: {}",
                stderr_output
            )),
            (true, true, Ok(())) => {
                if !stderr_output.trim().is_empty() {
                    debug!("child stderr: {}", stderr_output);
                }
                stdin_result.with_context(|| "Failed to write stdin script")
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
