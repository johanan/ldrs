//! Behaviour of the spawned Arrow source: concurrent stdin/stderr handling, and the settlement
//! verdict a caller receives.
//!
//! duckdb is the producer for the realistic cases

use arrow::ipc::writer::StreamWriter;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use ldrs_core::spawn::{resolve_schema, spawn_arrow_source, Spawned};
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// Suppresses the CLI's own result rendering. Without it any row-returning statement writes a
/// formatted table to the same stdout the IPC stream uses; `COPY ... TO '/dev/stdout'` is
/// unaffected because it opens that path itself.
const PRELUDE: &str = ".output /dev/null\nLOAD nanoarrow;\n";

fn duckdb_source(script: String) -> Spawned {
    Spawned {
        binary: which::which("duckdb")
            .expect("duckdb must be on PATH; see scripts/setup_duckdb.sh"),
        args: vec!["-bail".to_string(), "-no-init".to_string()],
        stdin: Some(script),
        env: std::env::vars()
            .map(|(key, value)| (key, value.into()))
            .collect(),
    }
}

fn shell_source(command: String, stdin: Option<String>) -> Spawned {
    Spawned {
        binary: PathBuf::from("/bin/sh"),
        args: vec!["-c".to_string(), command],
        stdin,
        env: std::env::vars()
            .map(|(key, value)| (key, value.into()))
            .collect(),
    }
}

/// Write a valid multi-batch IPC stream to a temp file. The returned offsets are the file length
/// after each batch, so a caller can cut the stream on a message boundary rather than mid-message.
fn write_ipc_fixture(name: &str, batches: usize, rows_per_batch: i64) -> (PathBuf, Vec<u64>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let path = std::env::temp_dir().join(name);
    let mut writer = StreamWriter::try_new(File::create(&path).unwrap(), &schema).unwrap();
    let mut boundaries = Vec::with_capacity(batches);
    for batch in 0..batches as i64 {
        let ids: Vec<i64> = (0..rows_per_batch)
            .map(|i| batch * rows_per_batch + i)
            .collect();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
        writer.write(&batch).unwrap();
        writer.flush().unwrap();
        boundaries.push(writer.get_ref().metadata().unwrap().len());
    }
    writer.finish().unwrap();
    (path, boundaries)
}

struct Run {
    schema: bool,
    batches: usize,
    rows: usize,
    settled: Result<(), String>,
}

/// Drive a source to completion. Bounded, because the failure these tests exist to catch is a hang
/// rather than a wrong answer, and `cargo test` has no per-test timeout.
async fn run(spec: Spawned) -> Run {
    let drive = async {
        let source = spawn_arrow_source(spec);
        let schema = resolve_schema(source.schema_rx).await.is_some();
        let mut stream = source.batch_stream;
        let (mut batches, mut rows) = (0, 0);
        let mut stream_err = None;
        while let Some(batch) = stream.next().await {
            match batch {
                Ok(batch) => {
                    batches += 1;
                    rows += batch.num_rows();
                }
                Err(e) => {
                    stream_err = Some(format!("{e:#}"));
                    break;
                }
            }
        }
        let settled = match stream_err {
            Some(e) => Err(e),
            None => source
                .command_handle
                .await
                .expect("cleanup task panicked")
                .map_err(|e| format!("{e:#}")),
        };
        Run {
            schema,
            batches,
            rows,
            settled,
        }
    };

    tokio::time::timeout(Duration::from_secs(60), drive)
        .await
        .expect("spawned source deadlocked")
}

#[test_log::test(tokio::test)]
async fn stdin_script_streams_batches() {
    let out = run(duckdb_source(format!(
        "{PRELUDE}COPY (SELECT i::BIGINT AS id FROM range(100000) t(i)) \
         TO '/dev/stdout' (FORMAT arrows);"
    )))
    .await;

    assert!(out.schema, "schema should arrive once the stream opens");
    assert_eq!(out.rows, 100_000);
    assert!(out.batches > 0);
    assert_eq!(out.settled, Ok(()));
}

/// A script far larger than a pipe buffer, in the shape ldrs actually emits (the `COPY` last).
#[test_log::test(tokio::test)]
async fn large_stdin_script_streams_batches() {
    let padding = format!("-- {}\n", "x".repeat(100)).repeat(2000);
    assert!(
        padding.len() > 64 * 1024,
        "script must exceed a pipe buffer"
    );

    let out = run(duckdb_source(format!(
        "{PRELUDE}{padding}COPY (SELECT i::BIGINT AS id FROM range(200000) t(i)) \
         TO '/dev/stdout' (FORMAT arrows);"
    )))
    .await;

    assert_eq!(out.rows, 200_000);
    assert_eq!(out.settled, Ok(()));
}

/// The regression test for concurrent stdin. This producer writes its entire stream to stdout
/// *before* reading stdin, so sending the whole stdin payload before draining stdout deadlocks: the
/// producer blocks on a full stdout pipe, never reaches its read, and the writer blocks in turn.
#[test_log::test(tokio::test)]
async fn stdin_is_written_while_stdout_drains() {
    let (fixture, _) = write_ipc_fixture("ldrs_spawn_backpressure.arrows", 20, 20_000);
    let payload = "x".repeat(256 * 1024);

    let out = run(shell_source(
        format!("cat '{}'; cat >/dev/null", fixture.display()),
        Some(payload),
    ))
    .await;

    assert_eq!(out.rows, 400_000, "the whole stream must arrive");
    assert_eq!(out.settled, Ok(()));

    let _ = std::fs::remove_file(fixture);
}

/// A failing statement closes the child's stdin, so the writer takes a broken pipe. The child's own
/// message has to outrank it, or the real cause never surfaces.
#[test_log::test(tokio::test)]
async fn child_error_reports_stderr_not_the_broken_pipe() {
    let out = run(duckdb_source(format!(
        "{PRELUDE}COPY (SELECT * FROM does_not_exist) TO '/dev/stdout' (FORMAT arrows);"
    )))
    .await;

    assert!(!out.schema);
    assert_eq!(out.rows, 0);
    let err = out.settled.expect_err("bad SQL must fail the source");
    assert!(err.contains("Catalog Error"), "got: {err}");
    assert!(
        !err.contains("Failed to write stdin script"),
        "the plumbing error must not mask the child's: {err}"
    );
}

#[test_log::test(tokio::test)]
async fn missing_extension_fails_before_the_stream_opens() {
    let out = run(duckdb_source(
        "LOAD no_such_extension;\nSELECT 1;".to_string(),
    ))
    .await;

    assert!(!out.schema);
    assert_eq!(out.batches, 0);
    let err = out.settled.expect_err("a missing extension must fail");
    assert!(err.contains("no_such_extension"), "got: {err}");
}

/// Without `.output /dev/null` a row-returning statement writes a rendered table ahead of the IPC
/// bytes. The reader rejects the stream, we close stdout, and the child dies on SIGPIPE.
#[test_log::test(tokio::test)]
async fn renderer_output_before_copy_breaks_the_stream() {
    let out = run(duckdb_source(
        "LOAD nanoarrow;\nSELECT 'noise' AS n;\n\
         COPY (SELECT 1 AS a) TO '/dev/stdout' (FORMAT arrows);"
            .to_string(),
    ))
    .await;

    assert!(
        !out.schema,
        "the IPC stream must not open behind rendered output"
    );
    assert_eq!(out.rows, 0);
    assert!(out.settled.is_err());
}

/// Cut on a message boundary: complete batches arrive, the stream ends without error and without
/// an EOS marker, and nothing in the data says anything is wrong.
#[test_log::test(tokio::test)]
async fn stream_ended_between_batches_fails_on_exit_status() {
    let (fixture, boundaries) = write_ipc_fixture("ldrs_spawn_between_batches.arrows", 8, 2000);
    let cut = boundaries[4];

    let out = run(shell_source(
        format!("head -c {cut} '{}'; exit 1", fixture.display()),
        None,
    ))
    .await;

    assert!(out.schema);
    assert_eq!(out.rows, 5 * 2000, "five whole batches, cleanly ended");
    let err = out
        .settled
        .expect_err("a child that exited non-zero must fail the source");
    assert!(
        err.contains("Spawned command failed with status"),
        "must settle on the exit status, not a stream error: {err}"
    );

    let _ = std::fs::remove_file(fixture);
}

/// Cut mid-message instead: the reader itself rejects the stream, so the failure arrives as a batch
/// error rather than through settlement.
#[test_log::test(tokio::test)]
async fn stream_cut_mid_message_fails_on_the_batch() {
    let (fixture, boundaries) = write_ipc_fixture("ldrs_spawn_mid_message.arrows", 8, 2000);
    let cut = boundaries[4] + 64;

    let out = run(shell_source(
        format!("head -c {cut} '{}'; exit 1", fixture.display()),
        None,
    ))
    .await;

    assert!(out.schema);
    assert_eq!(out.rows, 5 * 2000);
    out.settled.expect_err("a partial message must fail");

    let _ = std::fs::remove_file(fixture);
}
