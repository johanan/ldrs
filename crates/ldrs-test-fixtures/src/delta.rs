//! Helpers shared by the Delta integration tests: the batch the tables are built from, the
//! scratch table directory, log reading, and reading a table back with the DuckDB CLI.

use std::ops::Range;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

// 2026-01-01T00:00:00Z in microseconds
pub const TARGET_BASE_TS: i64 = 1_767_225_600_000_000;
// 2026-04-12T00:00:00Z in microseconds
pub const SOURCE_BASE_TS: i64 = 1_775_952_000_000_000;
// 1 minute in microseconds
pub const TS_STEP: i64 = 60_000_000;

pub fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]))
}

pub fn make_batch(id_range: Range<i64>, value_offset: i64, base_ts: i64) -> RecordBatch {
    make_batch_from_ids(id_range.collect(), value_offset, base_ts)
}

pub fn make_batch_from_ids(ids: Vec<i64>, value_offset: i64, base_ts: i64) -> RecordBatch {
    let values: Vec<i64> = ids.iter().map(|id| id + value_offset).collect();
    let names: Vec<String> = ids.iter().map(|id| format!("row-{:06}", id)).collect();
    let timestamps: Vec<i64> = ids.iter().map(|id| base_ts + id * TS_STEP).collect();

    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
            Arc::new(StringArray::from(names)),
            Arc::new(TimestampMicrosecondArray::from(timestamps).with_timezone("UTC")),
        ],
    )
    .unwrap()
}

pub fn make_target_batch(id_range: Range<i64>) -> RecordBatch {
    make_batch(id_range, 0, TARGET_BASE_TS)
}

pub fn make_source_batch(id_range: Range<i64>) -> RecordBatch {
    make_batch(id_range, 10_000, SOURCE_BASE_TS)
}

/// A scratch table directory under the calling test binary's `tests/test_data`. Callers prefix
/// `name` so two test files cannot collide on one directory.
pub fn delta_table_path(name: &str) -> String {
    let cd = std::env::current_dir().unwrap();
    format!("{}/tests/test_data/delta_writes/{}", cd.display(), name)
}

pub fn cleanup_table(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

/// Copy a checked-in table from the fixture directory into a scratch directory the test may write
/// to, and return that path.
pub fn copy_fixture_table(fixture: &str, name: &str) -> String {
    let destination = delta_table_path(name);
    cleanup_table(&destination);
    copy_dir(&crate::fixture(fixture), Path::new(&destination));
    destination
}

fn copy_dir(source: &Path, destination: &Path) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", source.display()))
    {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        match entry.file_type().unwrap().is_dir() {
            true => copy_dir(&entry.path(), &target),
            false => {
                std::fs::copy(entry.path(), &target).unwrap();
            }
        }
    }
}

pub fn read_log_actions(table_path: &str, version: u64) -> Vec<serde_json::Value> {
    let log_path = format!("{}/_delta_log/{:020}.json", table_path, version);
    let content = std::fs::read_to_string(&log_path)
        .unwrap_or_else(|_| panic!("Failed to read log version {}", version));
    content
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

/// Highest committed version in `_delta_log`.
pub fn latest_version(table_path: &str) -> u64 {
    let log_dir = format!("{}/_delta_log", table_path);
    std::fs::read_dir(&log_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            e.file_name()
                .to_str()
                .and_then(|n| n.strip_suffix(".json"))
                .and_then(|n| n.parse::<u64>().ok())
        })
        .max()
        .expect("at least one commit")
}

pub fn count_actions(actions: &[serde_json::Value], action_type: &str) -> usize {
    actions
        .iter()
        .filter(|a| a.get(action_type).is_some())
        .count()
}

pub fn find_action<'a>(
    actions: &'a [serde_json::Value],
    action_type: &str,
) -> Option<&'a serde_json::Value> {
    actions.iter().find_map(|a| a.get(action_type))
}

/// Interop check: read a Delta table with the DuckDB CLI.
pub fn duckdb_csv(sql: &str) -> String {
    let output = Command::new("duckdb")
        .args(["-noheader", "-csv", "-c", sql])
        .output()
        .expect("duckdb must be on PATH");
    assert!(
        output.status.success(),
        "duckdb failed for {sql}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

pub fn duckdb_count(table_path: &str) -> String {
    duckdb_csv(&format!("SELECT count(*) FROM delta_scan('{table_path}')"))
}

pub fn duckdb_summary(table_path: &str) -> String {
    duckdb_csv(&format!(
        "SELECT value - id, count(*), min(id), max(id) \
         FROM delta_scan('{table_path}') GROUP BY 1 ORDER BY 1"
    ))
}
