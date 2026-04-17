use std::ops::Range;
use std::process::Command;
use std::sync::Arc;

use arrow_array::builder::Int64Builder;
use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::stream;
use ldrs::ldrs_delta::{merge_delta, overwrite_delta, MergeConfig, MergeStats, TxnConfig};

// 2026-01-01T00:00:00Z in microseconds
const TARGET_BASE_TS: i64 = 1_767_225_600_000_000;
// 2026-04-12T00:00:00Z in microseconds
const SOURCE_BASE_TS: i64 = 1_775_952_000_000_000;
// 1 minute in microseconds
const TS_STEP: i64 = 60_000_000;

fn test_schema() -> SchemaRef {
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

fn make_batch(id_range: Range<i64>, value_offset: i64, base_ts: i64) -> RecordBatch {
    let ids: Vec<i64> = id_range.collect();
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

fn make_target_batch(id_range: Range<i64>) -> RecordBatch {
    make_batch(id_range, 0, TARGET_BASE_TS)
}

fn make_source_batch(id_range: Range<i64>) -> RecordBatch {
    make_batch(id_range, 10_000, SOURCE_BASE_TS)
}

fn test_table_path(name: &str) -> String {
    let cd = std::env::current_dir().unwrap();
    format!(
        "{}/tests/test_data/delta_writes/merge_{}",
        cd.display(),
        name
    )
}

fn cleanup_table(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn read_log_actions(table_path: &str, version: u64) -> Vec<serde_json::Value> {
    let log_path = format!(
        "{}/_delta_log/{:020}.json",
        table_path, version
    );
    let content = std::fs::read_to_string(&log_path)
        .unwrap_or_else(|_| panic!("Failed to read log version {}", version));
    content
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn count_actions(actions: &[serde_json::Value], action_type: &str) -> usize {
    actions.iter().filter(|a| a.get(action_type).is_some()).count()
}

fn find_action<'a>(actions: &'a [serde_json::Value], action_type: &str) -> Option<&'a serde_json::Value> {
    actions.iter().find_map(|a| a.get(action_type))
}

// Interop check: read the Delta table with the DuckDB CLI and assert the row count.
// Skipped with a warning if `duckdb` is not on PATH so local `cargo test` still passes
// for devs without DuckDB installed. CI installs it.
fn verify_duckdb_count(table_path: &str, expected: i64) {
    let sql = format!("SELECT count(*) FROM delta_scan('{}')", table_path);
    let output = match Command::new("duckdb")
        .args(["-noheader", "-csv", "-c", &sql])
        .output()
    {
        Ok(o) => o,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("SKIP: duckdb CLI not on PATH; interop check skipped for {table_path}");
            return;
        }
        Err(e) => panic!("failed to invoke duckdb: {e}"),
    };
    assert!(
        output.status.success(),
        "duckdb failed for {table_path}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let actual: i64 = stdout
        .trim()
        .parse()
        .unwrap_or_else(|_| panic!("could not parse duckdb output: {stdout:?}"));
    assert_eq!(actual, expected, "duckdb row count mismatch for {table_path}");
}

#[tokio::test]
#[test_log::test]
async fn test_merge_basic_int_key() {
    let table_path = test_table_path("basic_int_key");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Write target: ids 1..=1000 via overwrite
    let target = make_target_batch(1..1001);
    let target_stream = stream::iter(vec![Ok(target)]);
    overwrite_delta(&table_url, schema.clone(), vec![], target_stream, None, None).await.unwrap();

    // Merge source: ids 501..=1500
    // 500 updates (501..=1000), 500 inserts (1001..=1500)
    let source = make_source_batch(501..1501);
    let source_stream = stream::iter(vec![Ok(source)]);
    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
    };

    let stats = merge_delta(&table_url, schema.clone(), vec![], source_stream, config)
        .await
        .unwrap();

    // Verify MergeStats
    assert_eq!(stats.source_rows, 1000, "source should have 1000 rows");
    assert_eq!(stats.matched_rows, 500, "500 rows should match (ids 501..=1000)");
    assert_eq!(stats.inserted_rows, 500, "500 rows should be inserts (ids 1001..=1500)");
    assert!(stats.files_with_dvs > 0, "should have files with DVs");
    assert!(stats.files_written > 0, "should have written source files");
    assert!(!stats.skipped, "should not be skipped");

    // Verify log version 2 (merge commit)
    let actions = read_log_actions(&table_path, 2);

    // Commit info
    let commit_info = find_action(&actions, "commitInfo").expect("should have commitInfo");
    assert_eq!(commit_info["operation"], "MERGE");

    // Protocol upgrade — first merge should add deletionVectors
    let protocol = find_action(&actions, "protocol").expect("first merge should upgrade protocol");
    let reader_features = protocol["readerFeatures"].as_array().unwrap();
    assert!(
        reader_features.iter().any(|f| f == "deletionVectors"),
        "protocol should include deletionVectors reader feature"
    );

    // Metadata with DV config
    let metadata = find_action(&actions, "metaData").expect("first merge should include metadata");
    assert_eq!(
        metadata["configuration"]["delta.enableDeletionVectors"], "true",
        "metadata should enable deletion vectors"
    );

    // Remove actions — matched target files get removed
    let remove_count = count_actions(&actions, "remove");
    assert!(remove_count > 0, "should have remove actions for matched files");

    // Add actions — should have both DV re-adds and new source file adds
    let add_actions: Vec<&serde_json::Value> = actions
        .iter()
        .filter_map(|a| a.get("add"))
        .collect();
    assert!(add_actions.len() > 0, "should have add actions");

    // DV adds: same number as removes (re-adding matched files with DVs)
    let dv_adds: Vec<&&serde_json::Value> = add_actions
        .iter()
        .filter(|a| a.get("deletionVector").is_some())
        .collect();
    assert_eq!(
        dv_adds.len(), remove_count,
        "each removed file should be re-added with a DV"
    );

    // Each DV add should have stats with numRecords
    for dv_add in &dv_adds {
        let stats_str = dv_add["stats"].as_str().expect("DV add should have stats");
        let stats: serde_json::Value = serde_json::from_str(stats_str).unwrap();
        assert!(
            stats["numRecords"].as_i64().unwrap() > 0,
            "DV add stats should have numRecords"
        );
    }

    // New source file adds: no DV
    let new_adds: Vec<&&serde_json::Value> = add_actions
        .iter()
        .filter(|a| a.get("deletionVector").is_none())
        .collect();
    assert!(new_adds.len() > 0, "should have new source file adds");

    // Interop: after merge the logical table is ids 1..=1500
    verify_duckdb_count(&table_path, 1500);
}

#[tokio::test]
#[test_log::test]
async fn test_merge_empty_table() {
    let table_path = test_table_path("empty_table");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // No overwrite — merge_delta's ensure_table creates v0, merge commits v1
    let source = make_source_batch(1..501);
    let source_stream = stream::iter(vec![Ok(source)]);
    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
    };

    let stats = merge_delta(&table_url, schema.clone(), vec![], source_stream, config)
        .await
        .unwrap();

    // Pure insert: no matches, no DVs
    assert_eq!(stats.source_rows, 500, "500 source rows");
    assert_eq!(stats.matched_rows, 0, "no matches in empty table");
    assert_eq!(stats.inserted_rows, 500, "all source rows are inserts");
    assert_eq!(stats.files_with_dvs, 0, "no DVs on pure insert");
    assert!(stats.files_written > 0, "should have written source files");
    assert!(!stats.skipped);

    // Merge commit is at v1 (v0 is ensure_table's create)
    let actions = read_log_actions(&table_path, 1);

    let commit_info = find_action(&actions, "commitInfo").expect("should have commitInfo");
    assert_eq!(commit_info["operation"], "MERGE");

    // No removes
    assert_eq!(
        count_actions(&actions, "remove"), 0,
        "no remove actions on empty table merge"
    );

    // Add actions: source files only, no DVs
    let add_actions: Vec<&serde_json::Value> = actions
        .iter()
        .filter_map(|a| a.get("add"))
        .collect();
    assert!(add_actions.len() > 0, "should have add actions");
    for add in &add_actions {
        assert!(
            add.get("deletionVector").is_none(),
            "no DVs should be present on pure insert"
        );
    }

    // Protocol upgrade happens on first merge even with no matches
    let protocol = find_action(&actions, "protocol")
        .expect("first merge should upgrade protocol");
    let reader_features = protocol["readerFeatures"].as_array().unwrap();
    assert!(
        reader_features.iter().any(|f| f == "deletionVectors"),
        "protocol should include deletionVectors feature"
    );
}

#[tokio::test]
#[test_log::test]
async fn test_merge_all_matches() {
    let table_path = test_table_path("all_matches");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target: ids 1..=1000
    let target = make_target_batch(1..1001);
    let target_stream = stream::iter(vec![Ok(target)]);
    overwrite_delta(&table_url, schema.clone(), vec![], target_stream, None, None).await.unwrap();

    // Source: same ids 1..=1000 — all updates, no inserts
    let source = make_source_batch(1..1001);
    let source_stream = stream::iter(vec![Ok(source)]);
    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
    };

    let stats = merge_delta(&table_url, schema.clone(), vec![], source_stream, config)
        .await
        .unwrap();

    assert_eq!(stats.source_rows, 1000);
    assert_eq!(stats.matched_rows, 1000, "every source row should match");
    assert_eq!(stats.inserted_rows, 0, "no inserts when all match");
    assert!(stats.files_with_dvs > 0, "matched files should have DVs");

    // Merge commit at v2 (v0=create, v1=overwrite, v2=merge)
    let actions = read_log_actions(&table_path, 2);

    let commit_info = find_action(&actions, "commitInfo").expect("should have commitInfo");
    assert_eq!(commit_info["operation"], "MERGE");

    let remove_count = count_actions(&actions, "remove");
    assert!(remove_count > 0, "should have remove actions for matched files");

    let add_actions: Vec<&serde_json::Value> = actions
        .iter()
        .filter_map(|a| a.get("add"))
        .collect();
    let dv_adds: Vec<&&serde_json::Value> = add_actions
        .iter()
        .filter(|a| a.get("deletionVector").is_some())
        .collect();
    assert_eq!(
        dv_adds.len(), remove_count,
        "each removed file should be re-added with a DV"
    );

    // DV cardinality should cover all 1000 target rows
    let total_dv_cardinality: i64 = dv_adds
        .iter()
        .map(|a| a["deletionVector"]["cardinality"].as_i64().unwrap())
        .sum();
    assert_eq!(
        total_dv_cardinality, 1000,
        "DVs should cover all 1000 target rows"
    );
}

#[tokio::test]
#[test_log::test]
async fn test_merge_with_existing_dvs() {
    let table_path = test_table_path("existing_dvs");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target: ids 1..=1000
    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(target)]),
        None,
        None,
    )
    .await
    .unwrap();

    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
    };

    // First merge: update ids 1..=500 (creates DVs covering rows 1-500)
    let first_source = make_source_batch(1..501);
    let stats1 = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(first_source)]),
        config.clone(),
    )
    .await
    .unwrap();
    assert_eq!(stats1.matched_rows, 500);
    assert_eq!(stats1.inserted_rows, 0);

    // Second merge: update ids 501..=1000 on the same target file
    // Existing DV (rows 1-500) should be unioned with new matches (rows 501-1000)
    let second_source = make_source_batch(501..1001);
    let stats2 = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(second_source)]),
        config.clone(),
    )
    .await
    .unwrap();
    assert_eq!(stats2.matched_rows, 500, "second merge should match 500 new rows");
    assert_eq!(stats2.inserted_rows, 0);

    // Second merge commit at v3 (v0=create, v1=overwrite, v2=first merge, v3=second merge)
    let actions = read_log_actions(&table_path, 3);

    // DV cardinality after second merge = 1000 (union of first 500 + second 500)
    let dv_adds: Vec<&serde_json::Value> = actions
        .iter()
        .filter_map(|a| a.get("add"))
        .filter(|a| a.get("deletionVector").is_some())
        .collect();
    let total_dv_cardinality: i64 = dv_adds
        .iter()
        .map(|a| a["deletionVector"]["cardinality"].as_i64().unwrap())
        .sum();
    assert_eq!(
        total_dv_cardinality, 1000,
        "DVs after second merge should cover all 1000 rows (union of both merges)"
    );

    // Protocol upgrade should NOT happen on second merge (already enabled)
    assert!(
        find_action(&actions, "protocol").is_none(),
        "second merge should not include protocol action"
    );
    assert!(
        find_action(&actions, "metaData").is_none(),
        "second merge should not include metadata action"
    );

    // Interop: both merges were pure updates — logical table is still ids 1..=1000
    verify_duckdb_count(&table_path, 1000);
}

#[tokio::test]
#[test_log::test]
async fn test_merge_string_key() {
    let table_path = test_table_path("string_key");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target: ids 1..=1000 → names "row-000001".."row-001000"
    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(target)]),
        None,
        None,
    )
    .await
    .unwrap();

    // Source: ids 501..=1500 → names "row-000501".."row-001500"
    // 500 name matches, 500 new names
    let source = make_source_batch(501..1501);
    let config = MergeConfig {
        merge_keys: vec!["name".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
    };

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(source)]),
        config,
    )
    .await
    .unwrap();

    assert_eq!(stats.source_rows, 1000);
    assert_eq!(stats.matched_rows, 500, "names row-000501..row-001000 should match");
    assert_eq!(stats.inserted_rows, 500);
    assert!(stats.files_with_dvs > 0);

    let actions = read_log_actions(&table_path, 2);
    let commit_info = find_action(&actions, "commitInfo").expect("should have commitInfo");
    assert_eq!(commit_info["operation"], "MERGE");

    let dv_adds: Vec<&serde_json::Value> = actions
        .iter()
        .filter_map(|a| a.get("add"))
        .filter(|a| a.get("deletionVector").is_some())
        .collect();
    let total_dv_cardinality: i64 = dv_adds
        .iter()
        .map(|a| a["deletionVector"]["cardinality"].as_i64().unwrap())
        .sum();
    assert_eq!(total_dv_cardinality, 500, "500 string keys should match");
}

#[tokio::test]
#[test_log::test]
async fn test_merge_timestamp_key() {
    let table_path = test_table_path("timestamp_key");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Both batches use TARGET_BASE_TS so timestamps align for shared id ranges.
    // Target ids 1..=1000    → timestamps T+60   .. T+60000 (1000 unique)
    // Source ids 501..=1500  → timestamps T+30060 .. T+90000 (1000 unique)
    // Overlap: timestamps T+30060 .. T+60000 = 500 shared (from ids 501..=1000)
    // value_offset differentiates target (0) vs source (10000) rows.
    let target = make_batch(1..1001, 0, TARGET_BASE_TS);
    overwrite_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(target)]),
        None,
        None,
    )
    .await
    .unwrap();

    let source = make_batch(501..1501, 10_000, TARGET_BASE_TS);
    let config = MergeConfig {
        merge_keys: vec!["updated_at".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
    };

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(source)]),
        config,
    )
    .await
    .unwrap();

    assert_eq!(stats.source_rows, 1000);
    assert_eq!(stats.matched_rows, 500, "500 timestamps should match");
    assert_eq!(stats.inserted_rows, 500);
    assert!(stats.files_with_dvs > 0);

    let actions = read_log_actions(&table_path, 2);
    let commit_info = find_action(&actions, "commitInfo").expect("should have commitInfo");
    assert_eq!(commit_info["operation"], "MERGE");

    let dv_adds: Vec<&serde_json::Value> = actions
        .iter()
        .filter_map(|a| a.get("add"))
        .filter(|a| a.get("deletionVector").is_some())
        .collect();
    let total_dv_cardinality: i64 = dv_adds
        .iter()
        .map(|a| a["deletionVector"]["cardinality"].as_i64().unwrap())
        .sum();
    assert_eq!(total_dv_cardinality, 500, "500 timestamp keys should match");
}

#[tokio::test]
#[test_log::test]
async fn test_merge_composite_key() {
    let table_path = test_table_path("composite_key");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target: ids 1..=1000 with deterministic names "row-000001".."row-001000"
    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(target)]),
        None,
        None,
    )
    .await
    .unwrap();

    // Source: ids 501..=1500 with names "row-000501".."row-001500"
    // Composite (id, name) pairs align: 501..=1000 match, 1001..=1500 new
    let source = make_source_batch(501..1501);
    let config = MergeConfig {
        merge_keys: vec!["id".to_string(), "name".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
    };

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(source)]),
        config,
    )
    .await
    .unwrap();

    assert_eq!(stats.source_rows, 1000);
    assert_eq!(stats.matched_rows, 500, "500 (id, name) pairs should match");
    assert_eq!(stats.inserted_rows, 500);
    assert!(stats.files_with_dvs > 0);

    let actions = read_log_actions(&table_path, 2);
    let commit_info = find_action(&actions, "commitInfo").expect("should have commitInfo");
    assert_eq!(commit_info["operation"], "MERGE");

    let dv_adds: Vec<&serde_json::Value> = actions
        .iter()
        .filter_map(|a| a.get("add"))
        .filter(|a| a.get("deletionVector").is_some())
        .collect();
    let total_dv_cardinality: i64 = dv_adds
        .iter()
        .map(|a| a["deletionVector"]["cardinality"].as_i64().unwrap())
        .sum();
    assert_eq!(total_dv_cardinality, 500, "500 composite keys should match");
}

#[tokio::test]
#[test_log::test]
async fn test_merge_txn_watermark_skip() {
    let table_path = test_table_path("txn_watermark");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target table
    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(target)]),
        None,
        None,
    )
    .await
    .unwrap();

    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::SourceWatermark {
            app_id: "ldrs-merge-test".to_string(),
            watermark_column: "updated_at".to_string(),
        },
    };

    // First merge should commit
    let source = make_source_batch(501..1501);
    let stats1 = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(source)]),
        config.clone(),
    )
    .await
    .unwrap();
    assert!(!stats1.skipped, "first merge should commit");
    assert_eq!(stats1.matched_rows, 500);

    // Verify v2 has txn action
    let v2_actions = read_log_actions(&table_path, 2);
    let txn = find_action(&v2_actions, "txn").expect("merge should include txn action");
    assert_eq!(txn["appId"], "ldrs-merge-test");
    let committed_version = txn["version"].as_i64().unwrap();

    // Second merge with same source → should skip (same watermark)
    let same_source = make_source_batch(501..1501);
    let stats2 = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(same_source)]),
        config.clone(),
    )
    .await
    .unwrap();
    assert!(stats2.skipped, "second merge with same watermark should skip");
    assert_eq!(
        stats2.skipped_version,
        Some(committed_version),
        "should report the existing txn version"
    );

    // No v3 commit
    let v3_path = format!("{}/_delta_log/00000000000000000003.json", table_path);
    assert!(
        !std::path::Path::new(&v3_path).exists(),
        "skipped merge should not produce v3"
    );
}

#[tokio::test]
#[test_log::test]
async fn test_merge_txn_processing_time_skip() {
    let table_path = test_table_path("txn_processing_time");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target table
    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(target)]),
        None,
        None,
    )
    .await
    .unwrap();

    // Fixed batch_version simulates an orchestrator retrying with the same version
    let batch_version = 1234567890_i64;
    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::ProcessingTime {
            app_id: "ldrs-merge-test".to_string(),
            batch_version: Some(batch_version),
        },
    };

    // First merge: commits
    let source = make_source_batch(501..1501);
    let stats1 = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(source)]),
        config.clone(),
    )
    .await
    .unwrap();
    assert!(!stats1.skipped, "first merge should commit");
    assert_eq!(stats1.matched_rows, 500);

    let v2_actions = read_log_actions(&table_path, 2);
    let txn = find_action(&v2_actions, "txn").expect("should have txn action");
    assert_eq!(txn["appId"], "ldrs-merge-test");
    assert_eq!(
        txn["version"].as_i64(),
        Some(batch_version),
        "txn should record the explicit batch_version"
    );

    // Second merge with same batch_version → skip (retry)
    let same_source = make_source_batch(501..1501);
    let stats2 = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(same_source)]),
        config.clone(),
    )
    .await
    .unwrap();
    assert!(stats2.skipped, "retry with same batch_version should skip");
    assert_eq!(stats2.skipped_version, Some(batch_version));

    // Third merge with NEWER batch_version → commits
    let newer_config = MergeConfig {
        txn_config: TxnConfig::ProcessingTime {
            app_id: "ldrs-merge-test".to_string(),
            batch_version: Some(batch_version + 1),
        },
        ..config.clone()
    };
    let different_source = make_source_batch(1001..2001);
    let stats3 = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(different_source)]),
        newer_config,
    )
    .await
    .unwrap();
    assert!(!stats3.skipped, "newer batch_version should proceed");

    let v3_path = format!("{}/_delta_log/00000000000000000003.json", table_path);
    assert!(
        std::path::Path::new(&v3_path).exists(),
        "newer version should commit at v3"
    );
}

fn list_parquets(table_path: &str) -> std::collections::HashSet<std::ffi::OsString> {
    std::fs::read_dir(table_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
        .map(|e| e.file_name())
        .collect()
}

#[tokio::test]
#[test_log::test]
async fn test_merge_null_keys_rejected_and_cleaned_up() {
    let table_path = test_table_path("null_keys_cleanup");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Establish a known set of parquets via overwrite
    let target = make_target_batch(1..101);
    overwrite_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(target)]),
        None,
        None,
    )
    .await
    .unwrap();

    let parquets_before = list_parquets(&table_path);

    // Source batch with a null in the `id` merge key column
    let mut ids = Int64Builder::new();
    ids.append_value(1);
    ids.append_null();
    ids.append_value(3);
    let source = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(ids.finish()),
            Arc::new(Int64Array::from(vec![10001, 10002, 10003])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(
                TimestampMicrosecondArray::from(vec![
                    SOURCE_BASE_TS,
                    SOURCE_BASE_TS + TS_STEP,
                    SOURCE_BASE_TS + 2 * TS_STEP,
                ])
                .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();

    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
    };

    let result = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(source)]),
        config,
    )
    .await;

    assert!(result.is_err(), "merge with null keys must fail");

    // No v2 commit — the failed merge must not write a log entry
    let v2_path = format!("{}/_delta_log/00000000000000000002.json", table_path);
    assert!(
        !std::path::Path::new(&v2_path).exists(),
        "failed merge must not commit"
    );

    // No orphaned source parquets — the failed validation path must clean up
    let parquets_after = list_parquets(&table_path);
    assert_eq!(
        parquets_before, parquets_after,
        "failed merge must clean up its source parquets"
    );
}

#[tokio::test]
#[test_log::test]
async fn test_merge_small_change_uses_inline_dv() {
    let table_path = test_table_path("inline_dv");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target: 1000 rows
    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(target)]),
        None,
        None,
    )
    .await
    .unwrap();

    // Source: 10 updates — DV bitmap serializes well under the 1024-byte inline threshold
    let source = make_source_batch(100..110);
    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
    };

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(source)]),
        config,
    )
    .await
    .unwrap();

    assert_eq!(stats.source_rows, 10);
    assert_eq!(stats.matched_rows, 10);
    assert_eq!(stats.inserted_rows, 0);
    assert_eq!(stats.files_with_dvs, 1, "one target file should get a DV");

    // Verify the commit records an inline DV (storageType "i"), not a file-based one
    let actions = read_log_actions(&table_path, 2);
    let dv_adds: Vec<&serde_json::Value> = actions
        .iter()
        .filter_map(|a| a.get("add"))
        .filter(|a| a.get("deletionVector").is_some())
        .collect();
    assert_eq!(dv_adds.len(), 1, "exactly one add should carry a DV");

    let dv = &dv_adds[0]["deletionVector"];
    assert_eq!(
        dv["storageType"].as_str(),
        Some("i"),
        "small DV should be stored inline; got: {dv}"
    );
    assert!(
        dv.get("offset").is_none_or(|v| v.is_null()),
        "inline DV should have no offset; got: {dv}"
    );
    assert_eq!(dv["cardinality"].as_i64(), Some(10));
    assert!(
        dv["pathOrInlineDv"].as_str().map(str::len).unwrap_or(0) > 0,
        "inline DV should carry encoded bitmap data"
    );

    // No DV file on disk — inline path must not produce a deletion_vector_*.bin file
    let dv_files: Vec<_> = std::fs::read_dir(&table_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("deletion_vector_")
        })
        .collect();
    assert!(
        dv_files.is_empty(),
        "inline DV must not produce a DV file; found: {:?}",
        dv_files.iter().map(|e| e.file_name()).collect::<Vec<_>>()
    );

    // A follow-up merge must be able to read the inline DV via delta-kernel.
    // Changing different keys (200..210) should produce a new DV that unions
    // with the existing inline one — proving delta-kernel decoded our inline bytes.
    let second_source = make_source_batch(200..210);
    let second_config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
    };
    let stats2 = merge_delta(
        &table_url,
        schema.clone(),
        vec![],
        stream::iter(vec![Ok(second_source)]),
        second_config,
    )
    .await
    .unwrap();
    assert_eq!(stats2.matched_rows, 10, "second merge matches 10 new rows");

    let v3_actions = read_log_actions(&table_path, 3);
    let v3_dv_adds: Vec<&serde_json::Value> = v3_actions
        .iter()
        .filter_map(|a| a.get("add"))
        .filter(|a| a.get("deletionVector").is_some())
        .collect();
    let v3_cardinality: i64 = v3_dv_adds
        .iter()
        .map(|a| a["deletionVector"]["cardinality"].as_i64().unwrap())
        .sum();
    assert_eq!(
        v3_cardinality, 20,
        "second merge's DV should union with the inline DV from v2 (10 + 10 = 20)"
    );

    // Interop: both merges were pure updates — logical table is still ids 1..=1000.
    // This also proves DuckDB can decode our inline DV bytes (storageType "i").
    verify_duckdb_count(&table_path, 1000);
}
