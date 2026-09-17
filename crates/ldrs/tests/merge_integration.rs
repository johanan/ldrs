use std::ops::Range;
use std::sync::Arc;

use arrow_array::builder::Int64Builder;
use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use futures::stream;
use ldrs_delta::{merge_delta, overwrite_delta, MergeConfig, OperationConfig, TxnConfig};
use ldrs_test_fixtures::delta::{
    cleanup_table, count_actions, delta_table_path, duckdb_count, find_action, latest_version,
    make_batch, make_batch_from_ids, make_source_batch, make_target_batch, read_log_actions,
    test_schema, SOURCE_BASE_TS, TARGET_BASE_TS, TS_STEP,
};

fn test_table_path(name: &str) -> String {
    delta_table_path(&format!("merge_{name}"))
}

// A deletion vector larger than the 1024-byte inline threshold is stored as a file
// (`storageType: "u"`).
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_file_based_dv_round_trip() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("file_based_dv");
    cleanup_table(&table_path);

    let schema = test_schema();
    // No trailing slash
    let table_url = format!("file://{}", table_path);

    // Target: ids 1..=2000 in a single file.
    let target = make_target_batch(1..2001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(target)]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

    let config = || MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
        inline_deletion_vectors: false,
    };

    // Merge 1: 700 scattered matches
    let odd_ids: Vec<i64> = (1..=1399).step_by(2).collect();
    let m1 = odd_ids.len() as i64;
    let source1 = make_batch_from_ids(odd_ids, 10_000, SOURCE_BASE_TS);
    let stats1 = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(source1)]),
        config(),
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();
    assert_eq!(stats1.matched_rows as i64, m1);

    let actions1 = read_log_actions(&table_path, latest_version(&table_path));
    let dv1 = actions1
        .iter()
        .filter_map(|a| a.get("add"))
        .find_map(|add| add.get("deletionVector"))
        .expect("merge 1 should write a deletion vector");
    assert_eq!(
        dv1["storageType"], "u",
        "DV must be file-based (>1024 B) to exercise the read path"
    );

    // Merge 2
    let even_ids: Vec<i64> = (2..=400).step_by(2).collect();
    let m2 = even_ids.len() as i64;
    let source2 = make_batch_from_ids(even_ids, 20_000, SOURCE_BASE_TS);
    let stats2 = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(source2)]),
        config(),
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .expect("merge 2 must read the existing file-based DV, not fail on a dropped path segment");
    assert_eq!(stats2.matched_rows as i64, m2);

    // The re-matched file's DV is now the union of both merges.
    let actions2 = read_log_actions(&table_path, latest_version(&table_path));
    let unioned = actions2
        .iter()
        .filter_map(|a| a.get("add"))
        .filter_map(|add| add.get("deletionVector"))
        .map(|dv| dv["cardinality"].as_i64().unwrap())
        .max()
        .expect("merge 2 should write a deletion vector");
    assert_eq!(unioned, m1 + m2, "DV should be the union of both merges");

    // Updates delete + reinsert, so the logical count is unchanged.
    assert_eq!(duckdb_count(&table_path), "2000");
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_basic_int_key() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("basic_int_key");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Write target: ids 1..=1000 via overwrite
    let target = make_target_batch(1..1001);
    let target_stream = stream::iter(vec![Ok(target)]);
    overwrite_delta(
        &table_url,
        schema.clone(),
        target_stream,
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

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
        inline_deletion_vectors: false,
    };

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        source_stream,
        config,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

    // Verify MergeStats
    assert_eq!(stats.source_rows, 1000, "source should have 1000 rows");
    assert_eq!(
        stats.matched_rows, 500,
        "500 rows should match (ids 501..=1000)"
    );
    assert_eq!(
        stats.inserted_rows, 500,
        "500 rows should be inserts (ids 1001..=1500)"
    );
    assert!(stats.files_with_dvs > 0, "should have files with DVs");
    assert!(stats.files_written > 0, "should have written source files");
    assert!(!stats.skipped, "should not be skipped");

    // Verify log version 2 (merge commit)
    let actions = read_log_actions(&table_path, 2);

    // Commit info
    let commit_info = find_action(&actions, "commitInfo").expect("should have commitInfo");
    assert_eq!(commit_info["operation"], "MERGE");

    // Protocol upgrade first merge should add deletionVectors
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

    // Remove actions matched target files get removed
    let remove_count = count_actions(&actions, "remove");
    assert!(
        remove_count > 0,
        "should have remove actions for matched files"
    );

    // Add actions should have both DV re-adds and new source file adds
    let add_actions: Vec<&serde_json::Value> =
        actions.iter().filter_map(|a| a.get("add")).collect();
    assert!(add_actions.len() > 0, "should have add actions");

    // DV adds: same number as removes (re-adding matched files with DVs)
    let dv_adds: Vec<&&serde_json::Value> = add_actions
        .iter()
        .filter(|a| a.get("deletionVector").is_some())
        .collect();
    assert_eq!(
        dv_adds.len(),
        remove_count,
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
    assert_eq!(duckdb_count(&table_path), "1500");
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_empty_table() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("empty_table");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // No overwrite merge_delta's ensure_table creates v0, merge commits v1
    let source = make_source_batch(1..501);
    let source_stream = stream::iter(vec![Ok(source)]);
    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
        inline_deletion_vectors: false,
    };

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        source_stream,
        config,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
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
        count_actions(&actions, "remove"),
        0,
        "no remove actions on empty table merge"
    );

    // Add actions: source files only, no DVs
    let add_actions: Vec<&serde_json::Value> =
        actions.iter().filter_map(|a| a.get("add")).collect();
    assert!(add_actions.len() > 0, "should have add actions");
    for add in &add_actions {
        assert!(
            add.get("deletionVector").is_none(),
            "no DVs should be present on pure insert"
        );
    }

    // Protocol upgrade happens on first merge even with no matches
    let protocol = find_action(&actions, "protocol").expect("first merge should upgrade protocol");
    let reader_features = protocol["readerFeatures"].as_array().unwrap();
    assert!(
        reader_features.iter().any(|f| f == "deletionVectors"),
        "protocol should include deletionVectors feature"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_all_matches() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("all_matches");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target: ids 1..=1000
    let target = make_target_batch(1..1001);
    let target_stream = stream::iter(vec![Ok(target)]);
    overwrite_delta(
        &table_url,
        schema.clone(),
        target_stream,
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

    // Source: same ids 1..=1000 all updates, no inserts
    let source = make_source_batch(1..1001);
    let source_stream = stream::iter(vec![Ok(source)]);
    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
        inline_deletion_vectors: false,
    };

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        source_stream,
        config,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
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
    assert!(
        remove_count > 0,
        "should have remove actions for matched files"
    );

    let add_actions: Vec<&serde_json::Value> =
        actions.iter().filter_map(|a| a.get("add")).collect();
    let dv_adds: Vec<&&serde_json::Value> = add_actions
        .iter()
        .filter(|a| a.get("deletionVector").is_some())
        .collect();
    assert_eq!(
        dv_adds.len(),
        remove_count,
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

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_with_existing_dvs() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("existing_dvs");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target: ids 1..=1000
    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(target)]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
        inline_deletion_vectors: false,
    };

    // First merge: update ids 1..=500 (creates DVs covering rows 1-500)
    let first_source = make_source_batch(1..501);
    let stats1 = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(first_source)]),
        config.clone(),
        &OperationConfig::new("ldrs-test"),
        &rt,
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
        stream::iter(vec![Ok(second_source)]),
        config.clone(),
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();
    assert_eq!(
        stats2.matched_rows, 500,
        "second merge should match 500 new rows"
    );
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

    // Interop: both merges were pure updates logical table is still ids 1..=1000
    assert_eq!(duckdb_count(&table_path), "1000");
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_string_key() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("string_key");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target: ids 1..=1000 → names "row-000001".."row-001000"
    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(target)]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
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
        inline_deletion_vectors: false,
    };

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(source)]),
        config,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

    assert_eq!(stats.source_rows, 1000);
    assert_eq!(
        stats.matched_rows, 500,
        "names row-000501..row-001000 should match"
    );
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

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_timestamp_key() {
    let rt = tokio::runtime::Handle::current();
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
        stream::iter(vec![Ok(target)]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
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
        inline_deletion_vectors: false,
    };

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(source)]),
        config,
        &OperationConfig::new("ldrs-test"),
        &rt,
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

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_composite_key() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("composite_key");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target: ids 1..=1000 with deterministic names "row-000001".."row-001000"
    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(target)]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
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
        inline_deletion_vectors: false,
    };

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(source)]),
        config,
        &OperationConfig::new("ldrs-test"),
        &rt,
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

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_txn_watermark_skip() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("txn_watermark");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target table
    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(target)]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
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
        inline_deletion_vectors: false,
    };

    // First merge should commit
    let source = make_source_batch(501..1501);
    let stats1 = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(source)]),
        config.clone(),
        &OperationConfig::new("ldrs-test"),
        &rt,
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
        stream::iter(vec![Ok(same_source)]),
        config.clone(),
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();
    assert!(
        stats2.skipped,
        "second merge with same watermark should skip"
    );
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

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_txn_processing_time_skip() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("txn_processing_time");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target table
    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(target)]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
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
        inline_deletion_vectors: false,
    };

    // First merge: commits
    let source = make_source_batch(501..1501);
    let stats1 = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(source)]),
        config.clone(),
        &OperationConfig::new("ldrs-test"),
        &rt,
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
        stream::iter(vec![Ok(same_source)]),
        config.clone(),
        &OperationConfig::new("ldrs-test"),
        &rt,
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
        stream::iter(vec![Ok(different_source)]),
        newer_config,
        &OperationConfig::new("ldrs-test"),
        &rt,
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

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_null_keys_rejected_and_cleaned_up() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("null_keys_cleanup");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Establish a known set of parquets via overwrite
    let target = make_target_batch(1..101);
    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(target)]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
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
        inline_deletion_vectors: false,
    };

    let result = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(source)]),
        config,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await;

    assert!(result.is_err(), "merge with null keys must fail");

    // No v2 commit the failed merge must not write a log entry
    let v2_path = format!("{}/_delta_log/00000000000000000002.json", table_path);
    assert!(
        !std::path::Path::new(&v2_path).exists(),
        "failed merge must not commit"
    );

    // No orphaned source parquets the failed validation path must clean up
    let parquets_after = list_parquets(&table_path);
    assert_eq!(
        parquets_before, parquets_after,
        "failed merge must clean up its source parquets"
    );
}

/// DV goes to a sidecar unless asked for inline.
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_a_small_dv_goes_to_a_file_by_default() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("file_dv_default");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(target)]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

    let source = make_source_batch(100..110);
    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
        inline_deletion_vectors: false,
    };

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(source)]),
        config,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();
    assert_eq!(stats.files_with_dvs, 1);

    let actions = read_log_actions(&table_path, 2);
    let dv = actions
        .iter()
        .filter_map(|a| a.get("add"))
        .find_map(|a| a.get("deletionVector"))
        .expect("an add should carry a DV");
    assert_eq!(
        dv["storageType"].as_str(),
        Some("u"),
        "a small DV should still go to a file; got: {dv}"
    );
    assert_eq!(dv["cardinality"].as_i64(), Some(10));

    let dv_files: Vec<_> = std::fs::read_dir(&table_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("deletion_vector_")
        })
        .collect();
    assert_eq!(dv_files.len(), 1, "expected one sidecar DV file");
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_inline_option_stores_a_small_dv_in_the_commit() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("inline_dv");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target: 1000 rows
    let target = make_target_batch(1..1001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(target)]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

    // Source: 10 updates DV bitmap serializes well under the 1024-byte inline threshold
    let source = make_source_batch(100..110);
    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
        inline_deletion_vectors: true,
    };

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(source)]),
        config,
        &OperationConfig::new("ldrs-test"),
        &rt,
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

    // No DV file on disk inline path must not produce a deletion_vector_*.bin file
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
    // with the existing inline one proving delta-kernel decoded our inline bytes.
    let second_source = make_source_batch(200..210);
    let second_config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
        inline_deletion_vectors: true,
    };
    let stats2 = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(second_source)]),
        second_config,
        &OperationConfig::new("ldrs-test"),
        &rt,
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

    // Interop: both merges were pure updates logical table is still ids 1..=1000.
    // This also proves DuckDB can decode our inline DV bytes (storageType "i").
    assert_eq!(duckdb_count(&table_path), "1000");
}

// Recovering an existing *sidecar* ('u') DV descriptor for the remove action: the first merge
// deletes enough scattered rows to spill the DV past the inline threshold into a `.bin` file, then
// the second merge touches the same file and must tombstone it with that exact sidecar descriptor
// (storageType "u", offset present).
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_recovers_existing_sidecar_dv() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("sidecar_dv");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    // Target: 2000 contiguous rows in one file.
    let target = make_target_batch(1..2001);
    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(target)]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
        inline_deletion_vectors: false,
    };

    // First merge: update every even id (1000 scattered rows). Scattered deletes defeat roaring's
    // run compression, so the serialized bitmap exceeds the 1024-byte inline threshold and lands in
    // a sidecar file (storageType "u").
    let even_ids: Vec<i64> = (1..=2000).filter(|id| id % 2 == 0).collect();
    let first_source = make_batch_from_ids(even_ids, 10_000, SOURCE_BASE_TS);
    let stats1 = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(first_source)]),
        config.clone(),
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();
    assert_eq!(stats1.matched_rows, 1000);
    assert_eq!(stats1.files_with_dvs, 1, "one target file should get a DV");

    // v2 add carries a sidecar DV; capture its descriptor path so we can assert the remove
    // references the exact same DV.
    let v2 = read_log_actions(&table_path, 2);
    let v2_dv_adds: Vec<&serde_json::Value> = v2
        .iter()
        .filter_map(|a| a.get("add"))
        .filter(|a| a.get("deletionVector").is_some())
        .collect();
    assert_eq!(v2_dv_adds.len(), 1, "exactly one add should carry a DV");
    let first_dv = &v2_dv_adds[0]["deletionVector"];
    assert_eq!(
        first_dv["storageType"].as_str(),
        Some("u"),
        "1000 scattered deletes should spill to a sidecar DV; got: {first_dv}"
    );
    let first_dv_path = first_dv["pathOrInlineDv"].as_str().map(str::to_string);

    // Second merge: touch three odd ids still live in the same file. This forces recovery of the
    // existing sidecar descriptor for the remove action.
    let second_source = make_batch_from_ids(vec![1, 3, 5], 10_000, SOURCE_BASE_TS);
    let stats2 = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(second_source)]),
        config.clone(),
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();
    assert_eq!(stats2.matched_rows, 3, "second merge matches the 3 odd ids");

    // v3 must tombstone the file with the exact sidecar descriptor recovered from the scan output.
    let v3 = read_log_actions(&table_path, 3);
    let dv_removes: Vec<&serde_json::Value> = v3
        .iter()
        .filter_map(|a| a.get("remove"))
        .filter(|r| r.get("deletionVector").is_some())
        .collect();
    assert_eq!(
        dv_removes.len(),
        1,
        "second merge should tombstone the one DV'd file"
    );
    let removed_dv = &dv_removes[0]["deletionVector"];
    assert_eq!(
        removed_dv["storageType"].as_str(),
        Some("u"),
        "recovered descriptor should be the sidecar DV; got: {removed_dv}"
    );
    assert_eq!(
        removed_dv["offset"].as_i64(),
        Some(1),
        "sidecar DV descriptor carries offset=1; got: {removed_dv}"
    );
    assert_eq!(
        removed_dv["pathOrInlineDv"].as_str().map(str::to_string),
        first_dv_path,
        "remove must reference the exact existing sidecar DV from v2"
    );

    // Union DV covers 1000 evens + 3 odds.
    let v3_cardinality: i64 = v3
        .iter()
        .filter_map(|a| a.get("add"))
        .filter(|a| a.get("deletionVector").is_some())
        .map(|a| a["deletionVector"]["cardinality"].as_i64().unwrap())
        .sum();
    assert_eq!(
        v3_cardinality, 1003,
        "second merge's DV should union with the sidecar DV from v2 (1000 + 3)"
    );

    // Interop: both merges were pure updates logical table is still ids 1..=2000.
    assert_eq!(duckdb_count(&table_path), "2000");
}

// A checkpoint lands once the log grows CHECKPOINT_INTERVAL (10) versions past the last one.
// What matters after that is that the table still resolves through the checkpoint parquet
// instead of the JSON commits it stands in for — for our reads and for DuckDB's.
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_the_table_property_sets_the_checkpoint_cadence() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("checkpoint_property");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    let mut table_config = OperationConfig::new("ldrs-test");
    table_config.set("delta.checkpointInterval", "100").unwrap();

    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(make_target_batch(1..101))]),
        None,
        None,
        &table_config,
        &rt,
    )
    .await
    .unwrap();

    // The same ten merges that checkpoint under the default interval.
    for i in 0..10i64 {
        let start = 101 + i * 10;
        merge_delta(
            &table_url,
            schema.clone(),
            stream::iter(vec![Ok(make_source_batch(start..start + 10))]),
            MergeConfig {
                merge_keys: vec!["id".to_string()],
                allow_null_keys: false,
                max_rows: None,
                max_bytes: None,
                txn_config: TxnConfig::None,
                inline_deletion_vectors: false,
            },
            &table_config,
            &rt,
        )
        .await
        .unwrap();
    }

    assert_eq!(latest_version(&table_path), 11);
    let checkpoint = format!(
        "{}/_delta_log/00000000000000000010.checkpoint.parquet",
        table_path
    );
    assert!(
        !std::path::Path::new(&checkpoint).exists(),
        "an interval of 100 should not checkpoint at a gap of 10"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_merge_writes_checkpoint_past_interval() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("checkpoint_interval");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    let config = || MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
        inline_deletion_vectors: false,
    };

    // v0 creates the table, v1 seeds ids 1..=100.
    let target_stream = stream::iter(vec![Ok(make_target_batch(1..101))]);
    overwrite_delta(
        &table_url,
        schema.clone(),
        target_stream,
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

    // Ten merges of disjoint ids commit v2..=v11. The tenth builds its snapshot at v10 with
    // no checkpoint behind it — a gap of exactly 10 — so it checkpoints v10 before committing.
    for i in 0..10i64 {
        let start = 101 + i * 10;
        let source_stream = stream::iter(vec![Ok(make_source_batch(start..start + 10))]);
        let stats = merge_delta(
            &table_url,
            schema.clone(),
            source_stream,
            config(),
            &OperationConfig::new("ldrs-test"),
            &rt,
        )
        .await
        .unwrap();
        assert_eq!(stats.inserted_rows, 10, "merge {i} should insert 10 rows");
    }

    assert_eq!(latest_version(&table_path), 11);

    let checkpoint = format!(
        "{}/_delta_log/00000000000000000010.checkpoint.parquet",
        table_path
    );
    assert!(
        std::path::Path::new(&checkpoint).exists(),
        "a gap of 10 versions should have written a checkpoint for v10"
    );
    let hint = format!("{}/_delta_log/_last_checkpoint", table_path);
    assert!(
        std::path::Path::new(&hint).exists(),
        "the checkpoint hint should name the checkpoint for readers"
    );
    let hint_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&hint).unwrap()).unwrap();
    assert_eq!(hint_json["version"], 10);

    // Delete every commit the checkpoint stands in for, which is what log retention will
    // eventually do. Now the only record of the seeded rows is the checkpoint parquet, so
    // what follows cannot pass by replaying JSON.
    for v in 0..=10 {
        std::fs::remove_file(format!("{}/_delta_log/{:020}.json", table_path, v)).unwrap();
    }

    // One more merge, overlapping the seeded ids. Matching them at all means the file list
    // was resolved out of the checkpoint.
    let source_stream = stream::iter(vec![Ok(make_source_batch(1..11))]);
    let stats = merge_delta(
        &table_url,
        schema.clone(),
        source_stream,
        config(),
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();
    assert_eq!(
        stats.matched_rows, 10,
        "the seeded rows are only reachable through the checkpoint"
    );
    assert_eq!(stats.inserted_rows, 0, "overlapping ids are all updates");

    // The gap is measured from the checkpoint that exists, so v11 — one past it — must not
    // produce another. A `version % interval` trigger would also pass here; a version that
    // never read `checkpoint_version` would not.
    let next = format!(
        "{}/_delta_log/00000000000000000011.checkpoint.parquet",
        table_path
    );
    assert!(
        !std::path::Path::new(&next).exists(),
        "one version past a checkpoint is not a new interval"
    );

    // 100 seeded + 100 inserted; the last merge only updated rows. An external reader has to
    // make sense of the checkpoint too — this is the half our own read path cannot vouch for.
    assert_eq!(duckdb_count(&table_path), "200");
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_overwrite_after_merge_retires_deletion_vectored_files() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("overwrite_after_merge_dv");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    let overwrite = async |ids: Range<i64>| {
        let batch = make_target_batch(ids);
        let stream = stream::iter(vec![Ok(batch)]);
        overwrite_delta(
            &table_url,
            schema.clone(),
            stream,
            None,
            None,
            &OperationConfig::new("ldrs-test"),
            &rt,
        )
        .await
        .unwrap();
    };

    overwrite(1..1001).await;

    let source = make_source_batch(501..1501);
    let config = MergeConfig {
        merge_keys: vec!["id".to_string()],
        allow_null_keys: false,
        max_rows: None,
        max_bytes: None,
        txn_config: TxnConfig::None,
        inline_deletion_vectors: false,
    };
    let stats = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(source)]),
        config,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();
    assert!(stats.files_with_dvs > 0, "merge should have written a DV");

    let dv_paths: Vec<String> = read_log_actions(&table_path, 2)
        .iter()
        .filter_map(|action| action.get("add"))
        .filter(|add| add.get("deletionVector").is_some())
        .map(|add| add["path"].as_str().unwrap().to_string())
        .collect();
    assert!(!dv_paths.is_empty(), "merge should have added a DV'd file");

    overwrite(1..101).await;

    let overwrite_actions = read_log_actions(&table_path, 3);
    let removes: Vec<&serde_json::Value> = overwrite_actions
        .iter()
        .filter_map(|action| action.get("remove"))
        .collect();
    for path in &dv_paths {
        let remove = removes
            .iter()
            .find(|remove| remove["path"].as_str() == Some(path.as_str()))
            .unwrap_or_else(|| panic!("overwrite should remove the DV'd file {path}"));
        assert!(
            remove.get("deletionVector").is_some(),
            "the remove for {path} must carry the descriptor of the DV it tombstones, or it names a \
             different logical file than the add: {remove}"
        );
    }

    assert_eq!(duckdb_count(&table_path), "100");
}
