use futures::stream;
use ldrs_delta::{merge_delta, overwrite_delta, MergeConfig, OperationConfig, TxnConfig};
use ldrs_test_fixtures::delta::{
    cleanup_table, count_actions, delta_table_path, duckdb_count, duckdb_summary, find_action,
    latest_version, make_batch_from_ids, make_source_batch, make_target_batch, read_log_actions,
    test_schema, SOURCE_BASE_TS,
};
use std::ops::Range;

fn test_table_path(name: &str) -> String {
    delta_table_path(&format!("optimize_{name}"))
}

// Compaction over what ldrs produces natively: several small files from repeated merges, one of
// them carrying a deletion vector.
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_compacts_small_files_and_materializes_deletion_vectors() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("small_files");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    let merge = async |ids: Range<i64>| {
        let config = MergeConfig {
            merge_keys: vec!["id".to_string()],
            allow_null_keys: false,
            max_rows: None,
            max_bytes: None,
            txn_config: TxnConfig::None,
            inline_deletion_vectors: false,
        };
        merge_delta(
            &table_url,
            schema.clone(),
            stream::iter(vec![Ok(make_source_batch(ids))]),
            config,
            &OperationConfig::new("ldrs-test"),
            &rt,
        )
        .await
        .unwrap()
    };

    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(make_target_batch(1..101))]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

    let stats = merge(51..151).await;
    assert!(stats.files_with_dvs > 0, "merge should have written a DV");
    merge(200..251).await;

    let contents = duckdb_summary(&table_path);
    assert_eq!(contents, "0,50,1,50\n10000,151,51,250");

    let before = ldrs_delta::plan_optimize(&table_url, None, Some("id"), &rt)
        .await
        .unwrap();
    let planned: usize = before.bins().iter().map(|bin| bin.input_files()).sum();
    assert!(
        !before.is_empty() && planned >= 3,
        "expected the small files to bin together, planned {planned} files"
    );

    let outcome = ldrs_delta::execute_plan(before, &OperationConfig::new("ldrs-test"), &rt)
        .await
        .unwrap();
    assert!(!outcome.skipped);
    assert_eq!(outcome.files_added, 1, "one bin should mean one file");
    assert_eq!(outcome.files_removed, planned);
    assert_eq!(
        outcome.deletion_vectors_removed, 1,
        "the DV'd file should be reported as materialized"
    );
    let version = outcome.version.expect("a commit should have been written");

    let actions = read_log_actions(&table_path, version);

    let commit_info = find_action(&actions, "commitInfo").expect("should have commitInfo");
    assert_eq!(commit_info["operation"], "OPTIMIZE");
    assert_eq!(commit_info["operationMetrics"]["numAddedFiles"], "1");
    assert_eq!(
        commit_info["operationMetrics"]["numDeletionVectorsRemoved"],
        "1"
    );
    assert!(
        find_action(&actions, "metaData").is_none(),
        "optimize changes neither schema nor configuration"
    );

    for key in ["add", "remove"] {
        let file_actions: Vec<&serde_json::Value> =
            actions.iter().filter_map(|a| a.get(key)).collect();
        assert!(!file_actions.is_empty(), "expected {key} actions");
        for action in file_actions {
            assert_eq!(
                action["dataChange"], false,
                "{key} must not claim a data change: {action}"
            );
        }
    }

    let added: Vec<&serde_json::Value> = actions.iter().filter_map(|a| a.get("add")).collect();
    assert_eq!(added.len(), 1);
    assert!(
        added[0].get("deletionVector").is_none(),
        "a rewritten file must not carry a DV: {}",
        added[0]
    );
    assert!(
        added[0]["stats"]
            .as_str()
            .is_some_and(|s| s.contains("numRecords")),
        "the new add needs regenerated stats: {}",
        added[0]
    );

    assert_eq!(duckdb_count(&table_path), (150 + 51).to_string());
    assert_eq!(duckdb_summary(&table_path), contents);
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_commits_removes_with_no_add_for_an_all_deleted_bin() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("all_deleted");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}/", table_path);

    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(make_target_batch(1..101))]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

    let stats = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(make_source_batch(1..101))]),
        MergeConfig {
            merge_keys: vec!["id".to_string()],
            allow_null_keys: false,
            max_rows: None,
            max_bytes: None,
            txn_config: TxnConfig::None,
            inline_deletion_vectors: false,
        },
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();
    assert_eq!(stats.matched_rows, 100);
    assert_eq!(stats.files_with_dvs, 1);

    let contents = duckdb_summary(&table_path);
    assert_eq!(contents, "10000,100,1,100");

    let plan = ldrs_delta::plan_optimize(&table_url, Some(1), Some("id"), &rt)
        .await
        .unwrap();
    assert_eq!(plan.bins().len(), 1);
    assert_eq!(plan.bins()[0].input_files(), 1);

    let outcome = ldrs_delta::execute_plan(plan, &OperationConfig::new("ldrs-test"), &rt)
        .await
        .unwrap();
    assert!(!outcome.skipped);
    assert_eq!(outcome.files_added, 0, "every row of the bin was deleted");
    assert_eq!(outcome.files_removed, 1);
    assert_eq!(outcome.deletion_vectors_removed, 1);
    assert_eq!(outcome.bytes_added, 0);
    let version = outcome.version.expect("a commit should have been written");

    let actions = read_log_actions(&table_path, version);
    assert_eq!(
        count_actions(&actions, "add"),
        0,
        "no rows survived the bin"
    );
    assert_eq!(count_actions(&actions, "remove"), 1);

    let commit_info = find_action(&actions, "commitInfo").expect("should have commitInfo");
    assert_eq!(commit_info["operation"], "OPTIMIZE");
    assert_eq!(commit_info["operationMetrics"]["numAddedFiles"], "0");
    assert_eq!(
        commit_info["operationMetrics"]["numDeletionVectorsRemoved"],
        "1"
    );

    assert_eq!(duckdb_summary(&table_path), contents);
    assert_eq!(duckdb_count(&table_path), "100");
}

async fn small_file_table(table_path: &str, rt: &tokio::runtime::Handle) -> String {
    cleanup_table(table_path);
    let table_url = format!("file://{}/", table_path);
    overwrite_delta(
        &table_url,
        test_schema(),
        stream::iter(vec![Ok(make_target_batch(1..101))]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        rt,
    )
    .await
    .unwrap();
    for ids in [200..251, 300..351] {
        merge_delta(
            &table_url,
            test_schema(),
            stream::iter(vec![Ok(make_source_batch(ids))]),
            MergeConfig {
                merge_keys: vec!["id".to_string()],
                allow_null_keys: false,
                max_rows: None,
                max_bytes: None,
                txn_config: TxnConfig::None,
                inline_deletion_vectors: false,
            },
            &OperationConfig::new("ldrs-test"),
            rt,
        )
        .await
        .unwrap();
    }
    table_url
}

fn parquet_file_count(table_path: &str) -> usize {
    std::fs::read_dir(table_path)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        .count()
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_abandons_when_another_writer_commits_first() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("commit_race");
    let table_url = small_file_table(&table_path, &rt).await;

    let plan = ldrs_delta::plan_optimize(&table_url, None, Some("id"), &rt)
        .await
        .unwrap();
    assert!(!plan.is_empty());
    let planned_at = latest_version(&table_path);

    merge_delta(
        &table_url,
        test_schema(),
        stream::iter(vec![Ok(make_source_batch(400..451))]),
        MergeConfig {
            merge_keys: vec!["id".to_string()],
            allow_null_keys: false,
            max_rows: None,
            max_bytes: None,
            txn_config: TxnConfig::None,
            inline_deletion_vectors: false,
        },
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();
    assert_eq!(latest_version(&table_path), planned_at + 1);
    let contents = duckdb_summary(&table_path);

    let error = ldrs_delta::execute_plan(plan, &OperationConfig::new("ldrs-test"), &rt)
        .await
        .expect_err("the version optimize planned to write was taken")
        .to_string();
    assert!(
        error.contains(&format!("committed version {}", planned_at + 1)),
        "the error should name the collision: {error}"
    );

    assert_eq!(latest_version(&table_path), planned_at + 1);
    assert_eq!(duckdb_summary(&table_path), contents);
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_planning_optimize_writes_nothing() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("dry_run");
    let table_url = small_file_table(&table_path, &rt).await;

    let before_version = latest_version(&table_path);
    let before_contents = duckdb_summary(&table_path);
    let before_files = parquet_file_count(&table_path);

    let plan = ldrs_delta::plan_optimize(&table_url, None, Some("id"), &rt)
        .await
        .unwrap();
    assert!(!plan.is_empty(), "there is work to report");
    drop(plan);

    assert_eq!(latest_version(&table_path), before_version, "no commit");
    assert_eq!(
        parquet_file_count(&table_path),
        before_files,
        "planning opens no writer"
    );
    assert_eq!(duckdb_summary(&table_path), before_contents);
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_target_size_prefers_the_flag_then_the_table_property() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("target_size");
    let table_url = small_file_table(&table_path, &rt).await;

    let bins = async |target: Option<u64>| {
        let plan = ldrs_delta::plan_optimize(&table_url, target, Some("id"), &rt)
            .await
            .unwrap();
        let files: usize = plan.bins().iter().map(|bin| bin.input_files()).sum();
        (plan.bins().len(), files)
    };

    assert_eq!(bins(None).await, (1, 3));

    let mut one_byte_files = OperationConfig::new("ldrs-test");
    one_byte_files.target_file_size = Some(std::num::NonZeroU64::new(1).unwrap());
    overwrite_delta(
        &table_url,
        test_schema(),
        stream::iter(vec![Ok(make_target_batch(1..101))]),
        None,
        None,
        &one_byte_files,
        &rt,
    )
    .await
    .unwrap();
    for ids in [200..251, 300..351] {
        merge_delta(
            &table_url,
            test_schema(),
            stream::iter(vec![Ok(make_source_batch(ids))]),
            MergeConfig {
                merge_keys: vec!["id".to_string()],
                allow_null_keys: false,
                max_rows: None,
                max_bytes: None,
                txn_config: TxnConfig::None,
                inline_deletion_vectors: false,
            },
            &OperationConfig::new("ldrs-test"),
            &rt,
        )
        .await
        .unwrap();
    }

    assert_eq!(
        bins(None).await,
        (0, 0),
        "the table property should be read when no flag is given"
    );

    assert_eq!(
        bins(Some(256 * 1024 * 1024)).await,
        (1, 3),
        "the flag should win over the table property"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_reads_a_sidecar_dv_through_a_root_without_a_trailing_slash() {
    let rt = tokio::runtime::Handle::current();
    let table_path = test_table_path("sidecar_dv_no_slash");
    cleanup_table(&table_path);

    let schema = test_schema();
    let table_url = format!("file://{}", table_path);

    overwrite_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(make_target_batch(1..2001))]),
        None,
        None,
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();

    let odd_ids: Vec<i64> = (1..=1399).step_by(2).collect();
    let stats = merge_delta(
        &table_url,
        schema.clone(),
        stream::iter(vec![Ok(make_batch_from_ids(
            odd_ids,
            10_000,
            SOURCE_BASE_TS,
        ))]),
        MergeConfig {
            merge_keys: vec!["id".to_string()],
            allow_null_keys: false,
            max_rows: None,
            max_bytes: None,
            txn_config: TxnConfig::None,
            inline_deletion_vectors: false,
        },
        &OperationConfig::new("ldrs-test"),
        &rt,
    )
    .await
    .unwrap();
    assert!(stats.files_with_dvs > 0, "merge should have written a DV");

    let contents = duckdb_summary(&table_path);

    let plan = ldrs_delta::plan_optimize(&table_url, None, Some("id"), &rt)
        .await
        .unwrap();
    assert!(!plan.is_empty());

    let outcome = ldrs_delta::execute_plan(plan, &OperationConfig::new("ldrs-test"), &rt)
        .await
        .unwrap();
    assert_eq!(outcome.deletion_vectors_removed, 1);
    assert_eq!(duckdb_summary(&table_path), contents);
}
