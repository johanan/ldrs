//! Delta operations over tables Spark wrote.
//!
//! `scripts/spark/build_fixtures.py` builds the fixtures.

use ldrs_delta::OperationConfig;
use ldrs_test_fixtures::delta::{
    copy_fixture_table, count_actions, duckdb_csv, find_action, latest_version, read_log_actions,
};

/// Rows and a checksum per island, which a rewrite must leave identical.
fn duckdb_islands(table_path: &str) -> String {
    duckdb_csv(&format!(
        "SELECT island, count(*), sum(body_mass_g), sum(sample_number) \
         FROM delta_scan('{table_path}') GROUP BY 1 ORDER BY 1"
    ))
}

/// Rows and a checksum per study year, under the table's logical column names.
fn duckdb_studies(table_path: &str) -> String {
    duckdb_csv(&format!(
        "SELECT \"studyName\", count(*), sum(\"Body Mass (g)\"), sum(\"Sample Number\") \
         FROM delta_scan('{table_path}') GROUP BY 1 ORDER BY 1"
    ))
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_compacts_a_spark_written_partitioned_table() {
    let rt = tokio::runtime::Handle::current();
    let table_path = copy_fixture_table(
        "delta_spark/penguins_partitioned",
        "ecosystem_penguins_partitioned",
    );
    let table_url = format!("file://{}/", table_path);

    assert_eq!(latest_version(&table_path), 2, "three appends");
    let contents = duckdb_islands(&table_path);

    let plan = ldrs_delta::plan_optimize(&table_url, None, Some("sample_number"), &rt)
        .await
        .unwrap();
    assert_eq!(plan.bins().len(), 3, "one bin per island partition");
    for bin in plan.bins() {
        assert_eq!(bin.input_files(), 3, "three appends land three files here");
    }

    let outcome = ldrs_delta::execute_plan(plan, &OperationConfig::new("ldrs-test"), &rt)
        .await
        .unwrap();
    assert!(!outcome.skipped);
    assert_eq!(outcome.files_added, 3);
    assert_eq!(outcome.files_removed, 9);
    assert_eq!(outcome.deletion_vectors_removed, 0);
    let version = outcome.version.expect("a commit should have been written");
    assert_eq!(version, 3);

    let actions = read_log_actions(&table_path, version);
    assert_eq!(count_actions(&actions, "add"), 3);
    assert_eq!(count_actions(&actions, "remove"), 9);

    let commit_info = find_action(&actions, "commitInfo").expect("should have commitInfo");
    assert_eq!(commit_info["operation"], "OPTIMIZE");
    assert_eq!(commit_info["operationMetrics"]["numAddedFiles"], "3");

    assert!(
        find_action(&actions, "metaData").is_none(),
        "optimize changes neither schema nor configuration"
    );
    assert!(
        find_action(&actions, "protocol").is_none(),
        "a rewrite must not restate, and so cannot downgrade, a foreign writer's protocol"
    );

    for key in ["add", "remove"] {
        for action in actions.iter().filter_map(|a| a.get(key)) {
            assert_eq!(
                action["dataChange"], false,
                "{key} must not claim a data change: {action}"
            );
        }
    }

    // Each output records its sources' partition values and keeps Spark's `island=<value>` path.
    let mut islands: Vec<String> = actions
        .iter()
        .filter_map(|a| a.get("add"))
        .map(|add| {
            let island = add["partitionValues"]["island"]
                .as_str()
                .unwrap()
                .to_string();
            let path = add["path"].as_str().unwrap();
            assert!(
                path.starts_with(&format!("island={island}/")),
                "the rewrite must land beside its sources: {path}"
            );
            assert!(
                add["stats"]
                    .as_str()
                    .is_some_and(|stats| stats.contains("numRecords")),
                "the new add needs regenerated stats: {add}"
            );
            island
        })
        .collect();
    islands.sort();
    assert_eq!(islands, ["Biscoe", "Dream", "Torgersen"]);

    assert_eq!(duckdb_islands(&table_path), contents);

    // One file per partition leaves nothing to bin against, so a second run plans no work.
    let again = ldrs_delta::plan_optimize(&table_url, None, Some("sample_number"), &rt)
        .await
        .unwrap();
    assert!(again.is_empty(), "compaction should be idempotent");
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_compacts_a_column_mapped_table() {
    let rt = tokio::runtime::Handle::current();
    let table_path = copy_fixture_table(
        "delta_spark/penguins_column_mapped",
        "ecosystem_penguins_column_mapped",
    );
    let table_url = format!("file://{}/", table_path);

    assert_eq!(latest_version(&table_path), 2, "three appends");
    let contents = duckdb_studies(&table_path);

    let plan = ldrs_delta::plan_optimize(&table_url, None, Some("Sample Number"), &rt)
        .await
        .unwrap();
    assert_eq!(plan.bins().len(), 1, "unpartitioned, one schema, one bin");

    let outcome = ldrs_delta::execute_plan(plan, &OperationConfig::new("ldrs-test"), &rt)
        .await
        .unwrap();
    assert_eq!(outcome.files_added, 1);
    assert_eq!(outcome.files_removed, 3);
    let version = outcome.version.expect("a commit should have been written");

    let actions = read_log_actions(&table_path, version);
    assert!(
        find_action(&actions, "metaData").is_none(),
        "the column mapping lives in metaData, and a rewrite must not restate it"
    );
    assert!(
        find_action(&actions, "protocol").is_none(),
        "a rewrite must carry a foreign writer's feature list through untouched"
    );

    // Delta keys a column-mapped table's statistics by physical name.
    let add = find_action(&actions, "add").expect("should have an add");
    let stats = add["stats"].as_str().expect("regenerated stats");
    assert!(
        stats.contains("col-"),
        "statistics must be keyed by physical name: {stats}"
    );
    assert!(
        !stats.contains("Sample Number"),
        "a logical name in statistics would be unreadable to the table's other writers: {stats}"
    );

    assert_eq!(duckdb_studies(&table_path), contents);
}

/// The physical name `column` is written under, from the metaData its first commit declared.
fn physical_name(table_path: &str, column: &str) -> String {
    let actions = read_log_actions(table_path, 0);
    let metadata = find_action(&actions, "metaData").expect("version 0 declares the schema");
    let schema: serde_json::Value =
        serde_json::from_str(metadata["schemaString"].as_str().unwrap()).unwrap();
    schema["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|field| field["name"] == column)
        .unwrap_or_else(|| panic!("the table has no column '{column}'"))["metadata"]
        ["delta.columnMapping.physicalName"]
        .as_str()
        .unwrap()
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_packs_a_column_mapped_table_by_its_logical_order_column() {
    let rt = tokio::runtime::Handle::current();
    let table_path = copy_fixture_table(
        "delta_spark/penguins_column_mapped",
        "ecosystem_column_mapped_order",
    );
    let table_url = format!("file://{}/", table_path);

    // Each file is a little under 14KB, so this holds two of them and leaves the third alone.
    let plan = ldrs_delta::plan_optimize(&table_url, Some(30_000), Some("Sample Number"), &rt)
        .await
        .unwrap();
    assert_eq!(
        plan.bins().len(),
        1,
        "the third file bins alone and is dropped"
    );
    assert_eq!(plan.bins()[0].input_files(), 2);

    let outcome = ldrs_delta::execute_plan(plan, &OperationConfig::new("ldrs-test"), &rt)
        .await
        .unwrap();
    assert_eq!(outcome.files_added, 1);
    assert_eq!(outcome.files_removed, 2);
    let version = outcome.version.expect("a commit should have been written");

    let actions = read_log_actions(&table_path, version);
    let add = find_action(&actions, "add").expect("should have an add");
    let stats: serde_json::Value = serde_json::from_str(add["stats"].as_str().unwrap()).unwrap();

    // Resolved, the two lowest-numbered files pack together and start at 1; unresolved, the pair
    // stays in log order and starts at 27.
    assert_eq!(
        stats["minValues"][physical_name(&table_path, "Sample Number")],
        1
    );
}

/// A column the table does not have is refused rather than quietly packing unordered.
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_refuses_an_order_column_the_table_does_not_have() {
    let rt = tokio::runtime::Handle::current();
    let table_path = copy_fixture_table(
        "delta_spark/penguins_column_mapped",
        "ecosystem_column_mapped_bad_order",
    );
    let table_url = format!("file://{}/", table_path);

    // The name this table's columns would have without column mapping.
    let err = ldrs_delta::plan_optimize(&table_url, None, Some("sample_number"), &rt)
        .await
        .err()
        .expect("an order column the table does not have must be refused");
    assert!(
        format!("{err:#}").contains("sample_number"),
        "the error must name the column: {err:#}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_refuses_a_row_tracking_and_clustered_table() {
    let rt = tokio::runtime::Handle::current();
    let table_path = copy_fixture_table(
        "delta_spark/penguins_row_tracking_clustered",
        "ecosystem_row_tracking_clustered",
    );
    let table_url = format!("file://{}/", table_path);

    let err = ldrs_delta::plan_optimize(&table_url, None, None, &rt)
        .await
        .err()
        .expect("a table ldrs cannot rewrite must be refused");
    let message = format!("{err:#}");

    assert!(
        message.contains("cannot optimize this table"),
        "got: {message}"
    );
    assert!(
        message.contains("cannot be carried forward"),
        "got: {message}"
    );
    for feature in ["rowTracking", "clustering"] {
        assert!(message.contains(feature), "must name {feature}: {message}");
    }
    for supported in ["appendOnly", "invariants", "domainMetadata"] {
        assert!(
            !message.contains(supported),
            "a supported feature must not be named: {message}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_refuses_an_iceberg_compatible_table() {
    let rt = tokio::runtime::Handle::current();
    let table_path = copy_fixture_table(
        "delta_spark/penguins_iceberg_compat",
        "ecosystem_iceberg_compat",
    );
    let table_url = format!("file://{}/", table_path);

    let err = ldrs_delta::plan_optimize(&table_url, None, None, &rt)
        .await
        .err()
        .expect("a table whose files are not enumerable must be refused");
    let message = format!("{err:#}");

    assert!(
        message.contains("cannot optimize this table"),
        "got: {message}"
    );
    assert!(
        message.contains("not in the table's add actions"),
        "got: {message}"
    );
    assert!(message.contains("icebergCompatV2"), "got: {message}");
    assert!(
        !message.contains("columnMapping"),
        "column mapping is supported and must not be named: {message}"
    );
}

#[test_log::test]
fn test_id_mode_still_names_parquet_columns_by_physical_name() {
    let table_path = ldrs_test_fixtures::fixture_str("delta_spark/penguins_column_mapped_id");
    let actions = read_log_actions(&table_path, 0);
    let file = find_action(&actions, "add").expect("one add")["path"]
        .as_str()
        .unwrap()
        .to_string();

    let named = duckdb_csv(&format!(
        "SELECT name FROM parquet_schema('{table_path}/{file}') WHERE field_id IS NOT NULL"
    ));
    let columns: Vec<&str> = named.lines().collect();

    assert!(
        columns.iter().all(|name| name.starts_with("col-")),
        "id mode must still name columns physically: {named}"
    );
    assert!(
        columns.contains(&physical_name(&table_path, "Sample Number").as_str()),
        "the column an order column resolves to must be one of them: {named}"
    );
}

/// The `inCommitTimestamp` a commit carries.
fn in_commit_timestamp(table_path: &str, version: u64) -> i64 {
    let actions = read_log_actions(table_path, version);
    find_action(&actions, "commitInfo")
        .and_then(|info| info["inCommitTimestamp"].as_i64())
        .unwrap_or_else(|| panic!("version {version} should carry an in-commit timestamp"))
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_carries_a_foreign_in_commit_timestamp_forward() {
    let rt = tokio::runtime::Handle::current();
    let table_path = copy_fixture_table(
        "delta_spark/penguins_in_commit_timestamps",
        "ecosystem_in_commit_timestamps",
    );
    let table_url = format!("file://{}/", table_path);

    assert_eq!(latest_version(&table_path), 1, "two appends");
    let previous = in_commit_timestamp(&table_path, 1);
    let contents = duckdb_islands(&table_path);

    let plan = ldrs_delta::plan_optimize(&table_url, None, Some("sample_number"), &rt)
        .await
        .unwrap();
    assert_eq!(plan.bins().len(), 1, "two files, one bin");

    let outcome = ldrs_delta::execute_plan(plan, &OperationConfig::new("ldrs-test"), &rt)
        .await
        .unwrap();
    assert_eq!(outcome.files_added, 1);
    assert_eq!(outcome.files_removed, 2);
    let version = outcome.version.expect("a commit should have been written");

    assert!(
        in_commit_timestamp(&table_path, version) > previous,
        "the sequence has to keep increasing across a writer change"
    );

    let actions = read_log_actions(&table_path, version);
    assert!(
        find_action(&actions, "protocol").is_none(),
        "the table already declares inCommitTimestamp, so there is nothing to add"
    );
    assert!(find_action(&actions, "metaData").is_none());

    assert_eq!(duckdb_islands(&table_path), contents);
}

/// Rows per partition, with a real null told apart from an empty string.
fn duckdb_partitions(table_path: &str) -> String {
    duckdb_csv(&format!(
        "SELECT island, coalesce(sex, '<null>'), count(*), sum(body_mass_g) \
         FROM delta_scan('{table_path}') GROUP BY 1, 2 ORDER BY 1, 2"
    ))
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_optimize_preserves_a_null_partition_value() {
    let rt = tokio::runtime::Handle::current();
    let table_path = copy_fixture_table(
        "delta_spark/penguins_partitioned_nulls",
        "ecosystem_partitioned_nulls",
    );
    let table_url = format!("file://{}/", table_path);

    let contents = duckdb_partitions(&table_path);

    let plan = ldrs_delta::plan_optimize(&table_url, None, Some("sample_number"), &rt)
        .await
        .unwrap();
    // The unsexed penguins on Dream and Torgersen appear in one study year only, so those two
    // partitions hold a single file and are left alone.
    assert_eq!(plan.bins().len(), 7);

    let outcome = ldrs_delta::execute_plan(plan, &OperationConfig::new("ldrs-test"), &rt)
        .await
        .unwrap();
    assert_eq!(outcome.files_added, 7);
    assert_eq!(outcome.files_removed, 14);
    let version = outcome.version.expect("a commit should have been written");

    let actions = read_log_actions(&table_path, version);
    let null_add = actions
        .iter()
        .filter_map(|a| a.get("add"))
        .find(|add| {
            add["path"]
                .as_str()
                .unwrap()
                .contains("__HIVE_DEFAULT_PARTITION__")
        })
        .expect("the unsexed penguins' partition should have been packed");

    let values = null_add["partitionValues"]
        .as_object()
        .expect("an add carries partition values");
    assert_eq!(
        values.len(),
        2,
        "an entry per partition column the table declares: {null_add}"
    );
    assert_eq!(
        values["sex"],
        serde_json::Value::Null,
        "an unrecorded sex is a null partition value, not an empty string: {null_add}"
    );
    assert_eq!(values["island"], "Biscoe");
    assert!(
        null_add["path"]
            .as_str()
            .unwrap()
            .starts_with("island=Biscoe/sex=__HIVE_DEFAULT_PARTITION__/"),
        "the rewrite must land beside its sources: {null_add}"
    );

    assert_eq!(duckdb_partitions(&table_path), contents);
}
