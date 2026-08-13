use delta_kernel::expressions::Scalar;
use futures::TryStreamExt;
use ldrs::ldrs_config::{execute_configs, parse_yaml_config, resolve_delta_targets};
use ldrs_delta::{
    delta_stats_to_json, overwrite_delta, parquet_metadata_to_delta_stats, vacuum, Retention,
};
use ldrs_parquet::builder_from_string;
use ldrs_test_fixtures::{data_url, fixture, fixture_url};

#[tokio::test]
async fn test_delta_stats_from_users_parquet() {
    let path = fixture_url("public.users/public.users.snappy.parquet");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let builder = builder_from_string(path, rt.handle().clone())
        .await
        .unwrap();

    let schema = builder.schema().clone();
    let metadata = builder.metadata().clone();

    let stats = parquet_metadata_to_delta_stats(&metadata, &schema);

    assert_eq!(stats.num_records, 2);
    assert_eq!(stats.columns.len(), schema.fields().len());

    assert!(!stats.tight_bounds);

    let find = |name: &str| {
        stats
            .columns
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, s)| s)
            .unwrap()
    };

    let name = find("name");
    assert_eq!(name.null_count, 0);
    assert_eq!(name.min, Some(Scalar::Binary(b"John Doe".to_vec())));
    assert_eq!(name.max, Some(Scalar::Binary(b"Test Tester".to_vec())));

    let active = find("active");
    assert_eq!(active.min, Some(Scalar::Boolean(false)));
    assert_eq!(active.max, Some(Scalar::Boolean(true)));
    assert_eq!(active.null_count, 0);

    let nullable = find("nullable_id");
    assert_eq!(nullable.null_count, 2);
    assert_eq!(nullable.min, None);
    assert_eq!(nullable.max, None);

    let created = find("created");
    assert_eq!(created.min, Some(Scalar::Long(1728408120000000)));
    assert_eq!(created.max, Some(Scalar::Long(1728408120000000)));

    let uuid = find("unique_id");
    assert_eq!(
        uuid.min,
        Some(Scalar::Binary(vec![
            160, 238, 188, 153, 156, 11, 78, 248, 187, 109, 107, 185, 189, 56, 10, 17
        ]))
    );
    assert_eq!(
        uuid.max,
        Some(Scalar::Binary(vec![
            160, 238, 188, 153, 156, 11, 78, 248, 187, 109, 107, 185, 189, 56, 10, 18
        ]))
    );

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test]
async fn test_delta_stats_from_strings_parquet() {
    let path = fixture_url("public.string_values/public.strings.snappy.parquet");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let builder = builder_from_string(path, rt.handle().clone())
        .await
        .unwrap();

    let schema = builder.schema().clone();
    let metadata = builder.metadata().clone();

    let stats = parquet_metadata_to_delta_stats(&metadata, &schema);

    assert_eq!(stats.num_records, 2);
    assert_eq!(stats.columns.len(), schema.fields().len());

    let find = |name: &str| {
        stats
            .columns
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, s)| s)
            .unwrap()
    };

    let varchar = find("varchar_value");
    assert_eq!(varchar.min, Some(Scalar::Binary(b"a".to_vec())));
    assert_eq!(varchar.max, Some(Scalar::Binary(b"b".to_vec())));
    assert_eq!(varchar.null_count, 0);

    let text = find("text_value");
    assert_eq!(text.min, Some(Scalar::Binary(b"a".to_vec())));
    assert_eq!(text.max, Some(Scalar::Binary(b"b".to_vec())));

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test]
async fn test_delta_stats_from_numbers_parquet() {
    let path = fixture_url("public.numbers/public.numbers.snappy.parquet");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let builder = builder_from_string(path, rt.handle().clone())
        .await
        .unwrap();

    let schema = builder.schema().clone();
    let metadata = builder.metadata().clone();

    let stats = parquet_metadata_to_delta_stats(&metadata, &schema);

    assert_eq!(stats.num_records, 5);
    assert_eq!(stats.columns.len(), schema.fields().len());

    let find = |name: &str| {
        stats
            .columns
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, s)| s)
            .unwrap()
    };

    let smallint = find("smallint_value");
    assert_eq!(smallint.min, Some(Scalar::Integer(0)));
    assert_eq!(smallint.max, Some(Scalar::Integer(32767)));
    assert_eq!(smallint.null_count, 0);

    let integer = find("integer_value");
    assert_eq!(integer.min, Some(Scalar::Integer(0)));
    assert_eq!(integer.max, Some(Scalar::Integer(2147483647)));

    let bigint = find("bigint_value");
    assert_eq!(bigint.min, Some(Scalar::Long(0)));
    assert_eq!(bigint.max, Some(Scalar::Long(9223372036854775807)));

    let double = find("double_value");
    assert_eq!(double.min, Some(Scalar::Double(1.0)));
    assert_eq!(double.max, Some(Scalar::Double(1.2345678901234568e20)));

    let float = find("float_value");
    assert_eq!(float.min, Some(Scalar::Float(1.0)));
    assert_eq!(float.max, Some(Scalar::Float(12345679.0)));

    let decimal = find("decimal_value");
    assert_eq!(decimal.null_count, 0);
    assert_eq!(
        decimal.min,
        Some(Scalar::Binary(vec![
            0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 141, 126, 164, 198, 128, 0
        ]))
    );
    assert_eq!(
        decimal.max,
        Some(Scalar::Binary(vec![
            0, 0, 0, 0, 0, 0, 0, 27, 131, 104, 185, 86, 247, 5, 48, 192
        ]))
    );

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test]
async fn test_delta_stats_json_from_users_parquet() {
    let path = fixture_url("public.users/public.users.snappy.parquet");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let builder = builder_from_string(path, rt.handle().clone())
        .await
        .unwrap();

    let schema = builder.schema().clone();
    let metadata = builder.metadata().clone();

    let stats = parquet_metadata_to_delta_stats(&metadata, &schema);
    let json_str = delta_stats_to_json(&stats, &schema).unwrap();
    let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

    assert_eq!(json["numRecords"], 2);

    assert_eq!(json["minValues"]["name"], "John Doe");
    assert_eq!(json["maxValues"]["name"], "Test Tester");

    assert_eq!(json["minValues"]["created"], "2024-10-08T17:22:00.000");
    assert_eq!(json["maxValues"]["created"], "2024-10-08T17:22:00.000");

    assert_eq!(json["minValues"]["createdz"], "2024-10-08T17:22:00.000Z");
    assert_eq!(json["maxValues"]["createdz"], "2024-10-08T17:22:00.000Z");

    assert_eq!(json["minValues"]["active"], false);
    assert_eq!(json["maxValues"]["active"], true);

    assert!(json["minValues"]["unique_id"]
        .as_str()
        .unwrap()
        .contains("\\u"));
    assert!(json["maxValues"]["unique_id"]
        .as_str()
        .unwrap()
        .contains("\\u"));

    assert_eq!(json["nullCount"]["nullable_id"], 2);
    assert!(json["minValues"].get("nullable_id").is_none());
    assert!(json["maxValues"].get("nullable_id").is_none());

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test]
async fn test_delta_stats_json_from_numbers_parquet() {
    let path = fixture_url("public.numbers/public.numbers.snappy.parquet");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let builder = builder_from_string(path, rt.handle().clone())
        .await
        .unwrap();

    let schema = builder.schema().clone();
    let metadata = builder.metadata().clone();

    let stats = parquet_metadata_to_delta_stats(&metadata, &schema);
    let json_str = delta_stats_to_json(&stats, &schema).unwrap();
    let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

    assert_eq!(json["numRecords"], 5);

    assert_eq!(json["minValues"]["smallint_value"], 0);
    assert_eq!(json["maxValues"]["smallint_value"], 32767);
    assert_eq!(json["minValues"]["integer_value"], 0);
    assert_eq!(json["maxValues"]["integer_value"], 2147483647);

    assert_eq!(json["minValues"]["bigint_value"], 0);
    assert_eq!(json["maxValues"]["bigint_value"], 9223372036854775807_i64);

    assert_eq!(json["minValues"]["double_value"], 1.0);
    assert_eq!(json["maxValues"]["double_value"], 1.2345678901234568e20);

    assert_eq!(json["minValues"]["float_value"], 1.0);
    assert_eq!(json["maxValues"]["float_value"], 12345679.0);

    let dec_min = json["minValues"]["decimal_value"].as_str().unwrap();
    let dec_max = json["maxValues"]["decimal_value"].as_str().unwrap();
    assert!(dec_min.contains('.'), "decimal should have decimal point");
    assert!(dec_max.contains('.'), "decimal should have decimal point");

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_overwrite_delta() {
    let source_path = fixture_url("public.users/public.users.snappy.parquet");

    let table_path = fixture("delta_writes/users_delta/").display().to_string();
    let table_url = fixture_url("delta_writes/users_delta/");
    let _ = std::fs::remove_dir_all(&table_path);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let builder = builder_from_string(source_path.clone(), rt.handle().clone())
        .await
        .unwrap();
    let schema = builder.schema().clone();
    let stream = builder
        .with_batch_size(1024)
        .build()
        .unwrap()
        .map_err(|e: parquet::errors::ParquetError| anyhow::anyhow!(e));

    overwrite_delta(&table_url, schema.clone(), stream, None, None, rt.handle())
        .await
        .unwrap();

    let v0_path = format!("{}/_delta_log/00000000000000000000.json", table_path);
    assert!(std::path::Path::new(&v0_path).exists());
    let v0_content = std::fs::read_to_string(&v0_path).unwrap();
    let v0_lines: Vec<&str> = v0_content.lines().collect();
    assert_eq!(
        v0_lines.len(),
        3,
        "Version 0 should have 3 actions (commitInfo, protocol, metaData)"
    );

    let v0_commit: serde_json::Value = serde_json::from_str(v0_lines[0]).unwrap();
    assert_eq!(v0_commit["commitInfo"]["operation"], "CREATE TABLE");

    let v1_path = format!("{}/_delta_log/00000000000000000001.json", table_path);
    assert!(std::path::Path::new(&v1_path).exists());
    let v1_content = std::fs::read_to_string(&v1_path).unwrap();
    let v1_lines: Vec<&str> = v1_content.lines().collect();
    assert!(
        v1_lines.len() >= 4,
        "Version 1 should have at least 4 actions"
    );

    let v1_commit: serde_json::Value = serde_json::from_str(v1_lines[0]).unwrap();
    assert_eq!(v1_commit["commitInfo"]["operation"], "WRITE");
    assert_eq!(
        v1_commit["commitInfo"]["operationParameters"]["mode"],
        "Overwrite"
    );

    let v1_protocol: serde_json::Value = serde_json::from_str(v1_lines[1]).unwrap();
    assert!(v1_protocol.get("protocol").is_some());
    let v1_metadata: serde_json::Value = serde_json::from_str(v1_lines[2]).unwrap();
    assert!(v1_metadata.get("metaData").is_some());

    let v1_add: serde_json::Value = serde_json::from_str(v1_lines[3]).unwrap();
    assert!(v1_add.get("add").is_some());
    let add = &v1_add["add"];
    assert!(add["path"].as_str().unwrap().ends_with(".parquet"));
    assert!(add["size"].as_i64().unwrap() > 0);

    // verify stats are present
    let stats_str = add["stats"].as_str().expect("add action should have stats");
    let stats: serde_json::Value = serde_json::from_str(stats_str).unwrap();
    assert!(stats["numRecords"].as_i64().unwrap() > 0);
    assert!(stats.get("minValues").is_some());
    assert!(stats.get("maxValues").is_some());
    assert!(stats.get("nullCount").is_some());
    assert!(stats.get("tightBounds").is_some());

    let first_parquet = add["path"].as_str().unwrap().to_string();

    let builder2 = builder_from_string(source_path, rt.handle().clone())
        .await
        .unwrap();
    let stream2 = builder2
        .with_batch_size(1024)
        .build()
        .unwrap()
        .map_err(|e: parquet::errors::ParquetError| anyhow::anyhow!(e));

    overwrite_delta(&table_url, schema, stream2, None, None, rt.handle())
        .await
        .unwrap();

    let v2_path = format!("{}/_delta_log/00000000000000000002.json", table_path);
    assert!(std::path::Path::new(&v2_path).exists());
    let v2_content = std::fs::read_to_string(&v2_path).unwrap();
    let v2_lines: Vec<&str> = v2_content.lines().collect();

    assert!(
        v2_lines.len() >= 5,
        "Version 2 should have at least 5 actions (with removes)"
    );

    let has_remove = v2_lines.iter().any(|line| {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        v.get("remove")
            .and_then(|r| r["path"].as_str())
            .map(|p| p == first_parquet)
            .unwrap_or(false)
    });
    assert!(
        has_remove,
        "Version 2 should remove the file from version 1"
    );

    let new_add = v2_lines.iter().find_map(|line| {
        let v: serde_json::Value = serde_json::from_str(line).unwrap();
        v.get("add")
            .map(|a| a["path"].as_str().unwrap().to_string())
    });
    assert!(new_add.is_some(), "Version 2 should have an add action");
    assert_ne!(
        new_add.unwrap(),
        first_parquet,
        "New parquet should differ from removed one"
    );

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_delta_overwrite_with_config() {
    let config = r#"
src: file
dest: delta.overwrite
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"

tables:
  - name: public.users
    target: public.users_curated
  - name: public.numbers
  - name: public.string_values
    filename: public.string_values/public.strings.snappy.parquet
"#;

    let src_url = data_url();
    let dest_url = fixture_url("delta_writes/config_delta_root/");
    let delta_root = fixture("delta_writes/config_delta_root/")
        .display()
        .to_string();

    // `public.users` has an explicit `target`, so it lands at `public.users_curated`, not its name.
    let table_names = [
        "public.users_curated",
        "public.numbers",
        "public.string_values",
    ];

    // cleanup before test
    for name in &table_names {
        let _ = std::fs::remove_dir_all(format!("{}/{}/", delta_root, name));
    }

    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), src_url),
        ("LDRS_DEST".to_string(), dest_url),
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    execute_configs(
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        rt.handle(),
    )
    .await
    .unwrap();

    for name in &table_names {
        let delta_table = format!("{}/{}/", delta_root, name);

        // version 0 should exist (create table)
        let v0_path = format!("{}/_delta_log/00000000000000000000.json", delta_table);
        assert!(
            std::path::Path::new(&v0_path).exists(),
            "Version 0 should exist for {}",
            name
        );

        // version 1 should exist (overwrite with data)
        let v1_path = format!("{}/_delta_log/00000000000000000001.json", delta_table);
        assert!(
            std::path::Path::new(&v1_path).exists(),
            "Version 1 should exist for {}",
            name
        );

        let v1_content = std::fs::read_to_string(&v1_path).unwrap();
        let v1_lines: Vec<&str> = v1_content.lines().collect();

        let v1_commit: serde_json::Value = serde_json::from_str(v1_lines[0]).unwrap();
        assert_eq!(v1_commit["commitInfo"]["operation"], "WRITE");
        assert_eq!(
            v1_commit["commitInfo"]["operationParameters"]["mode"],
            "Overwrite"
        );

        // find the first add action
        let v1_add = v1_lines
            .iter()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|v| v.get("add").is_some())
            .expect(&format!("Version 1 should have an add action for {}", name));

        let add = &v1_add["add"];
        assert!(add["path"].as_str().unwrap().ends_with(".parquet"));
        let stats_str = add["stats"].as_str().expect("add should have stats");
        let stats: serde_json::Value = serde_json::from_str(stats_str).unwrap();
        assert!(
            stats["numRecords"].as_i64().unwrap() > 0,
            "numRecords should be > 0 for {}",
            name
        );
    }

    // for name in &table_names {
    //     let _ = std::fs::remove_dir_all(format!("{}/{}/", delta_root, name));
    // }

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_delta_merge_with_config() {
    // Same config run twice:
    //   1st run: ensure_table creates v0, merge inserts all source rows at v1
    //   2nd run: merge finds same keys in target, writes DVs + new adds at v2
    let config = r#"
src: file
dest: delta.merge
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"

tables:
  - name: public.numbers
    delta.merge_keys: [bigint_value]
"#;

    let src_url = data_url();
    let dest_url = fixture_url("delta_writes/config_delta_merge_root/");
    let delta_root = fixture("delta_writes/config_delta_merge_root/")
        .display()
        .to_string();
    let table_path = format!("{}/public.numbers/", delta_root);

    // cleanup before test
    let _ = std::fs::remove_dir_all(&table_path);

    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), src_url),
        ("LDRS_DEST".to_string(), dest_url),
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    // First run: creates the table (v0) and commits the initial merge (v1)
    execute_configs(
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        rt.handle(),
    )
    .await
    .unwrap();

    let v0_path = format!("{}_delta_log/00000000000000000000.json", table_path);
    let v1_path = format!("{}_delta_log/00000000000000000001.json", table_path);
    assert!(
        std::path::Path::new(&v0_path).exists(),
        "first run should create v0 (ensure_table)"
    );
    assert!(
        std::path::Path::new(&v1_path).exists(),
        "first run should commit merge at v1"
    );

    let v1_content = std::fs::read_to_string(&v1_path).unwrap();
    let v1_commit: serde_json::Value =
        serde_json::from_str(v1_content.lines().next().unwrap()).unwrap();
    assert_eq!(
        v1_commit["commitInfo"]["operation"], "MERGE",
        "first run should dispatch to merge_delta"
    );

    // First merge into an empty table should have no remove actions pure insert
    let v1_removes = v1_content
        .lines()
        .filter(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|v| v.get("remove").cloned())
                .is_some()
        })
        .count();
    assert_eq!(v1_removes, 0, "first merge on empty table has no removes");

    // Second run: same source, same keys, so all rows match → DV path
    execute_configs(
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        rt.handle(),
    )
    .await
    .unwrap();

    let v2_path = format!("{}_delta_log/00000000000000000002.json", table_path);
    assert!(
        std::path::Path::new(&v2_path).exists(),
        "second run should commit merge at v2"
    );

    let v2_content = std::fs::read_to_string(&v2_path).unwrap();
    let v2_lines: Vec<&str> = v2_content.lines().collect();

    let v2_commit: serde_json::Value = serde_json::from_str(v2_lines[0]).unwrap();
    assert_eq!(v2_commit["commitInfo"]["operation"], "MERGE");

    // Second merge should find the same keys → at least one remove + DV add
    let v2_removes = v2_lines
        .iter()
        .filter(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|v| v.get("remove").cloned())
                .is_some()
        })
        .count();
    assert!(v2_removes > 0, "second merge should produce remove actions");

    let has_dv_add = v2_lines.iter().any(|line| {
        serde_json::from_str::<serde_json::Value>(line)
            .ok()
            .and_then(|v| v.get("add").and_then(|a| a.get("deletionVector")).cloned())
            .is_some()
    });
    assert!(has_dv_add, "second merge should produce an add with a DV");

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

// The overwrite path reaches the checkpoint interval through its own snapshot (the one on
// `TableState`). Once the commits the checkpoint replaces are gone, the table's `protocol`
// and `metaData` live only in the checkpoint parquet — and every overwrite re-reads them to
// build its commit, so the next one cannot succeed without resolving the checkpoint.
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_overwrite_writes_checkpoint_past_interval() {
    let source_path = fixture_url("public.users/public.users.snappy.parquet");
    let table_path = fixture("delta_writes/users_checkpoint/")
        .display()
        .to_string();
    let table_url = fixture_url("delta_writes/users_checkpoint/");
    let _ = std::fs::remove_dir_all(&table_path);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    // v0 creates the table, then eleven overwrites commit v1..=v11. The eleventh builds its
    // snapshot at v10 with no checkpoint behind it, so it checkpoints v10 before committing.
    for _ in 0..11 {
        let builder = builder_from_string(source_path.clone(), rt.handle().clone())
            .await
            .unwrap();
        let schema = builder.schema().clone();
        let stream = builder
            .with_batch_size(1024)
            .build()
            .unwrap()
            .map_err(|e: parquet::errors::ParquetError| anyhow::anyhow!(e));
        overwrite_delta(&table_url, schema, stream, None, None, rt.handle())
            .await
            .unwrap();
    }

    let checkpoint = format!(
        "{}/_delta_log/00000000000000000010.checkpoint.parquet",
        table_path
    );
    assert!(
        std::path::Path::new(&checkpoint).exists(),
        "a gap of 10 versions should have written a checkpoint for v10"
    );
    let hint = format!("{}/_delta_log/_last_checkpoint", table_path);
    let hint_json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&hint).unwrap()).unwrap();
    assert_eq!(hint_json["version"], 10);

    // The table id as committed at v11, before that commit history goes away.
    let v11 = std::fs::read_to_string(format!(
        "{}/_delta_log/00000000000000000011.json",
        table_path
    ))
    .unwrap();
    let table_id = v11
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find_map(|v| v.get("metaData").map(|m| m["id"].clone()))
        .expect("v11 should carry metaData");

    for v in 0..=10 {
        std::fs::remove_file(format!("{}/_delta_log/{:020}.json", table_path, v)).unwrap();
    }

    let builder = builder_from_string(source_path, rt.handle().clone())
        .await
        .unwrap();
    let schema = builder.schema().clone();
    let stream = builder
        .with_batch_size(1024)
        .build()
        .unwrap()
        .map_err(|e: parquet::errors::ParquetError| anyhow::anyhow!(e));
    overwrite_delta(&table_url, schema, stream, None, None, rt.handle())
        .await
        .unwrap();

    let v12 = std::fs::read_to_string(format!(
        "{}/_delta_log/00000000000000000012.json",
        table_path
    ))
    .unwrap();
    let actions: Vec<serde_json::Value> = v12
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    let metadata = actions
        .iter()
        .find_map(|v| v.get("metaData"))
        .expect("v12 should carry metaData");
    assert_eq!(
        metadata["id"], table_id,
        "the table id can only have come from the checkpoint"
    );
    assert!(
        actions.iter().any(|v| v.get("remove").is_some()),
        "the overwrite should tombstone the file it replaces"
    );

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

/// The path of the single `add` in a commit.
fn added_path(log_json: &str) -> String {
    std::fs::read_to_string(log_json)
        .unwrap()
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .find_map(|v| {
            v.get("add")
                .map(|a| a["path"].as_str().unwrap().to_string())
        })
        .expect("commit should carry an add")
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_vacuum_deletes_orphans_and_keeps_referenced_files() {
    let source_path = fixture_url("public.users/public.users.snappy.parquet");
    let table_path = fixture("delta_writes/users_vacuum/").display().to_string();
    let table_url = fixture_url("delta_writes/users_vacuum/");
    let _ = std::fs::remove_dir_all(&table_path);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    // v0 creates the table, v1 adds a file, v2 tombstones it and adds another
    for _ in 0..2 {
        let builder = builder_from_string(source_path.clone(), rt.handle().clone())
            .await
            .unwrap();
        let schema = builder.schema().clone();
        let stream = builder
            .with_batch_size(1024)
            .build()
            .unwrap()
            .map_err(|e: parquet::errors::ParquetError| anyhow::anyhow!(e));
        overwrite_delta(&table_url, schema, stream, None, None, rt.handle())
            .await
            .unwrap();
    }

    let orphan = added_path(&format!(
        "{}_delta_log/00000000000000000001.json",
        table_path
    ));
    let live = added_path(&format!(
        "{}_delta_log/00000000000000000002.json",
        table_path
    ));
    assert_ne!(orphan, live);

    // The files were written seconds ago, so the seven-day default puts both inside the window.
    let protected = vacuum(&table_url, Retention::TableDefault, false, rt.handle())
        .await
        .unwrap();
    assert_eq!(
        protected.files_selected, 0,
        "retention protects an orphan that is younger than the cutoff"
    );
    assert!(
        std::path::Path::new(&format!("{}{}", table_path, orphan)).exists(),
        "the orphan survives its retention window"
    );

    // A zero window puts the cutoff at now, so the same orphan is past it.
    let dry = vacuum(
        &table_url,
        Retention::Unchecked(std::time::Duration::ZERO),
        true,
        rt.handle(),
    )
    .await
    .unwrap();
    assert_eq!(dry.files_selected, 1, "the orphan is now past the cutoff");
    assert_eq!(dry.files_deleted, 0, "a dry run deletes nothing");
    assert!(
        std::path::Path::new(&format!("{}{}", table_path, orphan)).exists(),
        "a dry run leaves the orphan in place"
    );

    let report = vacuum(
        &table_url,
        Retention::Unchecked(std::time::Duration::ZERO),
        false,
        rt.handle(),
    )
    .await
    .unwrap();

    assert_eq!(
        report.files_deleted, 1,
        "only the tombstoned file is deleted"
    );
    assert!(report.delete_errors.is_empty());
    assert!(
        !std::path::Path::new(&format!("{}{}", table_path, orphan)).exists(),
        "the tombstoned file should be gone"
    );
    assert!(
        std::path::Path::new(&format!("{}{}", table_path, live)).exists(),
        "the file the snapshot references must survive"
    );
    assert!(
        std::path::Path::new(&format!(
            "{}_delta_log/00000000000000000002.json",
            table_path
        ))
        .exists(),
        "the log is skipped, not swept"
    );

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_vacuum_refuses_retention_under_the_table_floor() {
    let source_path = fixture_url("public.users/public.users.snappy.parquet");
    let table_path = fixture("delta_writes/users_vacuum_floor/")
        .display()
        .to_string();
    let table_url = fixture_url("delta_writes/users_vacuum_floor/");
    let _ = std::fs::remove_dir_all(&table_path);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let builder = builder_from_string(source_path, rt.handle().clone())
        .await
        .unwrap();
    let schema = builder.schema().clone();
    let stream = builder
        .with_batch_size(1024)
        .build()
        .unwrap()
        .map_err(|e: parquet::errors::ParquetError| anyhow::anyhow!(e));
    overwrite_delta(&table_url, schema, stream, None, None, rt.handle())
        .await
        .unwrap();

    let err = vacuum(
        &table_url,
        Retention::At(std::time::Duration::from_secs(60)),
        false,
        rt.handle(),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("shorter than the table's"),
        "got: {err}"
    );

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_overwrite_preserves_table_properties() {
    // The merge turns on deletion vectors in `metaData.configuration`; the overwrite that follows
    // rewrites `metaData`, and has to carry that configuration forward rather than replace it.
    let merge_config = r#"
src: file
dest: delta.merge
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"

tables:
  - name: public.numbers
    delta.merge_keys: [bigint_value]
"#;
    let overwrite_config = r#"
src: file
dest: delta.overwrite
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"

tables:
  - name: public.numbers
"#;

    let dest_url = fixture_url("delta_writes/config_delta_properties_root/");
    let delta_root = fixture("delta_writes/config_delta_properties_root/")
        .display()
        .to_string();
    let table_path = format!("{}/public.numbers/", delta_root);
    let _ = std::fs::remove_dir_all(&table_path);

    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), data_url()),
        ("LDRS_DEST".to_string(), dest_url),
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    for config in [merge_config, overwrite_config] {
        execute_configs(
            parse_yaml_config(config, &ldrs_env).unwrap(),
            None,
            &ldrs_env,
            rt.handle(),
        )
        .await
        .unwrap();
    }

    let configuration_at = |version: u64| -> serde_json::Value {
        std::fs::read_to_string(format!("{}_delta_log/{:020}.json", table_path, version))
            .unwrap()
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .find_map(|v| v.get("metaData").map(|m| m["configuration"].clone()))
            .expect("commit should carry metaData")
    };

    assert_eq!(
        configuration_at(1)["delta.enableDeletionVectors"],
        "true",
        "the merge should have enabled deletion vectors at v1"
    );
    assert_eq!(
        configuration_at(2)["delta.enableDeletionVectors"],
        "true",
        "the overwrite at v2 must not drop the property the merge set"
    );

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn test_overwrite_preserves_partition_columns() {
    // ldrs never writes a partitioned table, so the partitioning is introduced by hand: v0 comes
    // from an overwrite, v1 restates its metaData with `partitionColumns`, and the overwrite at v2
    // has to carry that forward.
    let config = r#"
src: file
dest: delta.overwrite
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"

tables:
  - name: public.numbers
"#;

    let dest_url = fixture_url("delta_writes/config_delta_partitions_root/");
    let delta_root = fixture("delta_writes/config_delta_partitions_root/")
        .display()
        .to_string();
    let table_path = format!("{}/public.numbers/", delta_root);
    let _ = std::fs::remove_dir_all(&table_path);

    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), data_url()),
        ("LDRS_DEST".to_string(), dest_url),
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let run = async || {
        execute_configs(
            parse_yaml_config(config, &ldrs_env).unwrap(),
            None,
            &ldrs_env,
            rt.handle(),
        )
        .await
        .unwrap();
    };

    let commit_path = |version: u64| format!("{}_delta_log/{:020}.json", table_path, version);
    let metadata_at = |version: u64| -> serde_json::Value {
        std::fs::read_to_string(commit_path(version))
            .unwrap()
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .find_map(|v| v.get("metaData").cloned())
            .expect("commit should carry metaData")
    };

    run().await;
    assert_eq!(
        metadata_at(0)["partitionColumns"],
        serde_json::json!([]),
        "ldrs creates unpartitioned tables"
    );

    let mut partitioned = metadata_at(0);
    partitioned["partitionColumns"] = serde_json::json!(["bigint_value"]);
    std::fs::write(
        commit_path(1),
        format!("{}\n", serde_json::json!({ "metaData": partitioned })),
    )
    .unwrap();

    run().await;
    assert_eq!(
        metadata_at(2)["partitionColumns"],
        serde_json::json!(["bigint_value"]),
        "the overwrite at v2 must not drop the partitioning named at v1"
    );

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[test]
fn test_resolve_delta_targets_picks_delta_destinations_only() {
    // A pq destination sits alongside the delta one; resolving it would need its own env, so the
    // filter has to happen before resolution rather than after.
    let config = r#"
version: 2
src: file
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"

tables:
  - name: public.numbers
    destinations:
      - dest: delta.overwrite
      - dest: pq
        filename: "out.parquet"
  - name: public.users
    destinations:
      - dest: delta.overwrite
"#;

    let dest_url = fixture_url("delta_writes/resolve_targets_root/");
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), data_url()),
        ("LDRS_DEST_DELTA".to_string(), dest_url.clone()),
    ];

    let targets = resolve_delta_targets(
        parse_yaml_config(config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
    )
    .unwrap();

    let names: Vec<&str> = targets.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(names, vec!["public.numbers", "public.users"]);
    assert!(
        targets
            .iter()
            .all(|(_, path)| path.starts_with(dest_url.trim_end_matches('/'))),
        "every target resolves under the delta root: {targets:?}"
    );

    let selected = resolve_delta_targets(
        parse_yaml_config(config, &ldrs_env).unwrap(),
        Some(vec!["public.users".to_string()]),
        &ldrs_env,
    )
    .unwrap();
    assert_eq!(selected.len(), 1, "--select narrows to one table");
    assert_eq!(selected[0].0, "public.users");
}

#[test]
fn test_resolve_delta_targets_errors_without_a_delta_destination() {
    let config = r#"
src: file
dest: pq
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"

tables:
  - name: public.numbers
    filename: "out.parquet"
"#;
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), data_url()),
        ("LDRS_DEST".to_string(), fixture_url("delta_writes/unused/")),
    ];

    let err = resolve_delta_targets(
        parse_yaml_config(config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
    )
    .unwrap_err()
    .to_string();
    assert!(err.contains("no delta destinations"), "{err}");
}

#[test]
fn test_resolve_delta_targets_ignores_load_only_fields() {
    // batch_version renders against env the orchestrator sets during a load. Vacuum runs out of
    // band without it, and never reads the field, so resolution must not touch it.
    let config = r#"
src: file
dest: delta.merge
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"

tables:
  - name: public.numbers
    delta.merge_keys: [bigint_value]
    delta.txn_mode: processing_time
    delta.batch_version: "{{ run_id }}"
"#;
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), data_url()),
        (
            "LDRS_DEST".to_string(),
            fixture_url("delta_writes/load_only_fields_root/"),
        ),
    ];

    let targets = resolve_delta_targets(
        parse_yaml_config(config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
    )
    .unwrap();
    assert_eq!(targets.len(), 1);
    assert!(targets[0].1.ends_with("public.numbers"), "{targets:?}");
}
