use delta_kernel::expressions::Scalar;
use futures::TryStreamExt;
use ldrs::ldrs_config::create_ldrs_exec;
use ldrs::ldrs_delta::{delta_stats_to_json, overwrite_delta, parquet_metadata_to_delta_stats};
use ldrs::parquet_provider;

#[tokio::test]
async fn test_delta_stats_from_users_parquet() {
    let cd = std::env::current_dir().unwrap();
    let path = format!(
        "{}/tests/test_data/public.users/public.users.snappy.parquet",
        cd.display()
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let builder = parquet_provider::builder_from_string(path, rt.handle().clone())
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
    let cd = std::env::current_dir().unwrap();
    let path = format!(
        "{}/tests/test_data/public.string_values/public.strings.snappy.parquet",
        cd.display()
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let builder = parquet_provider::builder_from_string(path, rt.handle().clone())
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
    let cd = std::env::current_dir().unwrap();
    let path = format!(
        "{}/tests/test_data/public.numbers/public.numbers.snappy.parquet",
        cd.display()
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let builder = parquet_provider::builder_from_string(path, rt.handle().clone())
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
    let cd = std::env::current_dir().unwrap();
    let path = format!(
        "{}/tests/test_data/public.users/public.users.snappy.parquet",
        cd.display()
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let builder = parquet_provider::builder_from_string(path, rt.handle().clone())
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
    let cd = std::env::current_dir().unwrap();
    let path = format!(
        "{}/tests/test_data/public.numbers/public.numbers.snappy.parquet",
        cd.display()
    );
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let builder = parquet_provider::builder_from_string(path, rt.handle().clone())
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

#[tokio::test]
async fn test_overwrite_delta() {
    let cd = std::env::current_dir().unwrap();
    let source_path = format!(
        "{}/tests/test_data/public.users/public.users.snappy.parquet",
        cd.display()
    );

    let table_path = format!("{}/tests/test_data/delta_writes/users_delta/", cd.display());
    let _ = std::fs::remove_dir_all(&table_path);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let builder = parquet_provider::builder_from_string(source_path.clone(), rt.handle().clone())
        .await
        .unwrap();
    let schema = builder.schema().clone();
    let stream = builder
        .with_batch_size(1024)
        .build()
        .unwrap()
        .map_err(|e: parquet::errors::ParquetError| anyhow::anyhow!(e));

    let transforms = vec![None; schema.fields().len()];
    overwrite_delta(&table_path, schema.clone(), transforms, stream, None, None)
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

    let builder2 = parquet_provider::builder_from_string(source_path, rt.handle().clone())
        .await
        .unwrap();
    let stream2 = builder2
        .with_batch_size(1024)
        .build()
        .unwrap()
        .map_err(|e: parquet::errors::ParquetError| anyhow::anyhow!(e));

    let transforms2 = vec![None; schema.fields().len()];
    overwrite_delta(&table_path, schema, transforms2, stream2, None, None)
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

#[tokio::test]
#[test_log::test]
async fn test_delta_overwrite_with_config() {
    let cd = std::env::current_dir().unwrap();

    let config = r#"
src: file
dest: delta.overwrite
src_defaults:
  filename: tests/test_data/{{ name }}/{{ name }}.snappy.parquet

tables:
  - name: public.users
  - name: public.numbers
  - name: public.string_values
    filename: tests/test_data/public.string_values/public.strings.snappy.parquet
"#;

    let src_url = "file://".to_string();
    let delta_root = format!(
        "{}/tests/test_data/delta_writes/config_delta_root",
        cd.display()
    );
    let dest_url = format!("file://{}/", delta_root);

    let table_names = ["public.users", "public.numbers", "public.string_values"];

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

    create_ldrs_exec(config, &ldrs_env, None, &rt.handle())
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
