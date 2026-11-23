use arrow_schema::DataType;
use ldrs::{
    ldrs_config::{execute_configs, infer_env_type, parse_tables},
    results::Results,
};
use ldrs_parquet::builder_from_string;
use ldrs_test_fixtures::{data_url, fixture_str, fixture_url};

#[tokio::test]
#[test_log::test]
async fn test_parquet_full_round_trip() {
    let config = r#"
src: file
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"
destinations:
  - dest: pq
    filename: parquet_writes/{{ target }}_roundtrip.snappy.parquet

tables:
  - name: public.users
    columns: [ { name: created, type: timestamptz, time_unit: Micros }]
    destinations:
      - dest: pq
        target: public.users_renamed
        filename: parquet_writes/{{ target }}_roundtrip.snappy.parquet
        bloom_filters: [[unique_id]]
  - name: public.numbers
"#;

    let file_url = data_url();
    let pq_url = data_url();
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), file_url.to_string()),
        ("LDRS_DEST".to_string(), pq_url.to_string()),
    ];
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let expected_files = vec![
        // `users` set an explicit `target`; `numbers` defaults to `name`
        fixture_str("parquet_writes/public.users_renamed_roundtrip.snappy.parquet"),
        fixture_str("parquet_writes/public.numbers_roundtrip.snappy.parquet"),
    ];
    // delete before the test
    for file in &expected_files {
        let _ = std::fs::remove_file(file);
    }
    execute_configs(
        parse_tables(
            &serde_yaml::from_str(&config).unwrap(),
            infer_env_type("LDRS_SRC", &ldrs_env),
        )
        .unwrap(),
        None,
        &ldrs_env,
        rt.handle(),
        &Results::default(),
    )
    .await
    .unwrap();
    // ensure the expected files exist
    for file in &expected_files {
        assert!(
            std::path::Path::new(file).exists(),
            "expected file not found: {}",
            file
        );
    }
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

/// A results file that cannot be written after a destination committed is a partial run, exit 3.
#[tokio::test]
#[test_log::test]
async fn test_results_write_failure_after_commit_is_partial() {
    let config = r#"
src: file
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"
destinations:
  - dest: pq
    filename: parquet_writes/{{ target }}_results_failure.snappy.parquet
tables:
  - name: public.numbers
"#;
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), data_url()),
        ("LDRS_DEST".to_string(), data_url()),
    ];
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    // Opens fine; every write fails with ENOSPC.
    let results = Results::create(Some("/dev/full")).unwrap();

    let err = execute_configs(
        parse_tables(
            &serde_yaml::from_str(config).unwrap(),
            infer_env_type("LDRS_SRC", &ldrs_env),
        )
        .unwrap(),
        None,
        &ldrs_env,
        rt.handle(),
        &results,
    )
    .await
    .expect_err("an unwritable results file must fail the run");
    assert_eq!(err.code(), 3, "got: {err}");
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

/// A relative `file://` dest with a relative filename resolves under the working directory,
/// not the filesystem root. Drives config -> ParquetSink -> object_store write and asserts
/// the file lands cwd-relative.
#[tokio::test]
#[test_log::test]
async fn test_parquet_relative_file_dest() {
    let config = r#"
src: file
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"
destinations:
  - dest: pq
    filename: tests/test_data/parquet_writes/{{ name }}_relative.snappy.parquet

tables:
  - name: public.users
"#;
    let ldrs_env = vec![
        // source is absolute (not under test); dest is the bare relative form
        ("LDRS_SRC".to_string(), data_url()),
        ("LDRS_DEST".to_string(), "file://".to_string()),
    ];
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    // the file must land relative to the working directory, proving `file://` did not
    // resolve to filesystem root
    let expected = std::env::current_dir()
        .unwrap()
        .join("tests/test_data/parquet_writes/public.users_relative.snappy.parquet");
    let _ = std::fs::remove_file(&expected);
    execute_configs(
        parse_tables(
            &serde_yaml::from_str(&config).unwrap(),
            infer_env_type("LDRS_SRC", &ldrs_env),
        )
        .unwrap(),
        None,
        &ldrs_env,
        rt.handle(),
        &Results::default(),
    )
    .await
    .unwrap();
    assert!(
        expected.exists(),
        "relative file:// dest should write under cwd, missing: {:?}",
        expected
    );
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

/// `max_rows` + a `{{ pad index N }}` filename through `execute_configs`: the run succeeds
/// and the first rotated file is written with the zero-padded index.
#[tokio::test]
#[test_log::test]
async fn test_parquet_rotation_namer_wired() {
    let config = r#"
src: file
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"
destinations:
  - dest: pq
    filename: parquet_writes/{{ name }}_rot_{{ pad index 5 }}.snappy.parquet
    max_rows: 1

tables:
  - name: public.users
"#;
    let url = data_url();
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), url.to_string()),
        ("LDRS_DEST".to_string(), url.to_string()),
    ];
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let expected = fixture_str("parquet_writes/public.users_rot_00000.snappy.parquet");
    let _ = std::fs::remove_file(&expected);
    execute_configs(
        parse_tables(
            &serde_yaml::from_str(&config).unwrap(),
            infer_env_type("LDRS_SRC", &ldrs_env),
        )
        .unwrap(),
        None,
        &ldrs_env,
        rt.handle(),
        &Results::default(),
    )
    .await
    .unwrap();
    assert!(
        std::path::Path::new(&expected).exists(),
        "rotation namer should render the zero-padded index 0, missing: {}",
        expected
    );
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

/// Seam 1: one source fans out to multiple destinations
#[tokio::test]
#[test_log::test]
async fn test_fanout_writes_to_all_destinations() {
    let config = r#"
src: file
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"
destinations:
  - dest: pq
    filename: parquet_writes/{{ name }}_fanout_a.snappy.parquet
  - dest: pq
    filename: parquet_writes/{{ name }}_fanout_b.snappy.parquet
tables:
  - name: public.users
"#;
    let url = data_url();
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), url.to_string()),
        ("LDRS_DEST".to_string(), url.to_string()),
    ];
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let expected = vec![
        fixture_str("parquet_writes/public.users_fanout_a.snappy.parquet"),
        fixture_str("parquet_writes/public.users_fanout_b.snappy.parquet"),
    ];
    for f in &expected {
        let _ = std::fs::remove_file(f);
    }
    execute_configs(
        parse_tables(
            &serde_yaml::from_str(&config).unwrap(),
            infer_env_type("LDRS_SRC", &ldrs_env),
        )
        .unwrap(),
        None,
        &ldrs_env,
        rt.handle(),
        &Results::default(),
    )
    .await
    .unwrap();
    for f in &expected {
        assert!(
            std::path::Path::new(f).exists(),
            "fan-out did not write all destinations, missing: {}",
            f
        );
    }
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

/// Seam 2: a mid-stream failure
#[tokio::test]
#[test_log::test]
async fn test_fanout_aborts_all_on_midstream_failure() {
    let config = r#"
src: file
src_defaults:
  filename: "{{ name }}/public.strings.snappy.parquet"
destinations:
  - dest: pq
    filename: parquet_writes/{{ name }}_abort_a.snappy.parquet
  - dest: pq
    filename: parquet_writes/{{ name }}_abort_b.snappy.parquet
tables:
  - name: public.string_values
    columns:
      - { name: text_value, type: uuid }
"#;
    let url = data_url();
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), url.to_string()),
        ("LDRS_DEST".to_string(), url.to_string()),
    ];
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let expected = vec![
        fixture_str("parquet_writes/public.string_values_abort_a.snappy.parquet"),
        fixture_str("parquet_writes/public.string_values_abort_b.snappy.parquet"),
    ];
    for f in &expected {
        let _ = std::fs::remove_file(f);
    }
    let result = execute_configs(
        parse_tables(
            &serde_yaml::from_str(&config).unwrap(),
            infer_env_type("LDRS_SRC", &ldrs_env),
        )
        .unwrap(),
        None,
        &ldrs_env,
        rt.handle(),
        &Results::default(),
    )
    .await;
    assert!(
        result.is_err(),
        "expected the bad-UUID transform to fail the run"
    );
    for f in &expected {
        assert!(
            !std::path::Path::new(f).exists(),
            "abort should leave no committed output, but found: {}",
            f
        );
    }
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

/// Shared-transform success: two destinations with the same columns resolve one shared transform
#[tokio::test]
#[test_log::test]
async fn test_fanout_shared_transform_applied_to_all() {
    let config = r#"
src: file
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"
destinations:
  - dest: pq
    filename: parquet_writes/{{ name }}_shared_a.snappy.parquet
  - dest: pq
    filename: parquet_writes/{{ name }}_shared_b.snappy.parquet
tables:
  - name: public.numbers
    columns:
      - { type: bigint, name: integer_value }
"#;
    let url = data_url();
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), url.to_string()),
        ("LDRS_DEST".to_string(), url.to_string()),
    ];
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let outputs = [
        "parquet_writes/public.numbers_shared_a.snappy.parquet",
        "parquet_writes/public.numbers_shared_b.snappy.parquet",
    ];
    for f in outputs {
        let _ = std::fs::remove_file(fixture_str(f));
    }
    execute_configs(
        parse_tables(
            &serde_yaml::from_str(config).unwrap(),
            infer_env_type("LDRS_SRC", &ldrs_env),
        )
        .unwrap(),
        None,
        &ldrs_env,
        rt.handle(),
        &Results::default(),
    )
    .await
    .unwrap();
    for f in outputs {
        let builder = builder_from_string(fixture_url(f), rt.handle().clone())
            .await
            .unwrap();
        let dt = builder
            .schema()
            .field_with_name("integer_value")
            .unwrap()
            .data_type()
            .clone();
        assert_eq!(
            dt,
            DataType::Int64,
            "shared transform should cast integer_value to Int64 in {}",
            f
        );
    }
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

/// Per-destination transforms
#[tokio::test]
#[test_log::test]
async fn test_fanout_per_destination_transforms() {
    let config = r#"
src: file
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"
destinations:
  - dest: pq
    filename: parquet_writes/{{ name }}_perdest_a.snappy.parquet
    columns:
      - { type: bigint, name: integer_value }
  - dest: pq
    filename: parquet_writes/{{ name }}_perdest_b.snappy.parquet
    columns:
      - { type: double, name: integer_value }
tables:
  - name: public.numbers
"#;
    let url = data_url();
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), url.to_string()),
        ("LDRS_DEST".to_string(), url.to_string()),
    ];
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let file_a = "parquet_writes/public.numbers_perdest_a.snappy.parquet";
    let file_b = "parquet_writes/public.numbers_perdest_b.snappy.parquet";
    let _ = std::fs::remove_file(fixture_str(file_a));
    let _ = std::fs::remove_file(fixture_str(file_b));
    execute_configs(
        parse_tables(
            &serde_yaml::from_str(config).unwrap(),
            infer_env_type("LDRS_SRC", &ldrs_env),
        )
        .unwrap(),
        None,
        &ldrs_env,
        rt.handle(),
        &Results::default(),
    )
    .await
    .unwrap();

    let a = builder_from_string(fixture_url(file_a), rt.handle().clone())
        .await
        .unwrap();
    let a_type = a
        .schema()
        .field_with_name("integer_value")
        .unwrap()
        .data_type()
        .clone();
    assert_eq!(a_type, DataType::Int64, "A casts integer_value to Int64");
    let b = builder_from_string(fixture_url(file_b), rt.handle().clone())
        .await
        .unwrap();
    let b_type = b
        .schema()
        .field_with_name("integer_value")
        .unwrap()
        .data_type()
        .clone();
    assert_eq!(
        b_type,
        DataType::Float64,
        "B casts integer_value to Float64"
    );
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}
