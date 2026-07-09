use std::sync::Arc;

use arrow_array::{Int32Array, Int8Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use ldrs::{
    file_source::FileSource,
    ldrs_config::{
        config::{parse_dest, parse_src, LdrsDestination, LdrsParsedConfig, LdrsSource},
        execute_configs, parse_yaml_config,
    },
    ldrs_snowflake::snowflake_source::{SFQuery, SFSource},
    parquet::ParquetDestination,
};
use ldrs_arrow::{build_arrow_transform_strategy, ColumnType};
use ldrs_parquet::{builder_from_string, write_parquet, write_parquet_split};
use ldrs_test_fixtures::{data_url, fixture, fixture_str, fixture_url};
use serde_yaml::Value;
use tracing::info;

const TEST_CASES: &[(&str, &str)] = &[
    (
        "public.users/public.users.snappy.parquet",
        "public.users.written.snappy.parquet",
    ),
    (
        "public.string_values/public.strings.snappy.parquet",
        "public.strings.written.snappy.parquet",
    ),
    (
        "public.numbers/public.numbers.snappy.parquet",
        "public.numbers.written.snappy.parquet",
    ),
];

#[tokio::test]
#[test_log::test]
async fn test_parquet() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    for case in TEST_CASES {
        let (source_path, target_path) = case;
        let path = fixture(source_path).display().to_string();
        let builder = builder_from_string(path.clone(), rt.handle().clone())
            .await
            .unwrap();
        let schema = builder.schema().clone();
        let stream = builder
            .with_batch_size(1024)
            .build()
            .unwrap()
            .map_err(|e| e.into());
        let file_path = fixture(format!("parquet_writes/{}", target_path).as_str())
            .display()
            .to_string();
        info!("schema: {:?}", schema);
        write_parquet(&file_path, schema, Vec::new(), None, stream)
            .await
            .unwrap();
    }

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

/// filename for destination will need to use the pq namespace to make sure that the src does not parse it
#[test_log::test]
fn test_parquet_file_src_and_dest() {
    let test_yaml = r#"
name: public.users
pq.filename: tests/test_data/parquet_writes/public.users.written.snappy.parquet
"#;

    let test_value: Value = serde_yaml::from_str(test_yaml).unwrap();
    let src = parse_src(test_value.clone(), &Some("file".into())).unwrap();
    let dest = parse_dest(test_value, &Some("pq".into())).unwrap();
    let config = LdrsParsedConfig {
        src,
        dests: vec![dest],
        finalize: Vec::new(),
        unknown_keys: Vec::new(),
    };
    let expected_config = LdrsParsedConfig {
        src: LdrsSource::File(FileSource {
            name: "public.users".into(),
            filename: None,
        }),
        dests: vec![LdrsDestination::Pq(ParquetDestination {
            name: "public.users".into(),
            target: None,
            filename: "tests/test_data/parquet_writes/public.users.written.snappy.parquet".into(),
            columns: Vec::new(),
            bloom_filters: Vec::new(),
            max_rows: None,
            max_bytes: None,
        })],
        finalize: Vec::new(),
        unknown_keys: Vec::new(),
    };
    assert_eq!(config, expected_config);

    // when the source is not a file we can use filename
    let sf_yaml = r#"
name: public.users
sql: select * from users
filename: tests/test_data/parquet_writes/public.users.written.snappy.parquet
"#;

    let test_value: Value = serde_yaml::from_str(sf_yaml).unwrap();
    let src = parse_src(test_value.clone(), &Some("sf".into())).unwrap();
    let dest = parse_dest(test_value, &Some("pq".into())).unwrap();
    let config = LdrsParsedConfig {
        src,
        dests: vec![dest],
        finalize: Vec::new(),
        unknown_keys: Vec::new(),
    };
    let expected_config = LdrsParsedConfig {
        src: LdrsSource::SF(SFSource::Query(SFQuery {
            name: "public.users".into(),
            sql: "select * from users".into(),
            param_keys: None,
        })),
        dests: vec![LdrsDestination::Pq(ParquetDestination {
            name: "public.users".into(),
            target: None,
            filename: "tests/test_data/parquet_writes/public.users.written.snappy.parquet".into(),
            columns: Vec::new(),
            bloom_filters: Vec::new(),
            max_rows: None,
            max_bytes: None,
        })],
        finalize: Vec::new(),
        unknown_keys: Vec::new(),
    };
    assert_eq!(config, expected_config);
}

#[tokio::test]
#[test_log::test]
async fn test_parquet_full_round_trip() {
    let config = r#"
src: file
dest: pq
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"
dest_defaults:
  pq.filename: parquet_writes/{{ target }}_roundtrip.snappy.parquet

tables:
  - name: public.users
    target: public.users_renamed
    bloom_filters: [[unique_id]]
    columns: [ { name: created, type: timestamptz, time_unit: Micros }]
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
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        &rt.handle(),
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

/// A relative `file://` dest with a relative filename resolves under the working directory,
/// not the filesystem root. Drives config -> ParquetSink -> object_store write and asserts
/// the file lands cwd-relative.
#[tokio::test]
#[test_log::test]
async fn test_parquet_relative_file_dest() {
    let config = r#"
src: file
dest: pq
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"
dest_defaults:
  pq.filename: tests/test_data/parquet_writes/{{ name }}_relative.snappy.parquet

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
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        &rt.handle(),
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

/// `pq.max_rows` + a `{{ pad index N }}` filename through `execute_configs`: the run succeeds
/// and the first rotated file is written with the zero-padded index.
#[tokio::test]
#[test_log::test]
async fn test_parquet_rotation_namer_wired() {
    let config = r#"
src: file
dest: pq
src_defaults:
  filename: "{{ name }}/{{ name }}.snappy.parquet"
dest_defaults:
  pq.filename: parquet_writes/{{ name }}_rot_{{ pad index 5 }}.snappy.parquet
  pq.max_rows: 1

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
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        &rt.handle(),
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

/// Snowflake sends DECIMAL(1,0) as Int8.
/// and parquet did not like the transform that came across
#[tokio::test]
#[test_log::test]
async fn test_parquet_numeric_precision1_scale0() {
    let source_logical = ColumnType::Numeric(1, 0);
    let target_logical = ColumnType::Numeric(1, 0);
    let source_physical = DataType::Int8;

    let strategy =
        build_arrow_transform_strategy(source_logical, target_logical.clone(), &source_physical)
            .unwrap();
    info!("strategy: {:?}", strategy);
    let target_physical = target_logical.to_arrow_datatype();
    let source_schema = Arc::new(Schema::new(vec![Field::new("flag", source_physical, true)]));
    let target_schema = Arc::new(Schema::new(vec![Field::new("flag", target_physical, true)]));

    let batch = RecordBatch::try_new(
        source_schema,
        vec![Arc::new(Int8Array::from(vec![Some(1), Some(0), None]))],
    )
    .unwrap();

    let cd = std::env::current_dir().unwrap();
    let file_path = format!(
        "{}/tests/test_data/parquet_writes/decimal32_test.snappy.parquet",
        cd.display()
    );

    let stream = futures::stream::iter(vec![Ok(batch)]);
    write_parquet(&file_path, target_schema, vec![strategy], None, stream)
        .await
        .unwrap();
}

fn split_test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn split_test_batches(schema: &SchemaRef) -> (RecordBatch, RecordBatch) {
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
        ],
    )
    .unwrap();

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["dave", "eve"])),
        ],
    )
    .unwrap();

    (batch1, batch2)
}

#[tokio::test]
#[test_log::test]
async fn test_parquet_split_by_rows() {
    let schema = split_test_schema();
    let (batch1, batch2) = split_test_batches(&schema);

    let cd = std::env::current_dir().unwrap();
    let dir_path = format!(
        "{}/tests/test_data/parquet_writes/split_test/",
        cd.display()
    );

    let _ = std::fs::remove_dir_all(&dir_path);
    std::fs::create_dir_all(&dir_path).unwrap();

    let stream = futures::stream::iter(vec![Ok(batch1), Ok(batch2)]);
    let results = write_parquet_split(
        &dir_path,
        schema,
        Vec::new(),
        stream,
        Some(2),
        None,
        |i| format!("part_{:05}.parquet", i),
        None,
    )
    .await
    .unwrap();

    assert_eq!(results.len(), 2, "expected 2 files from split");
    assert_eq!(results[0].0, "part_00000.parquet");
    assert_eq!(results[1].0, "part_00001.parquet");

    // verify files exist on disk
    for (filename, _metadata) in &results {
        let full = format!("{}{}", dir_path, filename);
        assert!(
            std::path::Path::new(&full).exists(),
            "expected file not found: {}",
            full
        );
    }
}

#[tokio::test]
#[test_log::test]
async fn test_parquet_split_by_size() {
    let schema = split_test_schema();
    let (batch1, batch2) = split_test_batches(&schema);

    let cd = std::env::current_dir().unwrap();
    let dir_path = format!(
        "{}/tests/test_data/parquet_writes/split_size_test/",
        cd.display()
    );

    let _ = std::fs::remove_dir_all(&dir_path);
    std::fs::create_dir_all(&dir_path).unwrap();

    let stream = futures::stream::iter(vec![Ok(batch1), Ok(batch2)]);
    let results = write_parquet_split(
        &dir_path,
        schema,
        Vec::new(),
        stream,
        None,
        Some(1),
        |i| format!("size_{:05}.parquet", i),
        None,
    )
    .await
    .unwrap();

    assert!(
        results.len() >= 2,
        "expected at least 2 files from size split, got {}",
        results.len()
    );

    for (filename, _metadata) in &results {
        let full = format!("{}{}", dir_path, filename);
        assert!(
            std::path::Path::new(&full).exists(),
            "expected file not found: {}",
            full
        );
    }
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
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        &rt.handle(),
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
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        &rt.handle(),
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
        parse_yaml_config(config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        &rt.handle(),
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
        parse_yaml_config(config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        &rt.handle(),
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
