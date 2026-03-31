use std::sync::Arc;

use arrow_array::{Int32Array, Int8Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::TryStreamExt;
use ldrs::{
    arrow_access::arrow_transforms::build_arrow_transform_strategy,
    ldrs_config::{
        config::{get_parsed_config, LdrsDestination, LdrsParsedConfig, LdrsSource},
        create_ldrs_exec,
    },
    ldrs_parquet::{write_parquet, write_parquet_split},
    ldrs_snowflake::snowflake_source::{SFQuery, SFSource},
    ldrs_storage::{FileSource, ParquetDestination},
    parquet_provider::{self},
    types::ColumnType,
};
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
    let cd = std::env::current_dir().unwrap();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    for case in TEST_CASES {
        let (source_path, target_path) = case;
        let path = format!("{}/tests/test_data/{}", cd.display(), source_path);
        let builder = parquet_provider::builder_from_string(path.clone(), rt.handle().clone())
            .await
            .unwrap();
        let schema = builder.schema().clone();
        let stream = builder
            .with_batch_size(1024)
            .build()
            .unwrap()
            .map_err(|e| e.into());
        let file_path = format!(
            "{}/tests/test_data/parquet_writes/{}",
            cd.display(),
            target_path
        );
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

    let test_value = serde_yaml::from_str(test_yaml).unwrap();
    let config = get_parsed_config(
        &Some("file".into()),
        &Some("pq".into()),
        &None,
        &None,
        test_value,
    )
    .unwrap();
    let expected_config = LdrsParsedConfig {
        src: LdrsSource::File(FileSource {
            name: "public.users".into(),
            filename: None,
        }),
        src_prefix: "file".into(),
        dest: LdrsDestination::Pq(ParquetDestination {
            name: "public.users".into(),
            filename: "tests/test_data/parquet_writes/public.users.written.snappy.parquet".into(),
            columns: Vec::new(),
            bloom_filters: Vec::new(),
        }),
        dest_prefix: "pq".into(),
    };
    assert_eq!(config, expected_config);

    // when the source is not a file we can use filename
    let sf_yaml = r#"
name: public.users
sql: select * from users
filename: tests/test_data/parquet_writes/public.users.written.snappy.parquet
"#;

    let test_value = serde_yaml::from_str(sf_yaml).unwrap();
    let config = get_parsed_config(
        &Some("sf".into()),
        &Some("pq".into()),
        &None,
        &None,
        test_value,
    )
    .unwrap();
    let expected_config = LdrsParsedConfig {
        src: LdrsSource::SF(SFSource::Query(SFQuery {
            name: "public.users".into(),
            sql: "select * from users".into(),
            param_keys: None,
        })),
        src_prefix: "sf".into(),
        dest: LdrsDestination::Pq(ParquetDestination {
            name: "public.users".into(),
            filename: "tests/test_data/parquet_writes/public.users.written.snappy.parquet".into(),
            columns: Vec::new(),
            bloom_filters: Vec::new(),
        }),
        dest_prefix: "pq".into(),
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
  filename: tests/test_data/{{ name }}/{{ name }}.snappy.parquet
dest_defaults:
  pq.filename: tests/test_data/parquet_writes/{{ name }}_roundtrip.snappy.parquet

tables:
  - name: public.users
    bloom_filters: [[unique_id]]
    columns: [ { name: created, type: timestamptz, time_unit: Micros }]
  - name: public.numbers
"#;

    let file_url = "file://";
    let pq_url = "file://";
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
        "tests/test_data/parquet_writes/public.users_roundtrip.snappy.parquet",
        "tests/test_data/parquet_writes/public.numbers_roundtrip.snappy.parquet",
    ];
    // delete before the test
    for file in &expected_files {
        let _ = std::fs::remove_file(file);
    }
    let _ = create_ldrs_exec(config, &ldrs_env, None, &rt.handle())
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
