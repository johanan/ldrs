use futures::TryStreamExt;
use ldrs::{
    ldrs_config::{
        config::{get_parsed_config, LdrsDestination, LdrsParsedConfig, LdrsSource},
        create_ldrs_exec,
    },
    ldrs_parquet::write_parquet,
    ldrs_snowflake::snowflake_source::{SFQuery, SFSource},
    ldrs_storage::{FileSource, ParquetDestination},
    parquet_provider::{self},
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
        write_parquet(&file_path, schema, Vec::new(), stream)
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
    let _ = create_ldrs_exec(config, &ldrs_env, &rt.handle())
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
