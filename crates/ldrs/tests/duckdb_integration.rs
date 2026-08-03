//! The duckdb source end to end: config -> spawned duckdb -> Arrow IPC -> destination.
//!
//! Requires `duckdb` on PATH with the `nanoarrow` extension; run `scripts/setup_duckdb.sh` first.
//! Source data is the Palmer Penguins fixture in `test_data/duckdb` (see its README).

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal64Array, FixedSizeBinaryArray,
    Float64Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use futures::TryStreamExt;
use ldrs::ldrs_config::{execute_configs, parse_yaml_config};
use ldrs_parquet::builder_from_string;
use ldrs_test_fixtures::{data_url, fixture, fixture_url};

fn cloud_io() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap()
}

/// `file://…/test_data/duckdb/`, the directory the queries glob over.
fn penguins_url() -> String {
    format!("{}duckdb/", data_url())
}

async fn run(config: &str, ldrs_env: &[(String, String)]) -> Result<(), anyhow::Error> {
    let rt = cloud_io();
    let result = execute_configs(
        parse_yaml_config(config, ldrs_env).unwrap(),
        None,
        ldrs_env,
        &rt.handle(),
    )
    .await;
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
    result
}

/// Read a written parquet file back as one concatenated set of batches.
async fn read_back(relative: &str) -> (arrow_schema::SchemaRef, Vec<arrow_array::RecordBatch>) {
    let rt = cloud_io();
    let builder = builder_from_string(fixture_url(relative), rt.handle().clone())
        .await
        .unwrap();
    let schema = builder.schema().clone();
    let batches: Vec<_> = builder.build().unwrap().try_collect().await.unwrap();
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
    (schema, batches)
}

/// Globs three CSVs, projects six of seventeen columns, and relies on `nullstr='NA'` to turn the
/// literal `NA` markers into real nulls and the numeric columns into numeric types.
#[tokio::test]
#[test_log::test]
async fn duckdb_query_globs_csvs_into_parquet() {
    let out = "duckdb_writes/penguins.parquet";
    let _ = std::fs::remove_file(fixture(out));

    let config = r#"
version: 2
src: duckdb.query
destinations:
  - dest: pq
    filename: duckdb_writes/penguins.parquet
tables:
  - name: penguins
    sql: |
      SELECT
        "Individual ID"     AS individual_id,
        Island              AS island,
        "Date Egg"          AS date_egg,
        "Body Mass (g)"     AS body_mass_g,
        "Delta 15 N (o/oo)" AS delta_15_n,
        Sex                 AS sex
      FROM read_csv('{{ src_url }}*.csv', nullstr='NA')
      ORDER BY individual_id, island
"#;

    let ldrs_env = vec![
        ("LDRS_SRC_PENGUINS".to_string(), penguins_url()),
        ("LDRS_DEST_PENGUINS".to_string(), data_url()),
    ];
    run(config, &ldrs_env).await.unwrap();

    let (schema, batches) = read_back(out).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 344, "all three island files should be globbed");

    // `nullstr` is what makes these numeric rather than Utf8
    assert_eq!(
        schema.field_with_name("body_mass_g").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("delta_15_n").unwrap().data_type(),
        &DataType::Float64
    );
    assert_eq!(
        schema.field_with_name("date_egg").unwrap().data_type(),
        &DataType::Date32
    );

    let batch = &batches[0];
    let column = |name: &str| batch.column_by_name(name).unwrap().clone();

    let islands = column("island");
    let islands = islands.as_any().downcast_ref::<StringArray>().unwrap();
    let distinct: std::collections::HashSet<&str> = (0..islands.len())
        .map(|i| islands.value(i))
        .collect::<std::collections::HashSet<_>>();
    assert!(
        distinct.contains("Biscoe") && distinct.contains("Dream") && distinct.contains("Torgersen"),
        "every island file contributed rows: {distinct:?}"
    );

    // 11 rows have no Sex and 2 have no Body Mass; both are the literal `NA` in the source
    let total_null = |name: &str| -> usize {
        batches
            .iter()
            .map(|b| b.column_by_name(name).unwrap().null_count())
            .sum()
    };
    assert_eq!(total_null("sex"), 11);
    assert_eq!(total_null("body_mass_g"), 2);

    // spot-check a known row against the upstream data
    let ids = column("individual_id");
    let ids = ids.as_any().downcast_ref::<StringArray>().unwrap();
    let first = ids.value(0);
    assert!(
        first.starts_with('N'),
        "individual ids look like N1A1: {first}"
    );

    let masses = column("body_mass_g");
    assert!(masses.as_any().downcast_ref::<Int64Array>().is_some());
    let dates = column("date_egg");
    assert!(dates.as_any().downcast_ref::<Date32Array>().is_some());
    let deltas = column("delta_15_n");
    assert!(deltas.as_any().downcast_ref::<Float64Array>().is_some());
}

/// Every type the bridge carries, at boundary values, with a null row behind it. Row 0 holds the
/// values; row 1 is null in every column.
///
/// `c_blob` carries bytes that are not valid UTF-8, which is what Arrow `Binary` support exists
/// for.
#[tokio::test]
#[test_log::test]
async fn duckdb_query_preserves_types_and_values() {
    let out = "duckdb_writes/types.parquet";
    let _ = std::fs::remove_file(fixture(out));

    let config = r#"
version: 2
src: duckdb.query
destinations:
  - dest: pq
    filename: duckdb_writes/types.parquet
tables:
  - name: types
    sql: |
      SELECT
        CASE WHEN i = 0 THEN 9223372036854775807::BIGINT END          AS c_bigint,
        CASE WHEN i = 0 THEN 2147483647::INTEGER END                  AS c_integer,
        CASE WHEN i = 0 THEN 32767::SMALLINT END                      AS c_smallint,
        CASE WHEN i = 0 THEN 'quoted, "text"' END                     AS c_varchar,
        CASE WHEN i = 0 THEN 12345678.1234::DECIMAL(18,4) END         AS c_decimal,
        CASE WHEN i = 0 THEN 1.5::DOUBLE END                          AS c_double,
        CASE WHEN i = 0 THEN true END                                 AS c_boolean,
        CASE WHEN i = 0 THEN DATE '2024-06-01' END                    AS c_date,
        CASE WHEN i = 0 THEN TIMESTAMP '2024-06-01 12:34:56.789' END  AS c_timestamp,
        CASE WHEN i = 0 THEN TIMESTAMPTZ '2024-06-01 12:34:56.789Z' END AS c_timestamptz,
        CASE WHEN i = 0 THEN '\xDE\xAD\xBE\xEF'::BLOB END        AS c_blob,
        CASE WHEN i = 0 THEN '550e8400-e29b-41d4-a716-446655440000'::UUID END AS c_uuid
      FROM range(2) t(i)
      ORDER BY i
"#;

    let ldrs_env = vec![("LDRS_DEST_TYPES".to_string(), data_url())];
    run(config, &ldrs_env).await.unwrap();

    let (schema, batches) = read_back(out).await;
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);

    let field = |name: &str| schema.field_with_name(name).unwrap().data_type().clone();
    assert_eq!(field("c_bigint"), DataType::Int64);
    assert_eq!(field("c_integer"), DataType::Int32);
    assert_eq!(field("c_smallint"), DataType::Int16);
    assert_eq!(field("c_varchar"), DataType::Utf8);
    // duckdb sends Decimal128(18,4); ldrs narrows to the smallest width that fits
    assert_eq!(field("c_decimal"), DataType::Decimal64(18, 4));
    assert_eq!(field("c_double"), DataType::Float64);
    assert_eq!(field("c_boolean"), DataType::Boolean);
    assert_eq!(field("c_date"), DataType::Date32);
    assert_eq!(field("c_blob"), DataType::Binary, "bytes stay bytes");
    assert_eq!(
        field("c_uuid"),
        DataType::Utf8,
        "duckdb exports its native UUID as text, with no arrow.uuid extension"
    );
    assert_eq!(
        field("c_timestamp"),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        field("c_timestamptz"),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        "a zoned timestamp normalizes to UTC on the way out"
    );

    let col = |name: &str| batch.column_by_name(name).unwrap().clone();
    let as_i64 = |name: &str| {
        col(name)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    };

    assert_eq!(
        as_i64("c_bigint"),
        i64::MAX,
        "no precision lost at the boundary"
    );
    assert_eq!(
        col("c_integer")
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        i32::MAX
    );
    assert_eq!(
        col("c_smallint")
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        i16::MAX
    );
    assert_eq!(
        col("c_varchar")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        r#"quoted, "text""#
    );
    assert_eq!(
        col("c_decimal")
            .as_any()
            .downcast_ref::<Decimal64Array>()
            .unwrap()
            .value(0),
        123_456_781_234,
        "decimal keeps its unscaled value at scale 4"
    );
    assert_eq!(
        col("c_double")
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        1.5
    );
    assert!(col("c_boolean")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .value(0));
    assert_eq!(
        col("c_date")
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(0),
        19875,
        "days since epoch for 2024-06-01"
    );

    assert_eq!(
        col("c_blob")
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        [0xDE, 0xAD, 0xBE, 0xEF],
        "non-UTF-8 bytes survive intact"
    );

    assert_eq!(
        col("c_uuid")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "550e8400-e29b-41d4-a716-446655440000"
    );

    let instant = 1_717_245_296_789_000;
    for name in ["c_timestamp", "c_timestamptz"] {
        assert_eq!(
            col(name)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            instant,
            "{name} holds the same instant"
        );
    }

    for name in [
        "c_bigint",
        "c_integer",
        "c_smallint",
        "c_varchar",
        "c_decimal",
        "c_double",
        "c_boolean",
        "c_date",
        "c_timestamp",
        "c_timestamptz",
        "c_blob",
        "c_uuid",
    ] {
        assert!(
            col(name).is_null(1),
            "{name} should be null in the second row"
        );
    }
}

/// Declared `columns:` must retype a spawned source the same way they retype a file source, even
/// though the schema arrives with the stream rather than from a footer. Undeclared columns pass
/// through untouched.
#[tokio::test]
#[test_log::test]
async fn declared_columns_retype_a_spawned_source() {
    let out = "duckdb_writes/overrides.parquet";
    let _ = std::fs::remove_file(fixture(out));

    let config = r#"
version: 2
src: duckdb.query
destinations:
  - dest: pq
    filename: duckdb_writes/overrides.parquet
tables:
  - name: overrides
    columns:
      - { name: c_integer, type: bigint }
      - { name: c_smallint, type: integer }
      - { name: c_uuid, type: uuid }
      - { name: c_timestamp, type: timestamp, time_unit: Millis }
    sql: |
      SELECT
        7::INTEGER   AS c_integer,
        8::SMALLINT  AS c_smallint,
        '550e8400-e29b-41d4-a716-446655440000'::UUID AS c_uuid,
        TIMESTAMP '2024-06-01 12:34:56.789' AS c_timestamp,
        'untouched'  AS c_passthrough
"#;

    let ldrs_env = vec![("LDRS_DEST_OVERRIDES".to_string(), data_url())];
    run(config, &ldrs_env).await.unwrap();

    let (schema, batches) = read_back(out).await;
    let batch = &batches[0];
    let field = |name: &str| schema.field_with_name(name).unwrap().data_type().clone();

    assert_eq!(
        field("c_integer"),
        DataType::Int64,
        "widened by the declaration"
    );
    assert_eq!(field("c_smallint"), DataType::Int32);
    assert_eq!(
        field("c_uuid"),
        DataType::FixedSizeBinary(16),
        "duckdb exports UUID as text; the declaration recovers the real type"
    );
    assert_eq!(
        field("c_timestamp"),
        DataType::Timestamp(TimeUnit::Millisecond, None),
        "time_unit is honored"
    );
    assert_eq!(
        field("c_passthrough"),
        DataType::Utf8,
        "an undeclared column is left alone"
    );

    let col = |name: &str| batch.column_by_name(name).unwrap().clone();
    assert_eq!(
        col("c_integer")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        col("c_smallint")
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        8
    );
    assert_eq!(
        col("c_timestamp")
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0),
        1_717_245_296_789,
        "rescaled from micros without losing the instant"
    );

    let uuid = col("c_uuid");
    let uuid = uuid
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(
        uuid.value(0),
        [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00
        ],
        "the text uuid parsed into its 16 bytes"
    );
}

/// Build a duckdb database file for the attached-db tests.
fn make_db(relative: &str, setup: &str) -> std::path::PathBuf {
    let path = fixture(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let _ = std::fs::remove_file(&path);
    let status = std::process::Command::new("duckdb")
        .arg(&path)
        .arg("-c")
        .arg(setup)
        .status()
        .expect("duckdb must be on PATH");
    assert!(status.success(), "failed to build {relative}");
    path
}

/// A query against an attached database needs no read location at all, so `LDRS_SRC` is absent.
/// The database is attached read-only, which the second half proves by having a write refused.
#[tokio::test]
#[test_log::test]
async fn attached_db_reads_without_a_src_url_and_refuses_writes() {
    let db = make_db(
        "duckdb_writes/warehouse.db",
        "CREATE TABLE readings (id BIGINT, label VARCHAR, taken DATE); \
         INSERT INTO readings VALUES (1, 'alpha', DATE '2024-01-02'), (2, 'beta', NULL);",
    );
    let out = "duckdb_writes/attached.parquet";
    let _ = std::fs::remove_file(fixture(out));

    let config = r#"
version: 2
src: duckdb.query
destinations:
  - dest: pq
    filename: duckdb_writes/attached.parquet
tables:
  - name: readings
    sql: SELECT id, label, taken FROM readings ORDER BY id
"#;

    let ldrs_env = vec![
        (
            "LDRS_DUCKDB_DB_READINGS".to_string(),
            db.to_string_lossy().into_owned(),
        ),
        ("LDRS_DEST_READINGS".to_string(), data_url()),
    ];
    run(config, &ldrs_env).await.unwrap();

    let (_, batches) = read_back(out).await;
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    let labels = batch.column_by_name("label").unwrap().clone();
    let labels = labels.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(labels.value(0), "alpha");
    assert_eq!(labels.value(1), "beta");
    assert!(
        batch.column_by_name("taken").unwrap().is_null(1),
        "the null date survives"
    );

    // Same database, but pre_sql tries to write to it.
    let writing = r#"
version: 2
src: duckdb.query
destinations:
  - dest: pq
    filename: duckdb_writes/attached_write.parquet
tables:
  - name: readings
    pre_sql: CREATE TABLE scratch (x INTEGER);
    sql: SELECT 1 AS ok
"#;
    let err = run(writing, &ldrs_env)
        .await
        .expect_err("an attached db is read-only");
    let err = format!("{err:#}");
    assert!(
        err.contains("read-only") || err.contains("read only"),
        "expected a read-only refusal, got: {err}"
    );
}

/// A failing query has to fail the run, carrying duckdb's own message.
#[tokio::test]
#[test_log::test]
async fn failing_query_fails_the_run() {
    let config = r#"
version: 2
src: duckdb.query
destinations:
  - dest: pq
    filename: duckdb_writes/never.parquet
tables:
  - name: broken
    sql: SELECT * FROM no_such_table
"#;
    let ldrs_env = vec![("LDRS_DEST_BROKEN".to_string(), data_url())];
    let err = run(config, &ldrs_env).await.expect_err("bad SQL must fail");
    let err = format!("{err:#}");
    assert!(
        err.contains("Catalog Error"),
        "duckdb's message reaches the caller: {err}"
    );
    assert!(
        !fixture("duckdb_writes/never.parquet").exists(),
        "nothing should be committed"
    );
}

/// A query that matches no rows still commits: the destination is written as a schema-only file,
/// so a previous run's data cannot survive a successful empty load.
#[tokio::test]
#[test_log::test]
async fn empty_result_writes_a_schema_only_file() {
    let out = "duckdb_writes/empty.parquet";

    // seed the destination with real data, so a no-op run would leave it stale
    let seed = r#"
version: 2
src: duckdb.query
destinations:
  - dest: pq
    filename: duckdb_writes/empty.parquet
tables:
  - name: empty
    sql: SELECT 1::BIGINT AS id, 'seeded' AS label
"#;
    let ldrs_env = vec![("LDRS_DEST_EMPTY".to_string(), data_url())];
    run(seed, &ldrs_env).await.unwrap();
    let (_, seeded) = read_back(out).await;
    assert_eq!(seeded[0].num_rows(), 1, "destination starts non-empty");

    let config = r#"
version: 2
src: duckdb.query
destinations:
  - dest: pq
    filename: duckdb_writes/empty.parquet
tables:
  - name: empty
    sql: SELECT 1::BIGINT AS id, 'x' AS label WHERE false
"#;
    run(config, &ldrs_env)
        .await
        .expect("an empty result is not an error");

    let (schema, batches) = read_back(out).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0, "the stale row is gone");
    assert_eq!(
        schema.field_with_name("id").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("label").unwrap().data_type(),
        &DataType::Utf8
    );
}

/// A `postgres://` read location is attached rather than made into a secret: ldrs emits
/// `ATTACH '' AS pg (TYPE postgres, READ_ONLY)` and passes the connection in the child environment,
/// so no connection detail reaches SQL and the query addresses `pg.<schema>.<table>`.
#[tokio::test]
#[test_log::test]
async fn postgres_source_attaches_and_reads_through_duckdb() {
    let out = "duckdb_writes/pg_users.parquet";
    let _ = std::fs::remove_file(fixture(out));

    let config = r#"
version: 2
src: duckdb.query
destinations:
  - dest: pq
    filename: duckdb_writes/pg_users.parquet
tables:
  - name: test_schema.users
    sql: SELECT name, active FROM pg.{{ name }} ORDER BY name
"#;

    let ldrs_env = vec![
        (
            "LDRS_SRC_TEST_SCHEMA_USERS".to_string(),
            "postgres://postgres:postgres@localhost:5432/postgres".to_string(),
        ),
        ("LDRS_DEST_TEST_SCHEMA_USERS".to_string(), data_url()),
    ];
    run(config, &ldrs_env).await.unwrap();

    let (schema, batches) = read_back(out).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(rows > 0, "the Postgres table was read");
    assert_eq!(
        schema.field_with_name("name").unwrap().data_type(),
        &DataType::Utf8
    );
    assert_eq!(
        schema.field_with_name("active").unwrap().data_type(),
        &DataType::Boolean
    );
}
