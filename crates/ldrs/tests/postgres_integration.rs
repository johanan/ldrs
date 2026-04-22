use ldrs::{ldrs_config::create_ldrs_exec, ldrs_postgres::client::create_connection};

const TEST_CASES: &[&str] = &[
    "
dest: pg.drop_replace
src: file

tables:
  - name: public_test.users
    filename: tests/test_data/public.users/public.users.snappy.parquet
    post_sql: create unique index if not exists unique_id_idx on {{ name }} (unique_id);
",
    // test the defaults
    "
dest: pg.drop_replace
src: file
src_defaults:
  filename: tests/test_data/public.users/public.{{ table }}.snappy.parquet

tables:
- name: public_test.users
  post_sql: create unique index if not exists unique_id_idx on {{ name }} (unique_id);
",
    // test without src or dest to see if the defaults work
    "
tables:
  - name: public_test.users
    dest: pg.drop_replace
    filename: tests/test_data/public.users/public.users.snappy.parquet
    post_sql: create unique index if not exists unique_id_idx on {{ name }} (unique_id);
",
    // now test truncate insert
    "
tables:
  - name: public_test.users
    dest: pg.truncate_insert
    filename: tests/test_data/public.users/public.users.snappy.parquet
    post_sql: create unique index if not exists unique_id_idx on {{ name }} (unique_id);
",
    // should be able to infer based on delete_keys
    "
tables:
  - name: public_test.users
    filename: tests/test_data/public.users/public.users.snappy.parquet
    delete_keys: [created]
",
    // test with param types
    "
tables:
  - name: public_test.users
    filename: tests/test_data/public.users/public.users.snappy.parquet
    delete_keys: [created]
    param_keys: [ { name: created, type: timestamp } ]
",
    "
tables:
  - name: public_test.users
    filename: tests/test_data/public.users/public.users.snappy.parquet
    merge_keys: [unique_id]
",
];

#[tokio::test]
#[test_log::test]
async fn test_postgres_file_drop() {
    let file_url = "file://";
    let pg_role_url =
        "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable&role=test_role";
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), file_url.to_string()),
        ("LDRS_DEST".to_string(), pg_role_url.to_string()),
        (
            "LDRS_PARAM_PUBLIC_TEST_USERS_P1_TIMESTAMP".to_string(),
            "2024-10-08T17:22:00".to_string(),
        ),
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
    for config in TEST_CASES {
        let client = create_connection(pg_url).await.unwrap();
        let _ = client.batch_execute("DROP SCHEMA IF EXISTS public_test CASCADE");
        let ex = create_ldrs_exec(config, &ldrs_env, None, &rt.handle()).await;
        assert_eq!(ex.is_ok(), true);

        let users = client
            .query("SELECT * FROM public_test.users", &[])
            .await
            .unwrap();
        assert_eq!(users.len(), 2);

        // ensure table is owned by test_role, which should be set from the url
        let owner_query = "
                   SELECT tableowner
                   FROM pg_tables
                   WHERE schemaname = 'public_test' AND tablename = 'users'
               ";
        let owner_row = client.query_one(owner_query, &[]).await.unwrap();
        let owner: String = owner_row.get(0);
        assert_eq!(owner, "test_role", "Table should be owned by test_role");
    }
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test]
async fn test_postgres_env_role() {
    let file_url = "file://";
    let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), file_url.to_string()),
        ("LDRS_DEST".to_string(), pg_url.to_string()),
        (
            "LDRS_PARAM_DEL_P1_TIMESTAMP".to_string(),
            "2024-10-08T17:22:00".to_string(),
        ),
        ("LDRS_PG_ROLE".to_string(), "non_existent_role".to_string()),
    ];
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let ex = create_ldrs_exec("", &ldrs_env, None, &rt.handle()).await;
    assert!(ex.is_err());
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test]
#[test_log::test]
async fn test_postgres_all_parquets() {
    let file_url = "file://";
    let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), file_url.to_string()),
        ("LDRS_DEST".to_string(), pg_url.to_string()),
    ];

    let config = "
dest: pg.drop_replace
src: file

tables:
  - name: public_test_all.users
    filename: tests/test_data/public.users/public.users.snappy.parquet
  - name: public_test_all.strings
    filename: tests/test_data/public.string_values/public.strings.snappy.parquet
  - name: public_test_all.numbers
    filename: tests/test_data/public.numbers/public.numbers.snappy.parquet
";

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let client = create_connection(pg_url).await.unwrap();
    let _ = client
        .batch_execute("DROP SCHEMA IF EXISTS public_test_all CASCADE")
        .await;
    let _ = client.batch_execute("CREATE SCHEMA public_test_all").await;

    let ex = create_ldrs_exec(config, &ldrs_env, None, &rt.handle()).await;
    assert!(ex.is_ok(), "ldrs exec should succeed: {:?}", ex.err());

    for table in ["users", "strings", "numbers"] {
        let rows = client
            .query(&format!("SELECT * FROM public_test_all.{}", table), &[])
            .await
            .unwrap();
        assert!(
            !rows.is_empty(),
            "public_test_all.{} should have rows after load",
            table
        );
    }

    // JSONB round-trip value checks. PG operators (->>, jsonb_typeof) reject
    // malformed JSONB, so these double as wire-format validation.
    let empty_type: String = client
        .query_one(
            "SELECT jsonb_typeof(jsonb_value) FROM public_test_all.strings \
             WHERE varchar_value = 'a'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        empty_type, "object",
        "empty jsonb '{{}}' should be an object"
    );

    let nested_value: String = client
        .query_one(
            "SELECT jsonb_value->>'key' FROM public_test_all.strings \
             WHERE varchar_value = 'b'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(nested_value, "value", "jsonb key extraction round-trip");

    // Numeric round-trip value checks. Cast to text so assertions are type-neutral —
    for (smallint, expected_decimal) in [
        (3i32, "3.000000000000000"),
        (0, "468797.177024568000000"),
        (32767, "507531.111989867000000"),
    ] {
        let actual: String = client
            .query_one(
                "SELECT decimal_value::text FROM public_test_all.numbers \
                 WHERE smallint_value = $1",
                &[&smallint],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            actual, expected_decimal,
            "decimal round-trip for smallint_value={}",
            smallint
        );
    }

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test]
#[test_log::test]
async fn test_postgres_numeric_edge_cases() {
    let file_url = "file://";
    let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), file_url.to_string()),
        ("LDRS_DEST".to_string(), pg_url.to_string()),
    ];

    let config = "
dest: pg.drop_replace
src: file

tables:
  - name: public_test_edge.numbers_edge
    filename: tests/test_data/public.numbers_edge/numbers_edge.snappy.parquet
";

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let client = create_connection(pg_url).await.unwrap();
    let _ = client
        .batch_execute("DROP SCHEMA IF EXISTS public_test_edge CASCADE")
        .await;
    let _ = client.batch_execute("CREATE SCHEMA public_test_edge").await;

    let ex = create_ldrs_exec(config, &ldrs_env, None, &rt.handle()).await;
    assert!(ex.is_ok(), "ldrs exec should succeed: {:?}", ex.err());

    // Edge cases covering PgFixedNumeric branches not exercised by the main fixture:
    //   id=1 → zero (ndigits=0 fast path)
    //   id=2 → negative, multi-group fractional
    //   id=3 → value < 1 (negative weight)
    //   id=4 → negative AND value < 1
    for (id, expected) in [
        (1i32, "0.000000000000000"),
        (2, "-123.456789012345000"),
        (3, "0.000000000000005"),
        (4, "-0.123456789012345"),
    ] {
        let actual: String = client
            .query_one(
                "SELECT decimal_value::text FROM public_test_edge.numbers_edge \
                 WHERE id = $1",
                &[&id],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            actual, expected,
            "decimal edge-case round-trip for id={}",
            id
        );
    }

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}
