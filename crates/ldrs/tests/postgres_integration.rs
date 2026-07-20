use ldrs::ldrs_config::{execute_configs, parse_yaml_config};
use ldrs_postgres::create_connection;
use ldrs_test_fixtures::data_url;

const TEST_CASES: &[&str] = &[
    "
dest: pg.drop_replace
src: file

tables:
  - name: public_test.users
    filename: public.users/public.users.snappy.parquet
    post_sql: create unique index if not exists unique_id_idx on {{ name }} (unique_id);
",
    // test the defaults
    "
dest: pg.drop_replace
src: file
src_defaults:
  filename: public.users/public.{{ table_of name }}.snappy.parquet

tables:
- name: public_test.users
  post_sql: create unique index if not exists unique_id_idx on {{ name }} (unique_id);
",
    // test without src or dest to see if the defaults work
    "
tables:
  - name: public_test.users
    dest: pg.drop_replace
    filename: public.users/public.users.snappy.parquet
    post_sql: create unique index if not exists unique_id_idx on {{ name }} (unique_id);
",
    // now test truncate insert
    "
tables:
  - name: public_test.users
    dest: pg.truncate_insert
    filename: public.users/public.users.snappy.parquet
    post_sql: create unique index if not exists unique_id_idx on {{ name }} (unique_id);
",
    // should be able to infer based on delete_keys
    "
tables:
  - name: public_test.users
    filename: public.users/public.users.snappy.parquet
    delete_keys: [created]
",
    // test the general LDRS_PARAM_<COL> fallback: `name` has no table-scoped var set,
    // only LDRS_PARAM_NAME, so resolution falls through to the general form.
    "
tables:
  - name: public_test.users
    filename: public.users/public.users.snappy.parquet
    delete_keys: [name]
",
    "
tables:
  - name: public_test.users
    filename: public.users/public.users.snappy.parquet
    merge_keys: [unique_id]
",
];

#[tokio::test]
#[test_log::test]
async fn test_postgres_file_drop() {
    let file_url = data_url();
    let pg_role_url =
        "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable&role=test_role";
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), file_url.to_string()),
        ("LDRS_DEST".to_string(), pg_role_url.to_string()),
        (
            "LDRS_PARAM_PUBLIC_TEST_USERS_CREATED".to_string(),
            "2024-10-08T17:22:00".to_string(),
        ),
        ("LDRS_PARAM_NAME".to_string(), "John Doe".to_string()),
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
    for config in TEST_CASES {
        let client = create_connection(pg_url).await.unwrap();
        let _ = client
            .batch_execute("DROP SCHEMA IF EXISTS public_test CASCADE")
            .await;
        let ex = execute_configs(
            parse_yaml_config(&config, &ldrs_env).unwrap(),
            None,
            &ldrs_env,
            &rt.handle(),
        )
        .await;
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
    let config = "
src: file
dest: pg.drop_replace
tables:
  - name: public_test.users
    filename: public.users/public.users.snappy.parquet
      ";
    let file_url = data_url();
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
    let ex = execute_configs(
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        &rt.handle(),
    )
    .await;
    assert!(ex.is_err());
    let ex_err = ex.unwrap_err();
    let msg = format!("{:?}", ex_err); // Debug walks the whole anyhow chain, not just top-level
    assert!(
        msg.contains("non_existent_role"),
        "expected role error, got: {}",
        msg,
    );
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test]
#[test_log::test]
async fn test_postgres_all_parquets() {
    let file_url = data_url();
    let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), file_url),
        ("LDRS_DEST".to_string(), pg_url.to_string()),
    ];

    let config = "
dest: pg.drop_replace
src: file

tables:
  - name: public_test_all.users
    target: public_test_all.renamed
    filename: public.users/public.users.snappy.parquet
  - name: public_test_all.strings
    filename: public.string_values/public.strings.snappy.parquet
  - name: public_test_all.numbers
    filename: public.numbers/public.numbers.snappy.parquet
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

    let ex = execute_configs(
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        &rt.handle(),
    )
    .await;
    assert!(ex.is_ok(), "ldrs exec should succeed: {:?}", ex.err());

    for table in ["strings", "numbers"] {
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

    // `users` was given an explicit `target`, so it lands at `renamed`, not at its `name`.
    let renamed = client
        .query("SELECT * FROM public_test_all.renamed", &[])
        .await
        .unwrap();
    assert!(
        !renamed.is_empty(),
        "users data should land at its target table `renamed`"
    );
    let untargeted: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'public_test_all' AND table_name = 'users')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        !untargeted,
        "no table should be created at the un-targeted name `users`"
    );

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

    // Numeric round-trip value checks. Cast to text so assertions are type-neutral
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
    let file_url = data_url();
    let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), file_url),
        ("LDRS_DEST".to_string(), pg_url.to_string()),
    ];

    let config = "
dest: pg.drop_replace
src: file

tables:
  - name: public_test_edge.numbers_edge
    filename: public.numbers_edge/numbers_edge.snappy.parquet
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

    let ex = execute_configs(
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        &rt.handle(),
    )
    .await;
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

#[tokio::test]
#[test_log::test]
async fn test_postgres_rollback_on_post_sql_failure() {
    let file_url = data_url();
    let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), file_url),
        ("LDRS_DEST".to_string(), pg_url.to_string()),
    ];

    let config = "
dest: pg.drop_replace
src: file

tables:
  - name: public_test_fail.users
    filename: public.users/public.users.snappy.parquet
    post_sql: this is not valid sql;
";

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let client = create_connection(pg_url).await.unwrap();
    let _ = client
        .batch_execute("DROP SCHEMA IF EXISTS public_test_fail CASCADE")
        .await;

    let ex = execute_configs(
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        &rt.handle(),
    )
    .await;
    assert!(ex.is_err(), "load should fail on the invalid post_sql");

    let exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
             WHERE table_schema = 'public_test_fail' AND table_name = 'users')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!exists, "table must not exist after rollback");

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}

#[tokio::test]
#[test_log::test]
async fn test_postgres_templated_target() {
    // The per-tenant path: one config, a runtime `LDRS_TEMPL_TENANT`, and a `target` that weaves
    // it into the landing table via helpers. Renders to `public_test_tenant.acme_users`.
    let file_url = data_url();
    let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), file_url),
        ("LDRS_DEST".to_string(), pg_url.to_string()),
        ("LDRS_TEMPL_TENANT".to_string(), "acme".to_string()),
    ];

    let config = "
dest: pg.drop_replace
src: file

tables:
  - name: public_test_tenant.users
    target: \"public_test_tenant.{{ tenant }}_{{ table_of name }}\"
    filename: public.users/public.users.snappy.parquet
";

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let client = create_connection(pg_url).await.unwrap();
    let _ = client
        .batch_execute("DROP SCHEMA IF EXISTS public_test_tenant CASCADE")
        .await;

    let ex = execute_configs(
        parse_yaml_config(&config, &ldrs_env).unwrap(),
        None,
        &ldrs_env,
        &rt.handle(),
    )
    .await;
    assert!(ex.is_ok(), "ldrs exec should succeed: {:?}", ex.err());

    let rows = client
        .query("SELECT * FROM public_test_tenant.acme_users", &[])
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "data should land at the templated target table"
    );

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}
