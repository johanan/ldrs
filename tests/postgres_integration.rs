use ldrs::{ldrs_config::create_ldrs_exec, ldrs_postgres::client::create_connection};

const TEST_CASES: &[&str] = &[
    "
dest: pg.drop_replace
src: file

tables:
  - name: public_test.users
    filename: tests/test_data/public.users/public.users.snappy.parquet
    post_sql: create unique index if not exists unique_id_idx on {{ name }} (unique_id);
    role: test_role
",
    // test without src or dest to see if the defaults work
    "
tables:
  - name: public_test.users
    dest: pg.drop_replace
    filename: tests/test_data/public.users/public.users.snappy.parquet
    post_sql: create unique index if not exists unique_id_idx on {{ name }} (unique_id);
    role: test_role
",
    // now test truncate insert
    "
tables:
  - name: public_test.users
    dest: pg.truncate_insert
    filename: tests/test_data/public.users/public.users.snappy.parquet
    post_sql: create unique index if not exists unique_id_idx on {{ name }} (unique_id);
    role: test_role
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
async fn test_postgres_file_drop() {
    let _default_guard = tracing::subscriber::set_global_default(
        tracing_subscriber::fmt::Subscriber::builder()
            .compact()
            .with_max_level(tracing::Level::TRACE)
            .finish(),
    );

    let file_url = "file://";
    let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
    let ldrs_env = vec![
        ("LDRS_SRC".to_string(), file_url.to_string()),
        ("LDRS_DEST".to_string(), pg_url.to_string()),
        (
            "LDRS_PARAM_DEL_P1_TIMESTAMP".to_string(),
            "2024-10-08T17:22:00".to_string(),
        ),
    ];

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    for config in TEST_CASES {
        let ex = create_ldrs_exec(config, &ldrs_env, &rt.handle())
            .await
            .unwrap();

        // assert_eq!(ex.is_ok(), true);

        let client = create_connection(pg_url).await.unwrap();
        let users = client
            .query("SELECT * FROM public_test.users", &[])
            .await
            .unwrap();
        assert_eq!(users.len(), 2);
    }
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}
