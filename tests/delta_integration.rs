use delta_kernel::expressions::Scalar;
use ldrs::ldrs_delta::parquet_metadata_to_delta_stats;
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
