use futures::TryStreamExt;
use ldrs::{
    ldrs_parquet::write_parquet,
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
        write_parquet(&file_path, schema, stream).await.unwrap();
    }

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}
