use std::sync::Arc;

use delta_kernel::engine::default::executor::{tokio::TokioBackgroundExecutor, TaskExecutor};
use futures::TryStreamExt;
use ldrs::{
    ldrs_delta::process_delta,
    parquet_provider::{self},
};

#[tokio::test]
async fn test_delta_poc() {
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
    let builder = parquet_provider::builder_from_string(path.clone(), rt.handle().clone())
        .await
        .unwrap();
    let stream = builder
        .with_batch_size(1024)
        .build()
        .unwrap()
        .map_err(|e| e.into());
    let background_executor = Arc::new(TokioBackgroundExecutor::default());
    let rows_per_file = 1000;
    let run = process_delta(stream, background_executor, rows_per_file);
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}
