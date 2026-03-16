use std::sync::Arc;

use futures::TryStreamExt;
use ldrs::parquet_provider::{self};

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
    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}
