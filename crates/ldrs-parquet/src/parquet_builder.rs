use anyhow::{Context, Error};
use ldrs_storage::{base_or_relative_path, build_store};
use object_store::ObjectStoreExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::runtime::Handle;
use url::Url;

use crate::store_reader::{SpawnedStoreReader, StoreReader};

pub async fn builder_from_url(
    url: Url,
    handle: Handle,
) -> Result<ParquetRecordBatchStreamBuilder<SpawnedStoreReader>, anyhow::Error> {
    let (store, path, _) = build_store(&url)?;

    let meta = store
        .head(&path)
        .await
        .with_context(|| "Could not find file in store")?;

    let reader = StoreReader::spawned(store, meta.location, meta.size, handle);

    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .with_context(|| "Could not create parquet record batch stream builder")?;

    Ok(builder)
}

pub async fn builder_from_string(
    path: String,
    handle: Handle,
) -> Result<ParquetRecordBatchStreamBuilder<SpawnedStoreReader>, Error> {
    let path_parsed = base_or_relative_path(&path)?;
    builder_from_url(path_parsed, handle).await
}
