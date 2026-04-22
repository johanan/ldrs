use anyhow::{Context, Error};
use object_store::ObjectStoreExt;
use parquet::arrow::{
    arrow_reader::ArrowReaderBuilder,
    async_reader::{AsyncReader, ParquetObjectReader},
    ParquetRecordBatchStreamBuilder,
};
use tokio::runtime::Handle;
use url::Url;

use crate::storage::{base_or_relative_path, build_store};

pub async fn builder_from_url(
    url: Url,
    handle: tokio::runtime::Handle,
) -> Result<ArrowReaderBuilder<AsyncReader<ParquetObjectReader>>, anyhow::Error> {
    let (store, path, _) = build_store(&url)?;

    let meta = store
        .head(&path)
        .await
        .with_context(|| "Could not find file in store")?;

    let reader = ParquetObjectReader::new(store, meta.location).with_runtime(handle);

    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .with_context(|| "Could not create parquet record batch stream builder")?;

    Ok(builder)
}

pub async fn builder_from_string(
    path: String,
    handle: Handle,
) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>, Error> {
    let path_parsed = base_or_relative_path(&path)?;
    builder_from_url(path_parsed, handle).await
}
