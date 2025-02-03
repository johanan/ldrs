use anyhow::Context;
use object_store::{parse_url, ObjectStore};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::{arrow_reader::ArrowReaderBuilder, async_reader::ParquetObjectReader};
use std::sync::Arc;
use tracing::{debug, info};
use url::Url;

pub async fn get_file_metadata(
    path_url: String,
) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>, anyhow::Error> {
    let path_parsed = Url::parse(&path_url).with_context(|| "Could not parse path URL")?;

    let (store, path) =
        parse_url(&path_parsed).with_context(|| "Could not find a valid object store")?;
    let store = Arc::new(store);

    let meta = store
        .head(&path)
        .await
        .with_context(|| "Could not find file in store")?;
    info!("meta: {:?}", meta);

    let reader = ParquetObjectReader::new(store, meta);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .with_context(|| "Could not create parquet record batch stream builder")?;
    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::*;
    // will need to get the parquet testing files from arrow-rs

    /*#[tokio::test]
    async fn test_get_file_metadata() {
        let path = "file:///path/to/file".to_string();
        let result = get_file_metadata(path).await;
        assert!(result.is_ok());
    }*/

    #[tokio::test]
    async fn test_get_file_metadata_invalid_url() {
        let path = "path/to/file".to_string();
        let result = get_file_metadata(path).await;
        assert!(result.is_err());
    }
}
