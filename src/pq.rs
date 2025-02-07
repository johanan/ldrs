use anyhow::Context;
use object_store::{parse_url, ObjectStore};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;
use url::Url;

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnDefintion {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub len: i32,
}

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

pub fn get_fields(
    metadata: &parquet::file::metadata::FileMetaData,
) -> anyhow::Result<&Vec<Arc<parquet::schema::types::Type>>, anyhow::Error> {
    let root = metadata.schema();
    match root {
        parquet::schema::types::Type::GroupType { fields, .. } => Ok(fields),
        _ => Err(anyhow::Error::msg("Invalid schema")),
    }
}

pub fn get_kv_fields(metadata: &parquet::file::metadata::FileMetaData) -> Vec<ColumnDefintion> {
    match metadata.key_value_metadata() {
        Some(kv) => {
            let cols = kv
                .iter()
                .find(|k| k.key == "cols")
                .and_then(|k| k.value.clone());
            let try_cols = cols.map(|cols| serde_json::from_str(&cols)).transpose();
            match try_cols {
                Ok(Some(cols)) => cols,
                _ => Vec::new(),
            }
        }
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_file_metadata() {
        let cd = std::env::current_dir().unwrap();
        let path = format!(
            "file://{}/test_data/public.users.snappy.parquet",
            cd.display()
        );
        let result = get_file_metadata(path).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        let metadata = result.metadata().file_metadata();
        println!("metadata: {:?}", metadata);
        assert_eq!(metadata.num_rows(), 2);

        // now get the fields
        let fields = get_fields(metadata).unwrap();
        assert_eq!(fields.len(), 6);
    }

    #[tokio::test]
    async fn test_get_file_metadata_invalid_url() {
        let path = "path/to/file".to_string();
        let result = get_file_metadata(path).await;
        assert!(result.is_err());
    }
}
