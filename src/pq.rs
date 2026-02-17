use parquet::basic::LogicalType;
use parquet::file::metadata::FileMetaData;
use parquet::schema::types::Type::{GroupType, PrimitiveType};
use std::sync::Arc;

use crate::types::{ColumnDefintion, ColumnSchema, TimeUnit};

pub fn get_fields(
    metadata: &parquet::file::metadata::FileMetaData,
) -> anyhow::Result<&Vec<Arc<parquet::schema::types::Type>>, anyhow::Error> {
    let root = metadata.schema();
    match root {
        parquet::schema::types::Type::GroupType { fields, .. } => Ok(fields),
        _ => Err(anyhow::Error::msg("Invalid schema")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_provider::builder_from_string;
    use crate::test_utils::{create_runtime, drop_runtime};

    #[tokio::test]
    async fn test_get_file_metadata() {
        let cd = std::env::current_dir().unwrap();
        let path = format!(
            "file://{}/test_data/public.users.snappy.parquet",
            cd.display()
        );
        let rt = create_runtime();
        let result = builder_from_string(path, rt.handle().clone()).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        let metadata = result.metadata().file_metadata();
        println!("metadata: {:?}", metadata);
        assert_eq!(metadata.num_rows(), 2);

        // now get the fields
        let fields = get_fields(metadata).unwrap();
        assert_eq!(fields.len(), 6);
        drop_runtime(rt);
    }

    #[tokio::test]
    async fn test_get_file_metadata_invalid_url() {
        let path = "path/to/file".to_string();
        let rt = create_runtime();
        let result = builder_from_string(path, rt.handle().clone()).await;
        assert!(result.is_err());
        drop_runtime(rt);
    }
}
