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

pub fn map_parquet_to_abstract<'a>(
    pq: &'a Arc<parquet::schema::types::Type>,
    user_defs: &[ColumnDefintion],
) -> Option<ColumnSchema<'a>> {
    match pq.as_ref() {
        GroupType { .. } => None,
        PrimitiveType { basic_info, .. } => {
            let name = pq.name();
            let pq_type: ColumnSchema<'_> = match basic_info {
                bi if bi.logical_type().is_some() => {
                    // we have a logical type, use that
                    match bi.logical_type().unwrap() {
                        LogicalType::String => user_defs
                            .iter()
                            .find(|cd| cd.name == name)
                            .and_then(|cd| {
                                if cd.ty == "VARCHAR" {
                                    Some(ColumnSchema::Varchar(name, cd.length))
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(ColumnSchema::Text(name)),
                        LogicalType::Timestamp {
                            is_adjusted_to_u_t_c: true,
                            unit,
                        } => ColumnSchema::TimestampTz(name, TimeUnit::from(&unit)),
                        LogicalType::Timestamp {
                            is_adjusted_to_u_t_c: false,
                            unit,
                        } => ColumnSchema::Timestamp(name, TimeUnit::from(&unit)),
                        LogicalType::Uuid => ColumnSchema::Uuid(name),
                        LogicalType::Json => ColumnSchema::Jsonb(name),
                        LogicalType::Decimal { scale, precision } => {
                            ColumnSchema::Numeric(name, precision, scale)
                        }
                        _ => ColumnSchema::Text(name),
                    }
                }
                _ => match pq.get_physical_type() {
                    parquet::basic::Type::FLOAT => ColumnSchema::Real(name),
                    parquet::basic::Type::DOUBLE => ColumnSchema::Double(name),
                    parquet::basic::Type::INT32 => ColumnSchema::Integer(name),
                    parquet::basic::Type::INT64 => ColumnSchema::BigInt(name),
                    parquet::basic::Type::BOOLEAN => ColumnSchema::Boolean(name),
                    _ => ColumnSchema::Text(name),
                },
            };
            Some(pq_type)
        }
    }
}

pub fn get_column_schema(
    metadata: &FileMetaData,
) -> anyhow::Result<Vec<ColumnSchema>, anyhow::Error> {
    let kv = get_kv_fields(metadata);

    let fields = get_fields(metadata)?;
    let mapped = fields
        .iter()
        .filter_map(|pq| map_parquet_to_abstract(pq, &kv))
        .collect::<Vec<ColumnSchema>>();
    Ok(mapped)
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
