use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::stream::StreamExt;
use futures::Stream;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::LogicalType;
use parquet::file::properties::WriterPropertiesBuilder;
use parquet::schema::types::Type::{GroupType, PrimitiveType};
use std::pin::pin;
use std::sync::Arc;

use crate::storage::StorageProvider;
use crate::types::{ColumnSchema, ColumnSpec, TimeUnit};

impl<'a> TryFrom<&'a Arc<parquet::schema::types::Type>> for ColumnSchema<'a> {
    type Error = anyhow::Error;

    fn try_from(field: &'a Arc<parquet::schema::types::Type>) -> Result<Self, Self::Error> {
        match field.as_ref() {
            GroupType { .. } => Err(anyhow::Error::msg(
                "Cannot convert GroupType to ColumnSchema",
            )),
            PrimitiveType { basic_info, .. } => {
                let name = field.name();
                let pq_type: ColumnSchema<'_> = match basic_info {
                    bi if bi.logical_type_ref().is_some() => {
                        // we have a logical type, use that
                        match bi.logical_type_ref().unwrap() {
                            LogicalType::String => ColumnSchema::Text(name),
                            LogicalType::Timestamp {
                                is_adjusted_to_u_t_c: true,
                                unit,
                            } => ColumnSchema::TimestampTz(name, TimeUnit::from(unit)),
                            LogicalType::Timestamp {
                                is_adjusted_to_u_t_c: false,
                                unit,
                            } => ColumnSchema::Timestamp(name, TimeUnit::from(unit)),
                            LogicalType::Uuid => ColumnSchema::Uuid(name),
                            LogicalType::Json => ColumnSchema::Jsonb(name),
                            LogicalType::Decimal { scale, precision } => {
                                ColumnSchema::Numeric(name, *precision, *scale)
                            }
                            _ => ColumnSchema::Text(name),
                        }
                    }
                    _ => match field.get_physical_type() {
                        parquet::basic::Type::FLOAT => ColumnSchema::Real(name),
                        parquet::basic::Type::DOUBLE => ColumnSchema::Double(name, None),
                        parquet::basic::Type::INT32 => ColumnSchema::Integer(name),
                        parquet::basic::Type::INT64 => ColumnSchema::BigInt(name),
                        parquet::basic::Type::BOOLEAN => ColumnSchema::Boolean(name),
                        _ => ColumnSchema::Text(name),
                    },
                };
                Ok(pq_type)
            }
        }
    }
}

impl TryFrom<&Arc<parquet::schema::types::Type>> for ColumnSpec {
    type Error = anyhow::Error;

    fn try_from(field: &Arc<parquet::schema::types::Type>) -> Result<Self, Self::Error> {
        match field.as_ref() {
            GroupType { .. } => Err(anyhow::Error::msg("Cannot convert GroupType to ColumnSpec")),
            PrimitiveType { basic_info, .. } => {
                let name = field.name().to_string();
                let pq_type: ColumnSpec = match basic_info {
                    bi if bi.logical_type_ref().is_some() => {
                        // we have a logical type, use that
                        match bi.logical_type_ref().unwrap() {
                            LogicalType::String => ColumnSpec::Text { name },
                            LogicalType::Timestamp {
                                is_adjusted_to_u_t_c: true,
                                unit,
                            } => ColumnSpec::TimestampTz {
                                name,
                                time_unit: TimeUnit::from(unit),
                            },
                            LogicalType::Timestamp {
                                is_adjusted_to_u_t_c: false,
                                unit,
                            } => ColumnSpec::Timestamp {
                                name,
                                time_unit: TimeUnit::from(unit),
                            },
                            LogicalType::Uuid => ColumnSpec::Uuid { name },
                            LogicalType::Json => ColumnSpec::Jsonb { name },
                            LogicalType::Decimal { scale, precision } => ColumnSpec::Numeric {
                                name,
                                precision: *precision,
                                scale: *scale,
                            },
                            _ => ColumnSpec::Text { name },
                        }
                    }
                    _ => match field.get_physical_type() {
                        parquet::basic::Type::FLOAT => ColumnSpec::Real { name },
                        parquet::basic::Type::DOUBLE => ColumnSpec::Double { name },
                        parquet::basic::Type::INT32 => ColumnSpec::Integer { name },
                        parquet::basic::Type::INT64 => ColumnSpec::BigInt { name },
                        parquet::basic::Type::BOOLEAN => ColumnSpec::Boolean { name },
                        _ => ColumnSpec::Text { name },
                    },
                };
                Ok(pq_type)
            }
        }
    }
}

pub async fn write_parquet<S>(
    file_path: &str,
    schema: SchemaRef,
    stream: S,
) -> Result<(), anyhow::Error>
where
    S: Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + 'static,
{
    let storage = StorageProvider::try_from_string(file_path)?;
    let (store, path) = storage.get_store_and_path()?;
    let parq_writer = ParquetObjectWriter::new(store, path);
    let writer_props = WriterPropertiesBuilder::default()
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let mut writer = AsyncArrowWriter::try_new(parq_writer, schema, Some(writer_props))?;
    // start reading the stream and writing to the parquet file
    let mut pinned_stream = pin!(stream);
    while let Some(batch) = pinned_stream.next().await {
        writer.write(&batch?).await?;
    }
    let metadata = writer.close().await?;
    println!("Metadata: {:?}", metadata);
    Ok(())
}
