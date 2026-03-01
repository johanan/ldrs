use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::stream::StreamExt;
use futures::Stream;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::LogicalType;
use parquet::file::properties::WriterPropertiesBuilder;
use parquet::schema::types::ColumnPath;
use parquet::schema::types::Type::{GroupType, PrimitiveType};
use std::pin::pin;
use std::sync::Arc;

use crate::arrow_access::arrow_transforms::{transform_batch, ArrowColumnTransformStrategy};
use crate::storage::StorageProvider;
use crate::types::{ColumnSpec, TimeUnit};

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
    transforms: Vec<Option<ArrowColumnTransformStrategy>>,
    bloom_filters: Vec<Vec<String>>,
    stream: S,
) -> Result<(), anyhow::Error>
where
    S: Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + 'static,
{
    // determine if we need to transform the batches
    let transform_plan = if transforms.iter().any(|s| s.is_some()) {
        Some(transforms)
    } else {
        None
    };

    let storage = StorageProvider::try_from_string(file_path)?;
    let (store, path) = storage.get_store_and_path()?;
    let parq_writer = ParquetObjectWriter::new(store, path);
    let mut writer_props = WriterPropertiesBuilder::default()
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_encoding(parquet::basic::Encoding::PLAIN);
    // add each bloom filter
    for filter in bloom_filters {
        writer_props = writer_props.set_column_bloom_filter_enabled(ColumnPath::from(filter), true);
    }

    let mut writer =
        AsyncArrowWriter::try_new(parq_writer, schema.clone(), Some(writer_props.build()))?;

    // start reading the stream and writing to the parquet file
    let mut pinned_stream = pin!(stream);
    while let Some(batch) = pinned_stream.next().await {
        let batch = batch?;
        let batch = match &transform_plan {
            Some(plan) => transform_batch(&batch, &plan, schema.clone())?,
            None => batch,
        };
        writer.write(&batch).await?;
    }
    let _ = writer.close().await?;
    Ok(())
}
