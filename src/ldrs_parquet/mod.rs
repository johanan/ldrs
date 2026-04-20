use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, SchemaRef};
use futures::stream::StreamExt;
use futures::Stream;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::{AsyncArrowWriter, ParquetRecordBatchStreamBuilder};
use parquet::basic::LogicalType;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use parquet::schema::types::Type::{GroupType, PrimitiveType};
use std::pin::pin;
use std::sync::Arc;

use parquet::arrow::RowNumber;

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

pub fn default_writer_props() -> WriterProperties {
    WriterPropertiesBuilder::default()
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_encoding(parquet::basic::Encoding::PLAIN)
        .build()
}

pub fn with_bloom_filters(
    props: WriterProperties,
    bloom_filters: Vec<Vec<String>>,
) -> WriterProperties {
    let mut builder = props.into_builder();
    for filter in bloom_filters {
        builder = builder.set_column_bloom_filter_enabled(ColumnPath::from(filter), true);
    }
    builder.build()
}

pub async fn write_parquet<S>(
    file_path: &str,
    schema: SchemaRef,
    transforms: Vec<Option<ArrowColumnTransformStrategy>>,
    writer_props: Option<WriterProperties>,
    stream: S,
) -> Result<ParquetMetaData, anyhow::Error>
where
    S: Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + 'static,
{
    let transform_plan = if transforms.iter().any(|s| s.is_some()) {
        Some(transforms)
    } else {
        None
    };

    let storage = StorageProvider::try_from_string(file_path)?;
    let (store, path) = storage.get_store_and_path()?;
    let parq_writer = ParquetObjectWriter::new(store, path);
    let props = writer_props.unwrap_or(default_writer_props());

    let mut writer = AsyncArrowWriter::try_new(parq_writer, schema.clone(), Some(props))?;

    let mut pinned_stream = pin!(stream);
    while let Some(batch) = pinned_stream.next().await {
        let batch = batch?;
        let batch = match &transform_plan {
            Some(plan) => transform_batch(&batch, plan, schema.clone())?,
            None => batch,
        };
        writer.write(&batch).await?;
    }
    let metadata = writer.close().await?;
    Ok(metadata)
}

pub async fn write_parquet_split<S, F>(
    dir_path: &str,
    schema: SchemaRef,
    transforms: Vec<Option<ArrowColumnTransformStrategy>>,
    stream: S,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
    name_file: F,
    writer_props: Option<WriterProperties>,
) -> Result<Vec<(String, ParquetMetaData)>, anyhow::Error>
where
    S: Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + 'static,
    F: Fn(usize) -> String,
{
    let transform_plan = if transforms.iter().any(|s| s.is_some()) {
        Some(transforms)
    } else {
        None
    };

    let storage = StorageProvider::try_from_string(dir_path)?;
    let (store, base_path) = storage.get_store_and_path()?;

    let props = writer_props.unwrap_or_else(default_writer_props);

    let mut results: Vec<(String, ParquetMetaData)> = Vec::new();
    let mut file_index: usize = 0;
    let mut current_rows: usize = 0;
    let mut writer: Option<AsyncArrowWriter<ParquetObjectWriter>> = None;
    let mut current_filename = String::new();

    let mut pinned_stream = pin!(stream);
    while let Some(batch) = pinned_stream.next().await {
        let batch = batch?;
        let batch = match &transform_plan {
            Some(plan) => transform_batch(&batch, plan, schema.clone())?,
            None => batch,
        };

        let w = match writer.as_mut() {
            Some(w) => w,
            None => {
                current_filename = name_file(file_index);
                let file_path = base_path.clone().join(current_filename.as_str());
                let parq_writer = ParquetObjectWriter::new(store.clone(), file_path);
                writer = Some(AsyncArrowWriter::try_new(
                    parq_writer,
                    schema.clone(),
                    Some(props.clone()),
                )?);
                writer.as_mut().unwrap()
            }
        };

        w.write(&batch).await?;
        current_rows += batch.num_rows();

        let should_rotate = max_rows.map_or(false, |limit| current_rows >= limit)
            || max_bytes.map_or(false, |limit| w.bytes_written() >= limit);

        if should_rotate {
            let metadata = writer.take().unwrap().close().await?;
            results.push((current_filename.clone(), metadata));
            file_index += 1;
            current_rows = 0;
        }
    }

    if let Some(w) = writer.take() {
        let metadata = w.close().await?;
        results.push((current_filename, metadata));
    }

    Ok(results)
}

pub const ROW_NUMBER_COLUMN: &str = "_row_number";

/// Read just the parquet footer metadata from a file in object store.
/// No data is read — only the file's footer (typically < 100KB).
pub async fn read_parquet_metadata(
    store: Arc<dyn ObjectStore>,
    path: &object_store::path::Path,
) -> Result<Arc<ParquetMetaData>, anyhow::Error> {
    let reader = ParquetObjectReader::new(store, path.clone());
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    Ok(builder.metadata().clone())
}

pub async fn stream_projected_parquet(
    store: Arc<dyn ObjectStore>,
    path: &object_store::path::Path,
    columns: &[String],
    row_groups: Option<Vec<usize>>,
) -> Result<
    parquet::arrow::async_reader::ParquetRecordBatchStream<ParquetObjectReader>,
    anyhow::Error,
> {
    let row_number_field = Arc::new(
        Field::new(ROW_NUMBER_COLUMN, DataType::Int64, false).with_extension_type(RowNumber),
    );
    let options = ArrowReaderOptions::new().with_virtual_columns(vec![row_number_field])?;

    let reader = ParquetObjectReader::new(store, path.clone());
    let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options).await?;

    let mask = parquet::arrow::ProjectionMask::columns(
        builder.parquet_schema(),
        columns.iter().map(|s| s.as_str()),
    );

    builder = builder.with_projection(mask);
    if let Some(groups) = row_groups {
        builder = builder.with_row_groups(groups);
    }

    let stream = builder.build()?;
    Ok(stream)
}
