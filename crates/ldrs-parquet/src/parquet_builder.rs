use std::sync::Arc;

use anyhow::{Context, Error};
use arrow_schema::{DataType, SchemaBuilder, SchemaRef, TimeUnit};
use ldrs_storage::{base_or_relative_path, build_store};
use object_store::ObjectStoreExt;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::basic::Type as PhysicalType;
use parquet::schema::types::Type;
use tokio::runtime::Handle;
use url::Url;

use crate::store_reader::{SpawnedStoreReader, StoreReader};

pub(crate) async fn open_builder(
    mut reader: SpawnedStoreReader,
    options: ArrowReaderOptions,
) -> Result<ParquetRecordBatchStreamBuilder<SpawnedStoreReader>, anyhow::Error> {
    // Loaded without `options` so the schema holds only the file's columns, as a hint must.
    let file = ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::new()).await?;
    let options = match int96_as_micros(&file) {
        Some(schema) => options.with_schema(schema),
        None => options,
    };
    let metadata = ArrowReaderMetadata::try_new(file.metadata().clone(), options)?;
    Ok(ParquetRecordBatchStreamBuilder::new_with_metadata(
        reader, metadata,
    ))
}

/// The schema with each top-level INT96 column as `Timestamp(µs, None)`, or `None` if it has none.
fn int96_as_micros(metadata: &ArrowReaderMetadata) -> Option<SchemaRef> {
    let int96: Vec<usize> = metadata
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema()
        .get_fields()
        .iter()
        .enumerate()
        .filter_map(|(i, column)| match column.as_ref() {
            Type::PrimitiveType {
                physical_type: PhysicalType::INT96,
                ..
            } => Some(i),
            _ => None,
        })
        .collect();
    if int96.is_empty() {
        return None;
    }

    let mut schema = SchemaBuilder::from(metadata.schema().as_ref());
    for i in int96 {
        let field = schema.field_mut(i);
        *field = Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, None)),
        );
    }
    Some(Arc::new(schema.finish()))
}

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

    let builder = open_builder(reader, ArrowReaderOptions::new())
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
