//! Drive a delta sink from a stream, the way the executor would without the rest of the load.

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::{Stream, StreamExt};
use ldrs_delta::{
    ensure_table, DeltaMergeSink, DeltaOverwriteSink, MergeConfig, MergeStats, OperationConfig,
};
use tokio::runtime::Handle;

pub async fn overwrite_delta<S>(
    table_path: &str,
    schema: SchemaRef,
    stream: S,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
    config: &OperationConfig,
    cloud_io: &Handle,
) -> Result<(), anyhow::Error>
where
    S: Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + 'static,
{
    let mut sink = DeltaOverwriteSink::new(
        table_path,
        schema.clone(),
        max_rows,
        max_bytes,
        config,
        cloud_io,
    )?;
    ensure_table(table_path, &schema, config).await?;
    let mut stream = std::pin::pin!(stream);
    while let Some(batch) = stream.next().await {
        sink.write_batch(&batch?).await?;
    }
    sink.finish().await
}

// delta_integration drives only the overwrite sink
#[allow(dead_code)]
pub async fn merge_delta<S>(
    table_path: &str,
    schema: SchemaRef,
    stream: S,
    merge_config: MergeConfig,
    config: &OperationConfig,
    cloud_io: &Handle,
) -> Result<MergeStats, anyhow::Error>
where
    S: Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + 'static,
{
    let mut sink = DeltaMergeSink::new(table_path, schema.clone(), merge_config, config, cloud_io)?;
    ensure_table(table_path, &schema, config).await?;
    let mut stream = std::pin::pin!(stream);
    while let Some(batch) = stream.next().await {
        sink.write_batch(&batch?).await?;
    }
    sink.finish().await
}
