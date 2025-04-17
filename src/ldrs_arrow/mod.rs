use std::{pin::{pin, Pin}, process::Stdio, vec};

use anyhow::Context;
use arrow::ipc::reader::StreamReader;
use arrow_array::RecordBatch;
use arrow_schema::FieldRef;
use clap::Args;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use parquet::format::{MicroSeconds, NanoSeconds};
use tokio::{process::Command, task};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::SyncIoBridge;

use crate::types::ColumnSchema;

#[derive(Args, Debug)]
pub struct ArrowIpcStreamArgs {
    #[arg(short, long)]
    pub full_table: String,
}

pub async fn print_arrow_ipc_batches<S>(stream: S) -> Result<(), anyhow::Error>
where
    S: futures::TryStream<Ok = RecordBatch, Error = arrow::error::ArrowError> + Send,
{
    stream
        .try_for_each(|batch| async move {
            println!("{:?}", batch);
            Ok(())
        })
        .await
        .with_context(|| "Failed to process Arrow IPC stream")
}

fn map_arrow_time_to_pq_time(time_unit: &arrow_schema::TimeUnit) -> parquet::basic::TimeUnit {
    match time_unit {
        arrow_schema::TimeUnit::Nanosecond => parquet::basic::TimeUnit::NANOS(NanoSeconds {}),
        arrow_schema::TimeUnit::Microsecond => parquet::basic::TimeUnit::MICROS(MicroSeconds {}),
        arrow_schema::TimeUnit::Millisecond => {
            parquet::basic::TimeUnit::MILLIS(parquet::format::MilliSeconds {})
        }
        arrow_schema::TimeUnit::Second => {
            parquet::basic::TimeUnit::MILLIS(parquet::format::MilliSeconds {})
        }
    }
}

pub fn map_to_pq_types<'a>(arrow_field: &'a FieldRef) -> Option<ColumnSchema<'a>> {
    match arrow_field.data_type() {
        arrow_schema::DataType::Utf8 => Some(ColumnSchema::Text(arrow_field.name())),
        arrow_schema::DataType::Timestamp(time_unit, tz) => match tz {
            Some(_) => Some(ColumnSchema::TimestampTz(
                arrow_field.name(),
                map_arrow_time_to_pq_time(time_unit),
            )),
            None => Some(ColumnSchema::Timestamp(
                arrow_field.name(),
                map_arrow_time_to_pq_time(time_unit),
            )),
        },
        arrow_schema::DataType::Int64 => Some(ColumnSchema::BigInt(arrow_field.name())),
        arrow_schema::DataType::Int32 => Some(ColumnSchema::Integer(arrow_field.name())),
        arrow_schema::DataType::Float64 => Some(ColumnSchema::Double(arrow_field.name())),
        arrow_schema::DataType::Float32 => Some(ColumnSchema::Real(arrow_field.name())),
        arrow_schema::DataType::Boolean => Some(ColumnSchema::Boolean(arrow_field.name())),
        arrow_schema::DataType::Binary => Some(ColumnSchema::Text(arrow_field.name())),
        arrow_schema::DataType::Date32 => Some(ColumnSchema::Integer(arrow_field.name())),
        _ => None,
    }
}

pub struct MvrStreamResult<S> {
    pub stream: S,
    pub command_handle: tokio::task::JoinHandle<Result<(), anyhow::Error>>,
}

pub async fn mvr_to_stream(full_table: &str) -> Result<
    MvrStreamResult<
        impl futures::TryStream<Ok = RecordBatch, Error = arrow::error::ArrowError> + Send,
    >,
    anyhow::Error,
> {
    let mut cmd = Command::new("mvr");
    let args = vec!["mv", "--format", "arrow", "--name", full_table, "-d"];

    cmd.env("MVR_DEST", "stdout://");

    cmd.args(args).stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .with_context(|| format!("Failed to spawn mvr"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout"))?;

    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("Failed to capture stderr"))?;

    let (tx, rx) = tokio::sync::mpsc::channel(16);
    task::spawn_blocking(move || {
        let sync_reader = SyncIoBridge::new(stdout);
        let buf_reader = std::io::BufReader::new(sync_reader);
        let mut stream_reader = StreamReader::try_new(buf_reader, None)
            .with_context(|| "Failed to create StreamReader")?;

        while let Some(batch_result) = stream_reader.next() {
            println!("Processing batch");
            // If send fails (receiver dropped), stop processing
            if tx.blocking_send(batch_result).is_err() {
                break;
            }
        }

        Ok::<(), anyhow::Error>(())
    });

    let command_handle = tokio::spawn(async move {
        let status = child.wait().await?;

        if !status.success() {
            // Read stderr only if command failed
            let mut stderr_reader = tokio::io::BufReader::new(stderr);
            let mut stderr_output = String::new();
            tokio::io::AsyncReadExt::read_to_string(&mut stderr_reader, &mut stderr_output).await?;

            return Err(anyhow::anyhow!(
                "Command failed with stderr: {}",
                stderr_output
            ));
        }

        Ok(())
    });

    let stream = ReceiverStream::new(rx).map(|result| result);
    Ok(MvrStreamResult {
        stream,
        command_handle,
    })
}

pub async fn peek_arrow_stream<S>(
    stream: S,
) -> Result<(
    arrow_schema::SchemaRef,
    impl futures::TryStream<Ok = RecordBatch, Error = S::Error> + Send,
), anyhow::Error>
where
    S: futures::TryStream<Ok = RecordBatch, Error = arrow::error::ArrowError> + Send + 'static,
{
    use futures::{StreamExt, TryStreamExt};

    let mut stream = Box::pin(stream.into_stream());
    // Get the first batch
    let first_batch_result = stream.next().await
        .ok_or_else(|| anyhow::anyhow!("Arrow IPC stream contained no batches"))?;
    
    // Check if the first batch is valid
    let first_batch = first_batch_result
        .map_err(|e| anyhow::anyhow!("Error reading first batch: {}", e))?;
    
    // Clone the schema so we fully own it
    let schema = first_batch.schema();
    
    // Create a stream starting with first batch, followed by the rest
    let first_batch_stream = futures::stream::once(async move { Ok(first_batch) });
    let combined_stream = first_batch_stream.chain(stream);
    
    Ok((schema, combined_stream))
}