use std::{future::Future, process::Stdio, vec};

use anyhow::Context;
use arrow::ipc::reader::StreamReader;
use arrow_array::RecordBatch;
use arrow_schema::FieldRef;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use tokio::{process::Command, task};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::SyncIoBridge;
use tracing::{debug, info};

use crate::types::{ColumnSchema, MvrColumn, TimeUnit};

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

fn map_arrow_time_to_pq_time(time_unit: &arrow_schema::TimeUnit) -> TimeUnit {
    match time_unit {
        arrow_schema::TimeUnit::Nanosecond => TimeUnit::Nanos,
        arrow_schema::TimeUnit::Microsecond => TimeUnit::Micros,
        arrow_schema::TimeUnit::Millisecond => TimeUnit::Millis,
        arrow_schema::TimeUnit::Second => TimeUnit::Millis,
    }
}

/// Struct to hold the result of the mvr_to_stream function
///
/// If the command fails, the command_handle will return an error
/// but the batch_stream and schema_stream will still be valid.
/// The schema will have to be checked to see if there are any fields.
/// No fields means the command failed and the underlying error should be
/// checked from the command_handle.
pub struct MvrStreamResult<B, S> {
    pub batch_stream: B,
    pub schema_stream: S,
    pub command_handle: tokio::task::JoinHandle<Result<(), anyhow::Error>>,
}

pub async fn mvr_to_stream(
    sql: &str,
    columns: &Vec<MvrColumn>,
) -> Result<
    MvrStreamResult<
        impl futures::TryStream<Ok = RecordBatch, Error = arrow::error::ArrowError> + Send,
        impl Future<Output = Option<arrow_schema::SchemaRef>> + Send,
    >,
    anyhow::Error,
> {
    let mut cmd = Command::new("mvr");
    let column_json =
        serde_json::to_string(columns).with_context(|| "Failed to serialize columns to JSON")?;
    let args = vec![
        "mv",
        "--format",
        "arrow",
        "--sql",
        sql,
        "-d",
        "--quiet",
        "--batch-size",
        "1024",
        "--columns",
        &column_json,
    ];

    let cmd = cmd.args(args);
    // log the command for debugging
    info!("Running command: {:?}", cmd);

    cmd.env("MVR_DEST", "stdout://");

    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

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
    let (schema_tx, schema_rx) = tokio::sync::oneshot::channel();

    task::spawn_blocking(move || {
        let sync_reader = SyncIoBridge::new(stdout);
        let buf_reader = std::io::BufReader::new(sync_reader);
        let mut stream_reader = StreamReader::try_new(buf_reader, None)
            .with_context(|| "Failed to create StreamReader")?;

        let schema = stream_reader.schema();
        if schema_tx.send(schema).is_err() {
            // The receiver was dropped before we could send
            return Err(anyhow::anyhow!("Failed to send schema: receiver dropped"));
        }

        while let Some(batch_result) = stream_reader.next() {
            debug!("Processing batch");
            // get the error from blocking_send
            if tx.blocking_send(batch_result).is_err() {
                return Err(anyhow::anyhow!("Failed to send batch"));
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

    let batch_stream = ReceiverStream::new(rx).map(|result| result);
    let schema_future = async move {
        match schema_rx.await {
            Ok(schema) => {
                if schema.fields().is_empty() {
                    None
                } else {
                    Some(schema)
                }
            }
            Err(_) => None,
        }
    };

    Ok(MvrStreamResult {
        batch_stream,
        schema_stream: schema_future,
        command_handle,
    })
}

pub fn map_arrow_to_abstract<'a>(field: &'a FieldRef) -> Option<ColumnSchema<'a>> {
    let name = field.name();
    let metadata = field.metadata();
    match field.data_type() {
        arrow_schema::DataType::Utf8 => match metadata.get("type") {
            Some(typ) if typ == "VARCHAR" => {
                let length = metadata
                    .get("length")
                    .and_then(|v| v.parse::<i32>().ok())
                    .unwrap_or(0);
                Some(ColumnSchema::Varchar(name, length))
            }
            Some(typ) if typ == "OBJECT" || typ == "JSON" || typ == "JSONB" => {
                Some(ColumnSchema::Jsonb(name))
            }
            Some(typ) if typ == "UUID" => Some(ColumnSchema::Uuid(name)),
            Some(_) => Some(ColumnSchema::Text(name)),
            None => Some(ColumnSchema::Text(name)),
        },
        arrow_schema::DataType::Timestamp(time_unit, tz) => match tz {
            Some(_) => Some(ColumnSchema::TimestampTz(
                name,
                map_arrow_time_to_pq_time(time_unit),
            )),
            None => Some(ColumnSchema::Timestamp(
                name,
                map_arrow_time_to_pq_time(time_unit),
            )),
        },
        arrow_schema::DataType::Int16 => Some(ColumnSchema::SmallInt(name)),
        arrow_schema::DataType::Int64 => Some(ColumnSchema::BigInt(name)),
        arrow_schema::DataType::Int32 => Some(ColumnSchema::Integer(name)),
        arrow_schema::DataType::Float64 => Some(ColumnSchema::Double(name)),
        arrow_schema::DataType::Float32 => Some(ColumnSchema::Real(name)),
        arrow_schema::DataType::Boolean => Some(ColumnSchema::Boolean(name)),
        arrow_schema::DataType::Binary => Some(ColumnSchema::Text(name)),
        arrow_schema::DataType::Date32 => Some(ColumnSchema::Date(name)),
        arrow_schema::DataType::FixedSizeBinary(16) => Some(ColumnSchema::Uuid(name)),
        arrow_schema::DataType::Decimal128(precision, scale) => Some(ColumnSchema::Numeric(
            name,
            (*precision).into(),
            (*scale).into(),
        )),
        _ => None,
    }
}
