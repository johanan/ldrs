use std::{
    future::Future,
    iter::{zip, Zip},
    process::Stdio,
    sync::Arc,
    vec::IntoIter,
};

use anyhow::Context;
use arrow::ipc::reader::StreamReader;
use arrow_array::RecordBatch;
use arrow_schema::{Field, FieldRef};
use futures::stream::StreamExt;
use tokio::{process::Command, task};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::SyncIoBridge;
use tracing::{debug, info};

use crate::types::{ColumnSchema, ColumnType, TimeUnit};

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
) -> Result<
    MvrStreamResult<
        impl futures::TryStream<Ok = RecordBatch, Error = arrow::error::ArrowError> + Send,
        impl Future<Output = Option<arrow_schema::SchemaRef>> + Send,
    >,
    anyhow::Error,
> {
    let mut cmd = Command::new("ldrs-sf");
    let args = vec!["query", "--sql", sql];

    let cmd = cmd.args(args);
    // log the command for debugging
    info!("Running command: {:?}", cmd);

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
                    debug!("Schema: {:?}", schema);
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

pub fn fill_vec_with_none<'a>(
    fields: &[Arc<Field>],
    mut schemas: Vec<ColumnSchema<'a>>,
) -> Vec<Option<ColumnSchema<'a>>> {
    fields
        .iter()
        .map(|field| {
            let maybe_index = schemas
                .iter()
                .position(|s| s.name().to_lowercase() == field.name().to_lowercase());
            match maybe_index {
                Some(index) => Some(schemas.swap_remove(index)),
                None => None,
            }
        })
        .collect()
}

pub fn get_sf_arrow_schema<'a>(field: &'a FieldRef) -> Option<ColumnSchema<'a>> {
    let name = field.name();
    // get other fields to help parse the type
    let precision = field
        .metadata()
        .get("precision")
        .and_then(|p| p.parse::<i32>().ok());
    let scale = field
        .metadata()
        .get("scale")
        .and_then(|s| s.parse::<i32>().ok());
    let precision_scale = precision.zip(scale);

    let char_length = field
        .metadata()
        .get("charLength")
        .and_then(|l| l.parse::<i32>().ok());

    // finally match on the logical type
    field
        .metadata()
        .get("logicalType")
        .and_then(|log_type| match log_type.as_str() {
            "FIXED" => match precision_scale {
                Some((p, s)) => Some(ColumnSchema::Numeric(name, p, s)),
                None => None,
            },
            "TEXT" => match char_length {
                // this is the max length for text in Snowflake so we treat it as full text
                Some(len) if len == 16777216 => Some(ColumnSchema::Text(name)),
                // this is a postgres limit for varchar, so we treat it as text
                Some(len) if len > 10485760 => Some(ColumnSchema::Text(name)),
                Some(len) => Some(ColumnSchema::Varchar(name, len)),
                None => Some(ColumnSchema::Text(name)),
            },
            "TIMESTAMP_TZ" => match field.data_type() {
                arrow_schema::DataType::Timestamp(time_unit, _) => Some(ColumnSchema::TimestampTz(
                    name,
                    map_arrow_time_to_pq_time(time_unit),
                )),
                _ => None,
            },
            _ => None,
        })
}

impl<'a> TryFrom<&'a FieldRef> for ColumnSchema<'a> {
    type Error = anyhow::Error;

    fn try_from(field: &'a FieldRef) -> Result<Self, Self::Error> {
        let mapping = map_arrow_to_abstract(field);
        mapping.ok_or_else(|| anyhow::anyhow!("Failed to map arrow field to abstract schema"))
    }
}

pub fn map_arrow_to_abstract<'a>(field: &'a FieldRef) -> Option<ColumnSchema<'a>> {
    let name = field.name();
    match field.data_type() {
        arrow_schema::DataType::Utf8 => Some(ColumnSchema::Text(name)),
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
        arrow_schema::DataType::Int8 => Some(ColumnSchema::SmallInt(name)),
        arrow_schema::DataType::Int16 => Some(ColumnSchema::SmallInt(name)),
        arrow_schema::DataType::Int64 => Some(ColumnSchema::BigInt(name)),
        arrow_schema::DataType::Int32 => Some(ColumnSchema::Integer(name)),
        arrow_schema::DataType::Float64 => Some(ColumnSchema::Double(name, None)),
        arrow_schema::DataType::Float32 => Some(ColumnSchema::Real(name)),
        arrow_schema::DataType::Boolean => Some(ColumnSchema::Boolean(name)),
        arrow_schema::DataType::Binary => Some(ColumnSchema::Text(name)),
        arrow_schema::DataType::Date32 => Some(ColumnSchema::Date(name)),
        arrow_schema::DataType::FixedSizeBinary(_) => Some(ColumnSchema::Bytea(name)),
        arrow_schema::DataType::Decimal128(precision, scale) => Some(ColumnSchema::Numeric(
            name,
            (*precision).into(),
            (*scale).into(),
        )),
        _ => None,
    }
}

pub fn build_final_schema<'a>(
    schema: &'a arrow_schema::SchemaRef,
    overrides: Vec<Vec<ColumnSchema<'a>>>,
) -> Result<(Vec<ColumnSchema<'a>>, Vec<Option<ColumnType>>), anyhow::Error> {
    let src_fields = schema
        .fields()
        .iter()
        .filter_map(map_arrow_to_abstract)
        .collect::<Vec<_>>();

    // we need a colschema for every field to process the arrow batch
    anyhow::ensure!(
        src_fields.len() == schema.fields().len(),
        "Columns length does not match schema fields length"
    );

    let source_types = src_fields.iter().map(ColumnType::from).collect::<Vec<_>>();
    let some_src_fields = src_fields.into_iter().map(Some).collect::<Vec<_>>();

    // check arrow metadata for Snowflake logical types
    let sf_fields = schema
        .fields()
        .iter()
        .map(get_sf_arrow_schema)
        .collect::<Vec<_>>();

    // we have a known schema override so do it here
    let init_src = flatten_schema_zip(zip(some_src_fields, sf_fields)).collect();

    let folded = overrides
        .into_iter()
        .fold(init_src, |acc, override_fields| {
            let filled = fill_vec_with_none(&schema.fields, override_fields);
            flatten_schema_zip(zip(acc, filled))
                .into_iter()
                .collect::<Vec<_>>()
        });
    // remove the Option as we should have a full vec
    let final_cols = folded.into_iter().flatten().collect::<Vec<_>>();
    let final_types = final_cols.iter().map(ColumnType::from).collect::<Vec<_>>();
    let transforms = flatten_arrow_transforms_zip(zip(source_types, final_types));
    Ok((final_cols, transforms))
}

pub fn flatten_arrow_transforms_zip(
    zip: Zip<IntoIter<ColumnType>, IntoIter<ColumnType>>,
) -> Vec<Option<ColumnType>> {
    zip.map(|(arr_source, out_col)| match (arr_source, out_col) {
        (a, b) if a == b => None,
        (ColumnType::Text, ColumnType::Varchar(_)) => None,
        // no uuid type in arrow so we always override to uuid
        (_, ColumnType::Uuid) => Some(ColumnType::Uuid),
        (_, ColumnType::Jsonb) => Some(ColumnType::Jsonb),
        (_, b) => Some(b),
    })
    .collect::<Vec<_>>()
}

/*
*
*
*
(Some(ColumnSchema::Numeric(_, precision, scale)), Some(ColumnSchema::Integer(name))) => {
    anyhow::ensure!(scale == 0, "Cannot map numeric with scale to integer");
    anyhow::ensure!(
        precision <= 9,
        "Cannot map numeric with precision > 9 to integer for {}",
        name
    );
    Ok(Some(ColumnSchema::Integer(name)))
}
(Some(ColumnSchema::Numeric(_, precision, scale)), Some(ColumnSchema::BigInt(name))) => {
    anyhow::ensure!(scale == 0, "Cannot map numeric with scale to integer");
    Ok(Some(ColumnSchema::BigInt(name)))
}
*/

pub fn flatten_schema_zip<'a>(
    zip: Zip<IntoIter<Option<ColumnSchema<'a>>>, IntoIter<Option<ColumnSchema<'a>>>>,
) -> IntoIter<Option<ColumnSchema<'a>>> {
    // special combination rules
    zip.map(|(a, b)| match (a, b) {
        // we can output a double, but the underlying type is an integer so we need the scale
        (Some(ColumnSchema::Numeric(_, _, scale)), Some(ColumnSchema::Double(name, None))) => {
            Some(ColumnSchema::Double(name, Some(scale)))
        }
        (Some(_), Some(b)) => Some(b),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    })
    .collect::<Vec<_>>()
    .into_iter()
}
