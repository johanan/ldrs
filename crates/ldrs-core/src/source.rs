//! Source opening: turn a resolved [`SourceSpec`] into a running batch stream plus the schema and
//! columns the sinks are built against. [`StreamType`] is the union of source streams the executor
//! drives.

use std::pin::pin;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::Stream;
use ldrs_arrow::ColumnSpec;
use ldrs_parquet::{builder_from_url, columnspec_from_parquet, get_fields};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStream};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use url::Url;

use crate::plan::SourceSpec;
use crate::spawn::{resolve_schema, spawn_arrow_source};

/// The source stream shapes the executor drives: a parquet file stream, or a pumped receiver
/// (spawned process today; FFI / stdin later).
pub enum StreamType {
    Parquet(ParquetRecordBatchStream<ParquetObjectReader>),
    Receiver(ReceiverStream<Result<RecordBatch, anyhow::Error>>),
}

impl Stream for StreamType {
    type Item = Result<RecordBatch, anyhow::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.get_mut() {
            StreamType::Parquet(stream) => pin!(stream)
                .poll_next(cx)
                .map(|opt| opt.map(|res| res.map_err(Into::into))),
            StreamType::Receiver(stream) => pin!(stream).poll_next(cx),
        }
    }
}

/// An opened source: its schema (`None` if the stream never produced one), the batch stream, the
/// source columns, the cleanup handle carrying the settlement verdict, and the files loaded (for
/// `PhaseOutput`).
pub struct SrcStream {
    pub schema: Option<SchemaRef>,
    pub stream_type: StreamType,
    pub source_cols: Vec<ColumnSpec>,
    pub cleanup_handle: Option<JoinHandle<Result<(), anyhow::Error>>>,
    pub source_files: Option<Vec<String>>,
}

/// Open a resolved source: build the object store / spawn the process and pump batches, producing
/// the schema and columns the sinks are built against.
pub async fn open_source(
    spec: SourceSpec,
    cloud_io_rt: &tokio::runtime::Handle,
) -> Result<SrcStream, anyhow::Error> {
    match spec {
        SourceSpec::File { url } => {
            let src_url = Url::parse(&url)?;
            let builder = builder_from_url(src_url, cloud_io_rt.clone()).await?;
            let schema = builder.schema().clone();
            let file_md = builder.metadata().clone();
            let fields = get_fields(file_md.file_metadata())?;
            // matches DuckDB STANDARD_VECTOR_SIZE
            let stream = builder.with_batch_size(2048).build()?;
            let source_cols = fields
                .iter()
                .filter_map(columnspec_from_parquet)
                .collect::<Vec<_>>();
            Ok(SrcStream {
                schema: Some(schema),
                stream_type: StreamType::Parquet(stream),
                source_cols,
                cleanup_handle: None,
                source_files: Some(vec![url]),
            })
        }
        SourceSpec::Spawned(spawned) => {
            let arrow_stream = spawn_arrow_source(spawned);
            let schema = resolve_schema(arrow_stream.schema_rx).await;
            Ok(SrcStream {
                schema,
                stream_type: StreamType::Receiver(arrow_stream.batch_stream),
                source_cols: vec![],
                cleanup_handle: Some(arrow_stream.command_handle),
                source_files: None,
            })
        }
    }
}
