use std::io::{BufWriter, Stdout, Write};
use std::pin::pin;

use arrow::ipc::writer::StreamWriter;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::{StreamExt, TryStream, TryStreamExt};
use ldrs_arrow::{transform_batch, ArrowColumnTransformStrategy};
use ldrs_delta::{DeltaMergeSink, DeltaOverwriteSink};
use ldrs_parquet::ParquetSink;

use crate::postgres::execute::PgSink;

/// Streams Arrow record batches to `writer` as an Arrow IPC stream.
pub struct ArrowStdoutSink<W: Write = Stdout> {
    writer: StreamWriter<BufWriter<W>>,
    schema: SchemaRef,
    transform_plan: Option<Vec<Option<ArrowColumnTransformStrategy>>>,
}

impl<W: Write> ArrowStdoutSink<W> {
    pub fn new(
        writer: W,
        schema: SchemaRef,
        transforms: Vec<Option<ArrowColumnTransformStrategy>>,
    ) -> Result<Self, anyhow::Error> {
        let transform_plan = if transforms.iter().any(|s| s.is_some()) {
            Some(transforms)
        } else {
            None
        };
        let writer = StreamWriter::try_new_buffered(writer, &schema)?;
        Ok(ArrowStdoutSink {
            writer,
            schema,
            transform_plan,
        })
    }

    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), anyhow::Error> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let transformed;
        let batch = match &self.transform_plan {
            Some(plan) => {
                transformed = transform_batch(batch, plan, self.schema.clone())?;
                &transformed
            }
            None => batch,
        };

        self.writer.write(batch)?;
        Ok(())
    }

    /// Writes the end-of-stream marker and flushes.
    pub fn finish(mut self) -> Result<(), anyhow::Error> {
        self.writer.finish()?;
        Ok(())
    }

    /// Failure path. Bytes already written to the pipe cannot be recalled, so there
    /// is nothing to undo; the stream is dropped without its end-of-stream marker.
    pub fn abort(self) {}
}

/// One destination in a fan-out, held as a single concrete type so a run can drive
/// a `Vec<Sink>` through one loop. `write_batch` is the only operation that is
/// uniform across destinations; construction, finish, and abort differ per
/// destination and live in their own match arms (in orchestration, where the
/// transaction/command context is in scope).
pub enum Sink {
    Pg(PgSink),
    Pq(ParquetSink),
    DeltaOverwrite(DeltaOverwriteSink),
    DeltaMerge(DeltaMergeSink),
    Arrow(ArrowStdoutSink),
}

impl Sink {
    /// Write one batch to this destination, delegating to the engine sink's own
    /// `write_batch`. The only method shared across every variant.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), anyhow::Error> {
        match self {
            Sink::Pg(s) => s.write_batch(batch).await,
            Sink::Pq(s) => s.write_batch(batch).await,
            Sink::DeltaOverwrite(s) => s.write_batch(batch).await,
            Sink::DeltaMerge(s) => s.write_batch(batch).await,
            Sink::Arrow(s) => s.write_batch(batch),
        }
    }
}

/// Drive one source stream into every sink, sequential per batch: each batch is
/// written to all sinks before the next is pulled
pub async fn drive<S>(stream: S, sinks: &mut [Sink]) -> Result<(), anyhow::Error>
where
    S: TryStream<Ok = RecordBatch, Error = anyhow::Error>,
{
    let stream = stream.into_stream();
    let mut stream = pin!(stream);
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        for sink in sinks.iter_mut() {
            sink.write_batch(&batch).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::{Arc, Mutex};

    use arrow::ipc::reader::StreamReader;
    use arrow_array::{Array, Int32Array, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn arrow_stdout_sink_is_send() {
        // The Sink enum holds ArrowStdoutSink<Stdout>; keeping it Send keeps the
        // whole enum Send. A new non-Send field must fail here.
        fn assert_send<T: Send + 'static>() {}
        assert_send::<ArrowStdoutSink>();
    }

    /// Write target that keeps the bytes so a test can read the stream back.
    #[derive(Clone)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl SharedBuf {
        fn new() -> Self {
            SharedBuf(Arc::new(Mutex::new(Vec::new())))
        }
        fn bytes(&self) -> Vec<u8> {
            self.0.lock().unwrap().clone()
        }
    }

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn int_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)]))
    }

    fn int_batch(schema: &SchemaRef, vals: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vals))]).unwrap()
    }

    fn read_back(bytes: Vec<u8>) -> (SchemaRef, Vec<RecordBatch>) {
        let reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        let schema = reader.schema();
        let batches = reader.collect::<Result<Vec<_>, _>>().unwrap();
        (schema, batches)
    }

    #[test]
    fn writes_single_batch() {
        let schema = int_schema();
        let buf = SharedBuf::new();
        let mut sink = ArrowStdoutSink::new(buf.clone(), schema.clone(), vec![None]).unwrap();
        sink.write_batch(&int_batch(&schema, vec![1, 2, 3]))
            .unwrap();
        sink.finish().unwrap();

        let (out_schema, batches) = read_back(buf.bytes());
        assert_eq!(out_schema.fields(), schema.fields());
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn preserves_multiple_batches_in_order() {
        let schema = int_schema();
        let buf = SharedBuf::new();
        let mut sink = ArrowStdoutSink::new(buf.clone(), schema.clone(), vec![None]).unwrap();
        sink.write_batch(&int_batch(&schema, vec![1, 2])).unwrap();
        sink.write_batch(&int_batch(&schema, vec![3])).unwrap();
        sink.finish().unwrap();

        let (_, batches) = read_back(buf.bytes());
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
    }

    #[test]
    fn empty_stream_is_schema_only() {
        let schema = int_schema();
        let buf = SharedBuf::new();
        let sink = ArrowStdoutSink::new(buf.clone(), schema.clone(), vec![None]).unwrap();
        sink.finish().unwrap();

        let (out_schema, batches) = read_back(buf.bytes());
        assert_eq!(out_schema.fields(), schema.fields());
        assert!(batches.is_empty());
    }

    #[test]
    fn skips_zero_row_batches() {
        let schema = int_schema();
        let buf = SharedBuf::new();
        let mut sink = ArrowStdoutSink::new(buf.clone(), schema.clone(), vec![None]).unwrap();
        sink.write_batch(&RecordBatch::new_empty(schema.clone()))
            .unwrap();
        sink.finish().unwrap();

        let (_, batches) = read_back(buf.bytes());
        assert!(batches.is_empty());
    }

    #[test]
    fn applies_transform_to_output_schema() {
        let source = int_schema();
        let target: SchemaRef = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
        let buf = SharedBuf::new();
        let strategies = vec![Some(ArrowColumnTransformStrategy::ArrowCast {
            target_type: DataType::Int64,
        })];
        let mut sink = ArrowStdoutSink::new(buf.clone(), target.clone(), strategies).unwrap();
        sink.write_batch(&int_batch(&source, vec![7, 8])).unwrap();
        sink.finish().unwrap();

        let (out_schema, batches) = read_back(buf.bytes());
        assert_eq!(out_schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(batches.len(), 1);
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 7);
        assert_eq!(col.value(1), 8);
    }

    #[test]
    fn abort_is_a_noop() {
        let schema = int_schema();
        let buf = SharedBuf::new();
        let mut sink = ArrowStdoutSink::new(buf.clone(), schema.clone(), vec![None]).unwrap();
        sink.write_batch(&int_batch(&schema, vec![1])).unwrap();
        sink.abort();
        // No panic, nothing to assert beyond a clean return.
    }
}
