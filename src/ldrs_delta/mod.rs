use arrow_array::RecordBatch;
use delta_kernel::engine::default::executor::TaskExecutor;
use delta_kernel::{engine::arrow_data::ArrowEngineData, DeltaResult, EngineData};

use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use std::sync::Arc;

struct BlockingStreamIterator<
    T: Send + 'static,
    E: delta_kernel::engine::default::executor::TaskExecutor,
> {
    stream: Option<BoxStream<'static, T>>,
    task_executor: Arc<E>,
}

impl<T: Send + 'static, E: delta_kernel::engine::default::executor::TaskExecutor> Iterator
    for BlockingStreamIterator<T, E>
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        // Move the stream into the future so we can block on it
        let mut stream = self.stream.take()?;
        let (item, stream) = self
            .task_executor
            .block_on(async move { (stream.next().await, stream) });

        // We must not poll an exhausted stream after it returned None
        if item.is_some() {
            self.stream = Some(stream);
        }

        item
    }
}

fn stream_to_iter<S, T, E>(stream: S, task_executor: Arc<E>) -> Box<dyn Iterator<Item = T> + Send>
where
    S: Stream<Item = T> + Send + 'static,
    T: Send + 'static,
    E: TaskExecutor,
{
    Box::new(BlockingStreamIterator {
        stream: Some(stream.boxed()),
        task_executor,
    })
}

struct TakeRowsUntilThreshold<I> {
    inner: I,
    rows_remaining: usize,
}

impl<I> Iterator for TakeRowsUntilThreshold<I>
where
    I: Iterator<Item = Result<RecordBatch, anyhow::Error>>,
{
    type Item = DeltaResult<Box<dyn EngineData>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rows_remaining == 0 {
            return None; // Threshold reached, stop this chunk
        }

        match self.inner.next()? {
            Ok(batch) => {
                let batch_rows = batch.num_rows();
                println!("Processing batch with {} rows", batch_rows);

                // Always emit the batch
                // Subtract rows AFTER deciding to emit
                self.rows_remaining = self.rows_remaining.saturating_sub(batch_rows);
                println!("Rows remaining: {}", self.rows_remaining);

                Some(Ok(
                    Box::new(ArrowEngineData::new(batch)) as Box<dyn EngineData>
                ))
            }
            Err(e) => Some(Err(delta_kernel::Error::generic(e.to_string()))),
        }
    }
}

trait TakeRowsExt: Iterator {
    fn take_rows_until_threshold(self, threshold: usize) -> TakeRowsUntilThreshold<Self>
    where
        Self: Sized,
    {
        TakeRowsUntilThreshold {
            inner: self,
            rows_remaining: threshold,
        }
    }
}

impl<I> TakeRowsExt for I where I: Iterator<Item = Result<RecordBatch, anyhow::Error>> {}

pub fn process_delta<S, E: delta_kernel::engine::default::executor::TaskExecutor>(
    stream: S,
    task_executor: Arc<E>,
    rows_per_file: usize,
) -> DeltaResult<()>
where
    S: Stream<Item = Result<RecordBatch, anyhow::Error>> + Send + 'static,
{
    // Convert stream to blocking iterator
    let mut big_iter = stream_to_iter(stream, task_executor);

    let mut file_idx = 0;
    loop {
        let mut chunk_iter = big_iter.by_ref().take_rows_until_threshold(rows_per_file);

        let first = match chunk_iter.next() {
            Some(item) => item,
            None => break,
        };

        println!("Would write file_{:05}.parquet", file_idx);

        let combined_chunk = std::iter::once(first).chain(chunk_iter);
        for result in combined_chunk {
            let _engine_data = result?;
            println!("Processed record batch");
        }

        file_idx += 1;
    }

    println!("Wrote {} files", file_idx);
    Ok(())
}
