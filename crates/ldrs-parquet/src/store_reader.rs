use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, SpawnedReader};
use parquet::errors::{ParquetError, Result};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use std::ops::Range;
use std::sync::Arc;
use tokio::runtime::Handle;

pub type SpawnedStoreReader = SpawnedReader<StoreReader>;

/// `AsyncFileReader` over an object-store location with a known file size.
#[derive(Clone, Debug)]
pub struct StoreReader {
    store: Arc<dyn ObjectStore>,
    path: Path,
    size: u64,
}

impl StoreReader {
    pub fn spawned(
        store: Arc<dyn ObjectStore>,
        path: Path,
        size: u64,
        handle: Handle,
    ) -> SpawnedStoreReader {
        SpawnedReader::new(StoreReader { store, path, size }, handle)
    }
}

fn to_parquet_err(e: object_store::Error) -> ParquetError {
    ParquetError::External(Box::new(e))
}

impl AsyncFileReader for StoreReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        self.store
            .get_range(&self.path, range)
            .map_err(to_parquet_err)
            .boxed()
    }

    fn get_byte_ranges(&mut self, ranges: Vec<Range<u64>>) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        async move {
            self.store
                .get_ranges(&self.path, &ranges)
                .await
                .map_err(to_parquet_err)
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        let size = self.size;
        async move {
            let metadata = ParquetMetaDataReader::new()
                .with_arrow_reader_options(options)
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}
