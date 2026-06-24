use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use ldrs_arrow::{transform_batch, ArrowColumnTransformStrategy};
use ldrs_storage::{base_or_relative_path, build_store};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::async_writer::ParquetObjectWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tracing::warn;

use crate::default_writer_props;

/// Produces the filename for the file at `index`. Boxed so every sink carries the
/// same nameable type; `Send + 'static` so a sink can be moved into a spawned task.
pub type FileNamer = Box<dyn Fn(usize) -> Result<String, anyhow::Error> + Send + 'static>;

pub struct ParquetSink {
    store: Arc<dyn ObjectStore>,
    base_path: object_store::path::Path,
    props: WriterProperties,
    schema: SchemaRef,
    transform_plan: Option<Vec<Option<ArrowColumnTransformStrategy>>>,
    max_rows: Option<usize>,
    max_bytes: Option<usize>,
    namer: FileNamer,
    // running state the split-writer loop locals
    writer: Option<AsyncArrowWriter<ParquetObjectWriter>>,
    results: Vec<(String, ParquetMetaData)>,
    file_index: usize,
    current_rows: usize,
    current_filename: String,
}

impl ParquetSink {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dir_path: &str,
        schema: SchemaRef,
        transforms: Vec<Option<ArrowColumnTransformStrategy>>,
        max_rows: Option<usize>,
        max_bytes: Option<usize>,
        namer: FileNamer,
        writer_props: Option<WriterProperties>,
    ) -> Result<Self, anyhow::Error> {
        let url = base_or_relative_path(dir_path)?;
        let (store, base_path, _) = build_store(&url)?;

        // namer(0) validates the template before any data moves. When rotation is
        // possible, consecutive names must differ or every rotation silently
        // overwrites the same file.
        let first = namer(0)?;
        if max_rows.is_some() || max_bytes.is_some() {
            let second = namer(1)?;
            anyhow::ensure!(
                first != second,
                "file name template produces the same name for consecutive files ({}); \
                 rotation would overwrite it — add {{{{ index }}}} to the template",
                first
            );
        }

        let transform_plan = if transforms.iter().any(|s| s.is_some()) {
            Some(transforms)
        } else {
            None
        };

        Ok(ParquetSink {
            store,
            base_path,
            props: writer_props.unwrap_or_else(default_writer_props),
            schema,
            transform_plan,
            max_rows,
            max_bytes,
            namer,
            writer: None,
            results: Vec::new(),
            file_index: 0,
            current_rows: 0,
            current_filename: String::new(),
        })
    }

    /// The object store this sink writes through. Cloning the `Arc` shares the
    /// same underlying client; a sink that embeds this one (e.g. `DeltaOverwriteSink`)
    /// reuses it for its own artifacts rather than building a second client.
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    /// Base path the sink's files are written under, for callers that need to
    /// address those files afterward (e.g. Delta stats).
    pub fn base_path(&self) -> &object_store::path::Path {
        &self.base_path
    }

    /// One iteration of the split-writer loop: transform, write, rotate.
    /// Zero-row batches are skipped so that "no rows" produces "no files"
    /// regardless of how the source chunks an empty result.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), anyhow::Error> {
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

        if self.writer.is_none() {
            self.current_filename = (self.namer)(self.file_index)?;
            let file_path = join_relative(&self.base_path, &self.current_filename);
            let parq_writer = ParquetObjectWriter::new(self.store.clone(), file_path);
            self.writer = Some(AsyncArrowWriter::try_new(
                parq_writer,
                self.schema.clone(),
                Some(self.props.clone()),
            )?);
        }
        let w = self.writer.as_mut().unwrap();

        w.write(batch).await?;
        self.current_rows += batch.num_rows();

        let should_rotate = self
            .max_rows
            .is_some_and(|limit| self.current_rows >= limit)
            || self
                .max_bytes
                .is_some_and(|limit| w.bytes_written() >= limit);

        if should_rotate {
            let metadata = self.writer.take().unwrap().close().await?;
            self.results
                .push((std::mem::take(&mut self.current_filename), metadata));
            self.file_index += 1;
            self.current_rows = 0;
        }
        Ok(())
    }

    /// Trailing flush. A zero-row stream opened no writer, so this returns an
    /// empty vec and writes nothing. If the final close fails, the files already
    /// completed are deleted before returning the error.
    pub async fn finish(mut self) -> Result<Vec<(String, ParquetMetaData)>, anyhow::Error> {
        if let Some(w) = self.writer.take() {
            match w.close().await {
                Ok(metadata) => self.results.push((self.current_filename, metadata)),
                Err(e) => {
                    self.delete_completed().await;
                    return Err(e.into());
                }
            }
        }
        Ok(self.results)
    }

    /// Marker capability for zero-row streams: writes one schema-only, zero-row
    /// file named `namer(0)`. The orchestrator calls this instead of `finish`
    /// when no rows arrived and the destination asked for a marker file.
    pub async fn finish_empty(self) -> Result<Vec<(String, ParquetMetaData)>, anyhow::Error> {
        anyhow::ensure!(
            self.writer.is_none() && self.results.is_empty(),
            "finish_empty called on a sink that has written rows"
        );
        let filename = (self.namer)(0)?;
        let file_path = join_relative(&self.base_path, &filename);
        let parq_writer = ParquetObjectWriter::new(self.store, file_path);
        let writer = AsyncArrowWriter::try_new(parq_writer, self.schema, Some(self.props))?;
        let metadata = writer.close().await?;
        Ok(vec![(filename, metadata)])
    }

    async fn delete_completed(&self) {
        for (filename, _) in &self.results {
            let path = join_relative(&self.base_path, filename);
            if let Err(e) = self.store.delete(&path).await {
                warn!("could not delete {}: {}", filename, e);
            }
        }
    }

    /// Failure path: drop the in-flight writer (an unfinished multipart upload
    /// never becomes a visible object), then delete the completed files.
    pub async fn abort(mut self) {
        drop(self.writer.take());
        self.delete_completed().await;
    }
}

/// Append a slash-separated relative path onto `base`. `Path::from` splits the
/// relative part on `/` (so it may name subdirectories) and drops empty segments,
/// so a stray leading/trailing/double slash is ignored; its parts are chained onto
/// `base`'s, since object_store's `Path` has no path-to-path join (only segment).
fn join_relative(base: &Path, relative: &str) -> Path {
    base.parts().chain(Path::from(relative).parts()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parquet_sink_is_send() {
        // Spawnability (`tokio::spawn` needs Send + 'static) is load-bearing for
        // the future parallel-COPY path; a new non-Send field must fail here.
        fn assert_send<T: Send + 'static>() {}
        assert_send::<ParquetSink>();
    }
}
