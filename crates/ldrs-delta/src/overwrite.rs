use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use ldrs_parquet::{default_writer_props, FileNamer, ParquetSink};
use ldrs_storage::base_or_relative_path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use parquet::file::metadata::ParquetMetaData;
use url::Url;
use uuid::Uuid;

use crate::{
    build_add, build_engine, build_overwrite_commit, cleanup_source_files, snapshot_table_state,
    version_to_log_filename, MAX_COMMIT_RETRIES,
};

/// Streaming Delta overwrite. Writes data files through an embedded [`ParquetSink`]
/// and commits them to the `_delta_log` on `finish`. The table must already exist.
pub struct DeltaOverwriteSink {
    inner: ParquetSink,
    // shared with `inner`: one object-store client for both the data writes and the commit
    store: Arc<dyn ObjectStore>,
    base_path: object_store::path::Path,
    url: Url,
    schema: SchemaRef,
}

impl DeltaOverwriteSink {
    pub fn new(
        table_path: &str,
        schema: SchemaRef,
        max_rows: Option<usize>,
        max_bytes: Option<usize>,
    ) -> Result<Self, anyhow::Error> {
        let url = base_or_relative_path(table_path)?;
        let namer: FileNamer = Box::new(|_| Ok(format!("{}.parquet", Uuid::new_v4())));
        let inner = ParquetSink::new(
            table_path,
            schema.clone(),
            max_rows,
            max_bytes,
            namer,
            Some(default_writer_props()),
        )?;
        let store = inner.store();
        let base_path = inner.base_path().clone();
        Ok(Self {
            inner,
            store,
            base_path,
            url,
            schema,
        })
    }

    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), anyhow::Error> {
        self.inner.write_batch(batch).await
    }

    /// Flush the data files and commit them. The files are kept only if the
    /// commit succeeds; on any failure they are deleted. An empty stream commits
    /// removes with no adds, truncating the table.
    pub async fn finish(self) -> Result<(), anyhow::Error> {
        let files = self.inner.finish().await?;
        match commit_overwrite(
            &self.store,
            &self.base_path,
            &self.url,
            &self.schema,
            &files,
        )
        .await
        {
            Ok(()) => Ok(()),
            Err(e) => {
                cleanup_source_files(&self.store, &self.base_path, &files).await;
                Err(e)
            }
        }
    }

    /// Delete the data files written so far. No commit was made, so there is no
    /// log entry to undo.
    pub async fn abort(self) {
        self.inner.abort().await;
    }
}

/// Commit the written data files as a Delta overwrite. Ok means the commit
/// landed; on any failure the caller decides what to clean up.
async fn commit_overwrite(
    store: &Arc<dyn ObjectStore>,
    base_path: &object_store::path::Path,
    url: &Url,
    schema: &SchemaRef,
    files: &[(String, ParquetMetaData)],
) -> Result<(), anyhow::Error> {
    let file_paths: Vec<_> = files
        .iter()
        .map(|(filename, _)| base_path.clone().join(filename.as_str()))
        .collect();

    let obj_metas =
        futures::future::try_join_all(file_paths.iter().map(|path| store.head(path))).await?;

    let adds = files
        .iter()
        .zip(obj_metas.iter())
        .map(|((filename, metadata), obj_meta)| build_add(filename, metadata, obj_meta, schema))
        .collect::<Result<Vec<_>, _>>()?;

    let engine = build_engine(store.clone());

    for _attempt in 0..MAX_COMMIT_RETRIES {
        let table_state = snapshot_table_state(engine.as_ref(), url)?;
        let (commit_body, next_version) = build_overwrite_commit(&table_state, schema, &adds)?;
        let log_path = base_path
            .clone()
            .join("_delta_log")
            .join(version_to_log_filename(next_version));

        match store
            .put_opts(
                &log_path,
                PutPayload::from(commit_body),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => continue,
            Err(e) => return Err(e.into()),
        }
    }
    anyhow::bail!("failed to commit delta overwrite after {MAX_COMMIT_RETRIES} attempts")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_overwrite_sink_is_send() {
        fn assert_send<T: Send + 'static>() {}
        assert_send::<DeltaOverwriteSink>();
    }
}
