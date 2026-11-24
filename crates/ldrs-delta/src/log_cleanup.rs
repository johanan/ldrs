//! Delete `_delta_log` entries the table's log retention has expired.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, NaiveTime, Utc};
use delta_kernel::history_manager::error::LogHistoryError;
use delta_kernel::history_manager::{HistoryCommitType, latest_version_as_of};
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use delta_kernel::{Engine, KernelError, Snapshot, SnapshotRef, Version};
use futures::{StreamExt, stream};
use object_store::ObjectStore;
use object_store::path::Path;
use tracing::debug;
use url::Url;

use crate::features::{log_can_truncate, writer_features};

/// Delta's log retention for a table that sets no `delta.logRetentionDuration`.
const DEFAULT_LOG_RETENTION: Duration = Duration::from_secs(30 * 24 * 60 * 60);

/// Delete the log entries the table's log retention has expired. Returns how many went.
pub(crate) async fn cleanup_expired_logs(
    engine: Arc<dyn Engine>,
    store: &dyn ObjectStore,
    base_path: &Path,
    snapshot: SnapshotRef,
    now: DateTime<Utc>,
) -> Result<u64, anyhow::Error> {
    let table_root = snapshot.table_root().clone();
    let Some(boundary) = expiry_horizon(engine, snapshot, now).await? else {
        return Ok(0);
    };
    let files = list_log(store, base_path, &table_root).await?;
    delete_paths(store, expired(&files, boundary)).await
}

/// The checkpoint the log can be truncated to. Everything older than it has expired.
async fn expiry_horizon(
    engine: Arc<dyn Engine>,
    snapshot: SnapshotRef,
    now: DateTime<Utc>,
) -> Result<Option<Version>, anyhow::Error> {
    let props = snapshot.table_properties();
    if props.enable_expired_log_cleanup == Some(false) {
        debug!("log cleanup disabled by delta.enableExpiredLogCleanup");
        return Ok(None);
    }
    let blocked: Vec<_> = writer_features(&snapshot)
        .iter()
        .filter(|feature| !log_can_truncate(feature))
        .collect();
    if !blocked.is_empty() {
        debug!(?blocked, "log cleanup skipped for the table's features");
        return Ok(None);
    }

    let retention = props
        .log_retention_duration
        .unwrap_or(DEFAULT_LOG_RETENTION);
    let retention = chrono::Duration::from_std(retention)
        .map_err(|e| anyhow::anyhow!("log retention {retention:?} out of range: {e}"))?;
    let cutoff = (now - retention)
        .date_naive()
        .and_time(NaiveTime::MIN)
        .and_utc();

    tokio::task::spawn_blocking(move || {
        let horizon = match latest_version_as_of(
            &snapshot,
            engine.as_ref(),
            cutoff.timestamp_millis(),
            HistoryCommitType::Recreatable,
        ) {
            Ok(commit) => commit.version,
            Err(KernelError::LogHistory(e))
                if matches!(*e, LogHistoryError::TimestampOutOfRange { .. }) =>
            {
                debug!(%cutoff, "no commit at or before the log retention cutoff");
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };
        let at_horizon = Snapshot::builder_for(snapshot.table_root().clone())
            .at_version(horizon)
            .build(engine.as_ref())?;
        match at_horizon.log_segment().checkpoint_version {
            Some(checkpoint) => Ok(Some(checkpoint)),
            None => {
                debug!(horizon, "no checkpoint at or before the cutoff commit");
                Ok(None)
            }
        }
    })
    .await
    .context("log cleanup task failed")?
}

/// The files directly under `_delta_log/`, paired with kernel's parse of each name.
async fn list_log(
    store: &dyn ObjectStore,
    base_path: &Path,
    table_root: &Url,
) -> Result<Vec<(Path, ParsedLogPath<Url>)>, anyhow::Error> {
    let log_url = table_root.join("_delta_log/")?;
    let listing = store
        .list_with_delimiter(Some(&base_path.clone().join("_delta_log")))
        .await?;
    listing
        .objects
        .into_iter()
        .map(
            |meta| -> Result<Option<(Path, ParsedLogPath<Url>)>, anyhow::Error> {
                let Some(name) = meta.location.filename() else {
                    return Ok(None);
                };
                let parsed = ParsedLogPath::try_from(log_url.join(name)?)?;
                Ok(parsed.map(|parsed| (meta.location, parsed)))
            },
        )
        .filter_map(Result::transpose)
        .collect()
}

/// The listed log files that fall before the boundary checkpoint.
fn expired(files: &[(Path, ParsedLogPath<Url>)], boundary: Version) -> Vec<Path> {
    files
        .iter()
        .filter(|(_, parsed)| is_expired(parsed, boundary))
        .map(|(path, _)| path.clone())
        .collect()
}

fn is_expired(file: &ParsedLogPath<Url>, boundary: Version) -> bool {
    use LogPathFileType::*;
    match file.file_type {
        Commit | ClassicCheckpoint | UuidCheckpoint | MultiPartCheckpoint { .. } | Crc => {
            file.version < boundary
        }
        // `version` is the first commit the compaction covers.
        CompactedCommit { .. } => file.version <= boundary,
        StagedCommit | Unknown => false,
    }
}

/// Delete every path. A path that is already gone counts as deleted.
async fn delete_paths(store: &dyn ObjectStore, paths: Vec<Path>) -> Result<u64, anyhow::Error> {
    let mut deleted = 0;
    let mut failed = Vec::new();
    let mut results = store.delete_stream(stream::iter(paths.into_iter().map(Ok)).boxed());
    while let Some(result) = results.next().await {
        match result {
            Ok(path) => {
                debug!(%path, "log cleanup deleted");
                deleted += 1;
            }
            Err(object_store::Error::NotFound { .. }) => deleted += 1,
            Err(e) => failed.push(e.to_string()),
        }
    }
    match failed.is_empty() {
        true => Ok(deleted),
        false => Err(anyhow::anyhow!(
            "deleted {deleted} log files, failed {}: {}",
            failed.len(),
            failed.join("; ")
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use tokio::runtime::Handle;

    const DAY_MS: i64 = 24 * 60 * 60 * 1000;

    const PROTOCOL: &str = r#"{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":[],"writerFeatures":["inCommitTimestamp"]}}"#;
    const METADATA: &str = r#"{"metaData":{"id":"00000000-0000-0000-0000-000000000000","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableInCommitTimestamps":"true"}}}"#;

    fn at(rfc3339: &str) -> DateTime<Utc> {
        rfc3339.parse().unwrap()
    }

    fn log_path(version: Version) -> Path {
        Path::from(format!("_delta_log/{version:020}.json"))
    }

    fn commit(version: Version, ict: i64) -> String {
        let info = format!(
            r#"{{"commitInfo":{{"timestamp":{ict},"operation":"WRITE","inCommitTimestamp":{ict}}}}}"#
        );
        match version {
            0 => format!("{info}\n{PROTOCOL}\n{METADATA}\n"),
            _ => format!("{info}\n"),
        }
    }

    /// With the default 30-day retention this puts the cutoff at midnight 2026-01-06: after
    /// commit 4, before commit 5.
    const NOW: &str = "2026-02-05T06:00:00Z";

    /// Commits 0..=7 a day apart at noon from 2026-01-01, checkpoints at 2 and 6.
    async fn seeded_table() -> (Arc<dyn ObjectStore>, Arc<dyn Engine>, SnapshotRef) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = crate::build_engine(store.clone(), &Handle::current());
        let root = Url::parse("memory:///").unwrap();

        let first = at("2026-01-01T12:00:00Z").timestamp_millis();
        for version in 0..=7 {
            let ict = first + version as i64 * DAY_MS;
            store
                .put(&log_path(version), commit(version, ict).into())
                .await
                .unwrap();
        }
        for version in [2, 6] {
            let snapshot = Snapshot::builder_for(root.clone())
                .at_version(version)
                .build(engine.as_ref())
                .unwrap();
            crate::write_checkpoint(engine.clone(), snapshot)
                .await
                .unwrap();
        }
        let latest = Snapshot::builder_for(root).build(engine.as_ref()).unwrap();
        (store, engine, latest)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn horizon_is_the_newest_checkpoint_at_or_before_the_cutoff_commit() {
        let (_, engine, latest) = seeded_table().await;
        assert_eq!(
            expiry_horizon(engine, latest, at(NOW)).await.unwrap(),
            Some(2)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cleanup_deletes_the_log_before_the_boundary_and_nothing_else() {
        let (store, engine, latest) = seeded_table().await;
        let sidecar = Path::from("_delta_log/_sidecars/orphan.parquet");
        store.put(&sidecar, "".into()).await.unwrap();

        let deleted =
            cleanup_expired_logs(engine, store.as_ref(), &Path::default(), latest, at(NOW))
                .await
                .unwrap();
        assert_eq!(deleted, 2);

        let remaining: Vec<String> = store
            .list(None)
            .map(|meta| meta.unwrap().location.to_string())
            .collect()
            .await;
        for gone in [log_path(0), log_path(1)] {
            assert!(!remaining.contains(&gone.to_string()), "{gone} survived");
        }
        for kept in [
            log_path(2),
            Path::from("_delta_log/00000000000000000002.checkpoint.parquet"),
            log_path(7),
            Path::from("_delta_log/_last_checkpoint"),
            sidecar,
        ] {
            assert!(remaining.contains(&kept.to_string()), "{kept} was deleted");
        }
    }

    fn listed(name: &str) -> (Path, ParsedLogPath<Url>) {
        let url = Url::parse(&format!("memory:///_delta_log/{name}")).unwrap();
        let parsed = ParsedLogPath::try_from(url).unwrap().unwrap();
        (Path::from(format!("_delta_log/{name}")), parsed)
    }

    #[test]
    fn expired_takes_everything_before_the_boundary_checkpoint() {
        let delete = [
            "00000000000000000009.json",
            "00000000000000000009.crc",
            "00000000000000000009.checkpoint.parquet",
            "00000000000000000009.checkpoint.0000000001.0000000002.parquet",
            "00000000000000000005.00000000000000000010.compacted.json",
            "00000000000000000010.00000000000000000012.compacted.json",
        ];
        let keep = [
            "00000000000000000010.json",
            "00000000000000000010.crc",
            "00000000000000000010.checkpoint.parquet",
            "00000000000000000011.json",
            "00000000000000000011.00000000000000000013.compacted.json",
        ];
        let files: Vec<_> = delete
            .iter()
            .chain(keep.iter())
            .map(|n| listed(n))
            .collect();

        let expected: Vec<_> = delete.iter().map(|n| listed(n).0).collect();
        assert_eq!(expired(&files, 10), expected);
    }
}
