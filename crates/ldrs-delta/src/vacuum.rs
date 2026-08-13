//! Delete files under a Delta table that its latest snapshot does not reference.

use std::collections::HashSet;
use std::time::Duration;

use std::sync::Arc;

use anyhow::Context;
use arrow::array::AsArray;
use arrow_array::Array;
use chrono::{DateTime, Utc};
use delta_kernel::commit_range::{CommitRange, DeltaAction};
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::path::{LogPathFileType, ParsedLogPath};
use delta_kernel::table_features::{TableFeature, MAX_VALID_WRITER_VERSION};
use delta_kernel::{Engine, Snapshot, SnapshotRef, Version};
use futures::StreamExt;
use ldrs_storage::{base_or_relative_path, build_store};
use object_store::path::Path;
use object_store::ObjectStore;
use tokio::runtime::Handle;
use tracing::{debug, info};

use crate::build_engine;

const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Debug, Clone, Copy)]
pub enum Retention {
    TableDefault,
    At(Duration),
    Unchecked(Duration),
}

#[derive(Debug)]
pub struct VacuumOutcome {
    pub files_listed: u64,
    pub files_kept: u64,
    pub files_deleted: u64,
    pub files_selected: u64,
    pub retention_used: Duration,
    pub cutoff: DateTime<Utc>,
    pub dry_run: bool,
    pub delete_errors: Vec<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Skip,
    Keep,
    Delete,
}

#[derive(Default)]
struct Sweep {
    listed: u64,
    kept: u64,
    candidates: Vec<Path>,
}

pub async fn vacuum(
    table_path: &str,
    retention: Retention,
    dry_run: bool,
    cloud_io: &Handle,
) -> Result<VacuumOutcome, anyhow::Error> {
    let url = base_or_relative_path(table_path)?;
    let (store, base_path, _) = build_store(&url)?;
    let engine = build_engine(store.clone(), cloud_io);
    let snapshot = Snapshot::builder_for(url.clone()).build(engine.as_ref())?;

    // Everything that can refuse the vacuum resolves before the first delete.
    refuse_unsupported_features(&snapshot)?;
    let retention_used = effective_retention(
        retention,
        snapshot
            .table_properties()
            .deleted_file_retention_duration
            .unwrap_or(DEFAULT_RETENTION),
    )?;
    let cutoff = Utc::now()
        - chrono::Duration::from_std(retention_used)
            .map_err(|e| anyhow::anyhow!("retention {retention_used:?} out of range: {e}"))?;

    // The keep set is built whole or not at all: a swallowed error here deletes live files.
    let keep = collect_keep_set(&snapshot, engine.as_ref())?;
    let partition_columns = snapshot
        .table_configuration()
        .metadata()
        .partition_columns()
        .to_vec();

    // Reading the commits costs one JSON read each, so it is skipped when no cdc
    let needs_change_data = may_have_change_data(&snapshot);

    let mut sweep = Sweep::default();
    let mut oldest_commit: Option<Version> = None;
    let mut listing = store.list(Some(&base_path));
    while let Some(meta) = listing.next().await {
        let meta = meta?;
        let Some(relative) = relative_path(&base_path, &meta.location) else {
            continue;
        };

        if relative.starts_with("_delta_log/") {
            let version = match needs_change_data {
                true => commit_version(&url, relative)?,
                false => None,
            };
            oldest_commit = [oldest_commit, version].into_iter().flatten().min();

            continue;
        }
        match classify(
            relative,
            meta.last_modified,
            &keep,
            cutoff,
            &partition_columns,
        ) {
            Verdict::Keep => {
                sweep.listed += 1;
                sweep.kept += 1;
            }
            Verdict::Delete => {
                sweep.listed += 1;
                sweep.candidates.push(meta.location);
            }
            Verdict::Skip => (),
        }
    }

    match needs_change_data {
        false => (),
        true => {
            let oldest_commit = oldest_commit.ok_or_else(|| {
                anyhow::anyhow!("no commit files found under {base_path}/_delta_log")
            })?;
            let cdc_hash = read_cdc_paths(url.as_str(), oldest_commit, &engine)?;
            sweep.candidates.retain(|location| {
                relative_path(&base_path, location)
                    .is_none_or(|relative| !cdc_hash.contains(relative))
            });
        }
    }

    let mut report = VacuumOutcome {
        files_listed: sweep.listed,
        files_kept: sweep.kept,
        files_deleted: 0,
        files_selected: sweep.candidates.len() as u64,
        retention_used,
        cutoff,
        dry_run,
        delete_errors: Vec::new(),
    };

    if dry_run {
        for path in &sweep.candidates {
            info!(%path, "vacuum would delete");
        }
        return Ok(report);
    }

    let paths = futures::stream::iter(sweep.candidates.into_iter().map(Ok)).boxed();
    let mut deletes = store.delete_stream(paths);
    while let Some(result) = deletes.next().await {
        match result {
            Ok(path) => {
                debug!(%path, "vacuum deleted");
                report.files_deleted += 1;
            }
            // Another vacuum got there first; the file is gone either way.
            Err(object_store::Error::NotFound { .. }) => report.files_deleted += 1,
            Err(e) => report.delete_errors.push(format!("{e}")),
        }
    }
    Ok(report)
}

/// The window to keep, or an error when the request is shorter than the table's own retention.
fn effective_retention(
    requested: Retention,
    table_retention: Duration,
) -> Result<Duration, anyhow::Error> {
    match requested {
        Retention::TableDefault => Ok(table_retention),
        Retention::At(d) if d < table_retention => Err(anyhow::anyhow!(
            "retention of {}s is shorter than the table's {}s (delta.deletedFileRetentionDuration). Files inside that window may belong to a write that has not committed yet",
            d.as_secs(),
            table_retention.as_secs()
        )),
        Retention::At(d) => Ok(d),
        Retention::Unchecked(d) => Ok(d),
    }
}

/// Parse an interval written for delta.deletedFileRetentionDuration
///
/// Mirrors kernel's table_properties/deserialize.rs to maintain coverage
pub fn parse_retention(value: &str) -> Result<Duration, anyhow::Error> {
    const SECONDS_PER_MINUTE: u64 = 60;
    const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
    const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
    const SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;

    let mut it = value.split_whitespace();
    let count = match it.next() {
        Some("interval") => it.next(),
        other => other,
    }
    .ok_or_else(|| anyhow::anyhow!("'{value}' is not an interval; expected e.g. 7 days"))?;
    let count: u64 = count
        .parse::<i64>()
        .map_err(|_| anyhow::anyhow!("'{count}' is not a whole number of units"))?
        .try_into()
        .map_err(|_| anyhow::anyhow!("interval '{value}' cannot be negative"))?;
    let unit = it
        .next()
        .ok_or_else(|| anyhow::anyhow!("'{value}' has no unit; expected e.g. 7 days"))?;

    let secs = |per: u64| {
        count
            .checked_mul(per)
            .map(Duration::from_secs)
            .ok_or_else(|| anyhow::anyhow!("interval {value} is too large"))
    };
    let duration = match unit {
        "nanosecond" | "nanoseconds" => Ok(Duration::from_nanos(count)),
        "microsecond" | "microseconds" => Ok(Duration::from_micros(count)),
        "millisecond" | "milliseconds" => Ok(Duration::from_millis(count)),
        "second" | "seconds" => Ok(Duration::from_secs(count)),
        "minute" | "minutes" => secs(SECONDS_PER_MINUTE),
        "hour" | "hours" => secs(SECONDS_PER_HOUR),
        "day" | "days" => secs(SECONDS_PER_DAY),
        "week" | "weeks" => secs(SECONDS_PER_WEEK),
        "month" | "months" => Err(anyhow::anyhow!("interval unit {unit} is not supported")),
        _ => Err(anyhow::anyhow!(
            "unknown interval unit {unit}; expected one of sub-second, second(s), minute(s), hour(s), day(s), week(s)"
        )),
    }?;
    match it.next() {
        Some(trailing) => Err(anyhow::anyhow!("interval {value} has trailing {trailing}")),
        None => Ok(duration),
    }
}

/// Refuse tables whose protocol names a feature that can reference files outside the add paths and
/// deletion vectors collected here, or a protocol version whose features are not enumerable.
fn refuse_unsupported_features(snapshot: &Snapshot) -> Result<(), anyhow::Error> {
    let protocol = snapshot.table_configuration().protocol();
    if protocol.min_writer_version() > MAX_VALID_WRITER_VERSION {
        anyhow::bail!(
            "cannot vacuum a table at writer version {}",
            protocol.min_writer_version()
        );
    }
    let unsupported: Vec<String> = protocol
        .writer_features()
        .unwrap_or_default()
        .iter()
        .filter(|feature| !files_are_enumerable(feature))
        .map(|feature| feature.to_string())
        .collect();
    match unsupported.is_empty() {
        true => Ok(()),
        false => Err(anyhow::anyhow!(
            "cannot vacuum a table with the feature(s) {}: they may reference files that are not in the table's add actions",
            unsupported.join(", ")
        )),
    }
}

/// Whether a feature leaves the table's full file set reachable from the active add actions and
/// their deletion vectors.
fn files_are_enumerable(feature: &TableFeature) -> bool {
    use TableFeature::*;
    match feature {
        // Log-, schema- or parquet-level only: no files beyond the add paths.
        AppendOnly
        | Invariants
        | CheckConstraints
        | GeneratedColumns
        | IdentityColumns
        | InCommitTimestamp
        | DomainMetadata
        | RowTracking
        | ColumnMapping
        | TypeWidening
        | TypeWideningPreview
        | VariantType
        | VariantTypePreview
        | VariantShredding
        | VariantShreddingPreview
        | MaterializePartitionColumns
        | AllowColumnDefaults
        | ClusteredTable
        | TimestampWithoutTimezone
        | DeletionVectors
        | VacuumProtocolCheck
        | ChangeDataFeed
        | V2Checkpoint => true,
        // Iceberg metadata sits outside `_delta_log` and is not named by any add action.
        IcebergCompatV1 | IcebergCompatV2 | IcebergCompatV3 => false,
        // Commits can live outside `_delta_log`, so a snapshot read from the store may be stale
        // and a committed file could look unreferenced.
        CatalogManaged | CatalogOwnedPreview => false,
        Unknown(_) => false,
    }
}

/// Every file the latest snapshot references, as table-relative paths in the form the object store
fn collect_keep_set(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
) -> Result<HashSet<String>, anyhow::Error> {
    let scan = snapshot.clone().scan_builder().build()?;
    let mut keep: HashSet<String> = HashSet::new();

    for scan_metadata in scan.scan_metadata(engine)? {
        let scan_metadata = scan_metadata?;
        // scan the batches for files
        let batch = &scan_metadata
            .scan_files
            .data()
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .ok_or_else(|| anyhow::anyhow!("scan output was not ArrowEngineData"))?
            .record_batch();
        let selection = &scan_metadata.scan_files.selection_vector();

        let paths = batch
            .column_by_name("path")
            .map(|c| c.as_string::<i32>())
            .ok_or_else(|| anyhow::anyhow!("scan row schema has no path column"))?;
        let dv = batch
            .column_by_name("deletionVector")
            .map(|c| c.as_struct())
            .ok_or_else(|| anyhow::anyhow!("scan row schema has no deletionVector column"))?;
        let storage_type = dv
            .column_by_name("storageType")
            .map(|c| c.as_string::<i32>())
            .ok_or_else(|| anyhow::anyhow!("deletionVector has no storageType column"))?;
        let path_or_inline = dv
            .column_by_name("pathOrInlineDv")
            .map(|c| c.as_string::<i32>())
            .ok_or_else(|| anyhow::anyhow!("deletionVector has no pathOrInlineDv column"))?;

        for row in 0..batch.num_rows() {
            let selected = selection.get(row).copied().unwrap_or(true);
            // if not selected or not path then nothing to keep
            if !selected || paths.is_null(row) {
                continue;
            }
            keep.insert(decode_log_path(paths.value(row))?);
            // check for a deletion vector
            if !dv.is_null(row) {
                let storage_type = storage_type.value(row);
                let path_or_inline = path_or_inline.value(row);
                if let Some(dv_path) = dv_file_path(storage_type, path_or_inline)? {
                    keep.insert(dv_path);
                }
            }
        }
    }
    Ok(keep)
}

fn commit_version(table_root: &url::Url, relative: &str) -> Result<Option<Version>, anyhow::Error> {
    let parsed = ParsedLogPath::try_from(table_root.join(relative)?)?;
    Ok(parsed
        .filter(|path| path.file_type == LogPathFileType::Commit)
        .map(|path| path.version))
}

// we cannot be sure on older tables
fn may_have_change_data(snapshot: &Snapshot) -> bool {
    match snapshot.table_configuration().protocol().writer_features() {
        Some(features) => features.contains(&TableFeature::ChangeDataFeed),
        None => true,
    }
}

fn read_cdc_paths(
    table_root: &str,
    start_version: Version,
    engine: &Arc<dyn Engine>,
) -> Result<HashSet<String>, anyhow::Error> {
    let range = CommitRange::builder_for(table_root, start_version).build(engine.as_ref())?;
    let mut paths = HashSet::new();
    for commit in range.commits(engine.clone(), None, &[DeltaAction::Cdc])? {
        for batch in commit?.get_actions(engine.as_ref())? {
            let batch = batch?;
            let batch = batch
                .any_ref()
                .downcast_ref::<ArrowEngineData>()
                .ok_or_else(|| anyhow::anyhow!("commit actions were not ArrowEngineData"))?
                .record_batch();
            let cdc = batch
                .column_by_name("cdc")
                .map(|c| c.as_struct())
                .ok_or_else(|| anyhow::anyhow!("commit action schema has no cdc column"))?;
            let cdc_paths = cdc
                .column_by_name("path")
                .map(|c| c.as_string::<i32>())
                .ok_or_else(|| anyhow::anyhow!("cdc has no path column"))?;
            for row in 0..batch.num_rows() {
                // most rows in a commit are some other action
                if cdc.is_null(row) || cdc_paths.is_null(row) {
                    continue;
                }
                paths.insert(decode_log_path(cdc_paths.value(row))?);
            }
        }
    }
    Ok(paths)
}

fn decode_log_path(path: &str) -> Result<String, anyhow::Error> {
    let _ = url::Url::parse(path).with_context(|| "cannot vacuum a table that references the absolute path {path}; its files are outside the table root")?;
    Ok(String::from(Path::from_url_path(path)?))
}

fn dv_file_path(storage_type: &str, dv: &str) -> Result<Option<String>, anyhow::Error> {
    match storage_type {
        "u" => {
            let split = dv
                .len()
                .checked_sub(20)
                .filter(|at| dv.is_char_boundary(*at))
                .ok_or_else(|| {
                    anyhow::anyhow!("deletion vector path {dv} does not end in a z85 uuid")
                })?;
            let (prefix, uuid_z85) = dv.split_at(split);
            let bytes = z85::decode(uuid_z85)
                .map_err(|e| anyhow::anyhow!("deletion vector path {dv} is not z85: {e}"))?;
            let uuid = uuid::Uuid::from_slice(&bytes)?;
            Ok(Some(match prefix.is_empty() {
                true => format!("deletion_vector_{uuid}.bin"),
                false => format!("{prefix}/deletion_vector_{uuid}.bin"),
            }))
        }
        "i" => Ok(None),
        _ => Err(anyhow::anyhow!(
            "deletion vector storage type {storage_type} is not supported for vacuum"
        )),
    }
}

/// A listed key as a table-relative path, or `None` when it is not under the table root.
fn relative_path<'a>(base: &Path, location: &'a Path) -> Option<&'a str> {
    let relative = location.as_ref().strip_prefix(base.as_ref())?;
    match base.as_ref().is_empty() {
        true => Some(relative),
        false => relative.strip_prefix('/'),
    }
}

/// What to do with one listed file. `Keep` covers both referenced files and files too young to
/// delete, so an uncommitted write's data files survive on the cutoff alone.
fn classify(
    relative: &str,
    modified: DateTime<Utc>,
    keep: &HashSet<String>,
    cutoff: DateTime<Utc>,
    partition_columns: &[String],
) -> Verdict {
    let hidden = relative
        .split('/')
        .any(|segment| is_hidden_segment(segment, partition_columns));
    match (hidden, keep.contains(relative) || modified >= cutoff) {
        (true, _) => Verdict::Skip,
        (false, true) => Verdict::Keep,
        (false, false) => Verdict::Delete,
    }
}

/// Whether one path segment hides its subtree from the sweep. `_` and `.` segments are hidden by
/// convention, so an unrecognised one is left alone rather than deleted.
fn is_hidden_segment(segment: &str, partition_columns: &[String]) -> bool {
    match segment.starts_with('_') || segment.starts_with('.') {
        false => false,
        true => match segment.split_once('=') {
            Some((column, _)) => !partition_columns.iter().any(|name| name == column),
            None => segment != "_change_data",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn keep_set(paths: &[&str]) -> HashSet<String> {
        paths.iter().map(|p| p.to_string()).collect()
    }

    #[test]
    fn parse_retention_takes_the_table_property_spelling() {
        let week = Duration::from_secs(7 * 24 * 60 * 60);
        assert_eq!(parse_retention("interval 7 days").unwrap(), week);
        assert_eq!(parse_retention("7 days").unwrap(), week);
        assert_eq!(parse_retention("  interval   7   days  ").unwrap(), week);
        assert_eq!(parse_retention("1 week").unwrap(), week);
    }

    #[test]
    fn parse_retention_covers_every_unit_singular_and_plural() {
        for (input, expected) in [
            ("123 nanoseconds", Duration::from_nanos(123)),
            ("123 nanosecond", Duration::from_nanos(123)),
            ("123 microseconds", Duration::from_micros(123)),
            ("123 microsecond", Duration::from_micros(123)),
            ("123 milliseconds", Duration::from_millis(123)),
            ("123 millisecond", Duration::from_millis(123)),
            ("123 seconds", Duration::from_secs(123)),
            ("123 second", Duration::from_secs(123)),
            ("2 minutes", Duration::from_secs(120)),
            ("2 minute", Duration::from_secs(120)),
            ("2 hours", Duration::from_secs(7200)),
            ("2 hour", Duration::from_secs(7200)),
            ("2 days", Duration::from_secs(172_800)),
            ("2 day", Duration::from_secs(172_800)),
            ("2 weeks", Duration::from_secs(1_209_600)),
            ("2 week", Duration::from_secs(1_209_600)),
            ("0 days", Duration::ZERO),
        ] {
            assert_eq!(parse_retention(input).unwrap(), expected, "{input}");
        }
    }

    #[test]
    fn parse_retention_rejects_a_bare_number() {
        let err = parse_retention("168").unwrap_err().to_string();
        assert!(err.contains("no unit"), "{err}");
        assert!(parse_retention("interval 168").is_err());
    }

    #[test]
    fn parse_retention_rejects_malformed_intervals() {
        for input in [
            "",
            "interval",
            "-7 days",
            "7.5 days",
            "seven days",
            "7 fortnights",
            "7 months",
            "7 years",
            "7 days 3 hours",
            "interval 7 days extra",
        ] {
            assert!(parse_retention(input).is_err(), "{input}");
        }
    }

    #[test]
    fn parse_retention_does_not_overflow() {
        // i64::MAX parses, the conversion to seconds does not fit
        let err = parse_retention("9223372036854775807 weeks")
            .unwrap_err()
            .to_string();
        assert!(err.contains("too large"), "{err}");
    }

    #[test]
    fn classify_skips_hidden_segments() {
        let keep = keep_set(&[]);
        // the log, a checkpoint sidecar, a committer leftover, and a nested `.crc` sidecar
        for path in [
            "_delta_log/00000000000000000001.json",
            "_delta_log/_sidecars/x.parquet",
            "_temporary/part-0.parquet",
            ".DS_Store",
            "dt=2026-01-01/.part-0.parquet.crc",
        ] {
            assert_eq!(
                classify(path, at(0), &keep, at(100), &[]),
                Verdict::Skip,
                "{path}"
            );
        }
    }

    #[test]
    fn classify_sweeps_change_data() {
        // cdc files are named by `cdc` actions, so they are swept and the keep set protects them
        let keep = keep_set(&["_change_data/live.parquet"]);
        assert_eq!(
            classify("_change_data/live.parquet", at(0), &keep, at(100), &[]),
            Verdict::Keep
        );
        assert_eq!(
            classify("_change_data/orphan.parquet", at(0), &keep, at(100), &[]),
            Verdict::Delete
        );
    }

    #[test]
    fn classify_sweeps_partitions_on_an_underscore_column() {
        let keep = keep_set(&["_ingested=2026-01-01/a.parquet"]);
        let columns = vec!["_ingested".to_string()];
        assert_eq!(
            classify(
                "_ingested=2026-01-01/a.parquet",
                at(0),
                &keep,
                at(100),
                &columns
            ),
            Verdict::Keep
        );
        assert_eq!(
            classify(
                "_ingested=2026-01-01/b.parquet",
                at(0),
                &keep,
                at(100),
                &columns
            ),
            Verdict::Delete
        );
        // the same layout on a table that does not declare the column stays hidden
        assert_eq!(
            classify("_ingested=2026-01-01/b.parquet", at(0), &keep, at(100), &[]),
            Verdict::Skip
        );
        // and a prefix match is not a column match
        assert_eq!(
            classify(
                "_ingested_at=2026-01-01/b.parquet",
                at(0),
                &keep,
                at(100),
                &columns
            ),
            Verdict::Skip
        );
    }

    #[test]
    fn classify_keeps_referenced_files_however_old() {
        let keep = keep_set(&["a.parquet"]);
        assert_eq!(
            classify("a.parquet", at(0), &keep, at(1_000), &[]),
            Verdict::Keep
        );
    }

    #[test]
    fn classify_keeps_unreferenced_files_inside_the_cutoff() {
        let keep = keep_set(&[]);
        assert_eq!(
            classify("new.parquet", at(500), &keep, at(100), &[]),
            Verdict::Keep
        );
    }

    #[test]
    fn classify_deletes_unreferenced_files_past_the_cutoff() {
        let keep = keep_set(&["a.parquet"]);
        assert_eq!(
            classify("orphan.parquet", at(50), &keep, at(100), &[]),
            Verdict::Delete
        );
    }

    #[test]
    fn classify_deletes_in_partition_directories() {
        let keep = keep_set(&["dt=2026-01-01/a.parquet"]);
        assert_eq!(
            classify("dt=2026-01-01/a.parquet", at(50), &keep, at(100), &[]),
            Verdict::Keep
        );
        assert_eq!(
            classify("dt=2026-01-01/b.parquet", at(50), &keep, at(100), &[]),
            Verdict::Delete
        );
    }

    #[test]
    fn effective_retention_defaults_to_the_table() {
        let table = Duration::from_secs(3_600);
        assert_eq!(
            effective_retention(Retention::TableDefault, table).unwrap(),
            table
        );
    }

    #[test]
    fn effective_retention_allows_longer_than_the_table() {
        let table = Duration::from_secs(3_600);
        let asked = Duration::from_secs(7_200);
        assert_eq!(
            effective_retention(Retention::At(asked), table).unwrap(),
            asked
        );
    }

    #[test]
    fn effective_retention_refuses_shorter_than_the_table() {
        let table = Duration::from_secs(3_600);
        let err = effective_retention(Retention::At(Duration::ZERO), table).unwrap_err();
        assert!(err.to_string().contains("shorter than the table's"));
    }

    #[test]
    fn effective_retention_unchecked_goes_under_the_table() {
        let table = Duration::from_secs(3_600);
        assert_eq!(
            effective_retention(Retention::Unchecked(Duration::ZERO), table).unwrap(),
            Duration::ZERO
        );
    }

    #[test]
    fn decode_log_path_decodes_uri_encoding() {
        // a partition value of `100%` is `dt=100%25` on disk and `dt=100%2525` in the log
        assert_eq!(
            decode_log_path("dt=100%2525/part-0.parquet").unwrap(),
            "dt=100%25/part-0.parquet"
        );
        assert_eq!(
            decode_log_path("dt=2026-01-01%2000%3A00%3A00/p.parquet").unwrap(),
            "dt=2026-01-01 00:00:00/p.parquet"
        );
    }

    #[test]
    fn decode_log_path_refuses_absolute_paths() {
        let err = decode_log_path("s3://other-bucket/t/a.parquet").unwrap_err();
        assert!(err.to_string().contains("absolute path"));
    }

    #[test]
    fn dv_file_path_rebuilds_the_bin_name() {
        let uuid = uuid::Uuid::new_v4();
        let encoded = z85::encode(uuid.as_bytes());
        assert_eq!(
            dv_file_path("u", &encoded).unwrap(),
            Some(format!("deletion_vector_{uuid}.bin"))
        );
    }

    #[test]
    fn dv_file_path_keeps_a_random_prefix_as_a_directory() {
        let uuid = uuid::Uuid::new_v4();
        let encoded = format!("ab{}", z85::encode(uuid.as_bytes()));
        assert_eq!(
            dv_file_path("u", &encoded).unwrap(),
            Some(format!("ab/deletion_vector_{uuid}.bin"))
        );
    }

    #[test]
    fn dv_file_path_refuses_a_path_without_a_z85_uuid() {
        let err = dv_file_path("u", "short").unwrap_err();
        assert!(err.to_string().contains("does not end in a z85 uuid"));
    }

    #[test]
    fn dv_file_path_has_no_file_when_inline() {
        assert_eq!(dv_file_path("i", "abc").unwrap(), None);
    }

    #[test]
    fn dv_file_path_refuses_absolute_storage() {
        let err = dv_file_path("p", "s3://bucket/dv.bin").unwrap_err();
        assert!(err.to_string().contains("storage type p is not supported"));
    }

    #[test]
    fn commit_version_reads_the_version_off_a_commit() {
        let root = url::Url::parse("file:///t/").unwrap();
        assert_eq!(
            commit_version(&root, "_delta_log/00000000000000000007.json").unwrap(),
            Some(7)
        );
    }

    #[test]
    fn commit_version_ignores_everything_that_is_not_a_commit() {
        let root = url::Url::parse("file:///t/").unwrap();
        for relative in [
            "_delta_log/00000000000000000007.checkpoint.parquet",
            "_delta_log/00000000000000000007.json.crc",
            "_delta_log/_last_checkpoint",
            "_delta_log/_sidecars/x.parquet",
            // a compacted commit's low version is not a commit boundary
            "_delta_log/00000000000000000010.00000000000000000020.compacted.json",
        ] {
            assert_eq!(commit_version(&root, relative).unwrap(), None, "{relative}");
        }
    }

    #[test]
    fn relative_path_strips_the_table_root() {
        let base = Path::from("some/table");
        assert_eq!(
            relative_path(&base, &Path::from("some/table/a.parquet")),
            Some("a.parquet")
        );
        assert_eq!(
            relative_path(&base, &Path::from("some/table/dt=1/a.parquet")),
            Some("dt=1/a.parquet")
        );
        // a sibling whose name merely starts with the root's is not under it
        assert_eq!(
            relative_path(&base, &Path::from("some/table_old/a.parquet")),
            None
        );
    }

    #[test]
    fn relative_path_handles_a_root_table() {
        assert_eq!(
            relative_path(&Path::default(), &Path::from("a.parquet")),
            Some("a.parquet")
        );
    }
}
