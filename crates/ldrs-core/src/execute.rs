//! The execution side of a task: build sinks from resolved destination specs (computing the
//! schema-derived target columns and cast), and the shared pg pool map they draw connections from.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use arrow_schema::{DataType, Schema, SchemaRef, TimeUnit as ArrowTimeUnit};
use deadpool_postgres::Pool;
use ldrs_arrow::{
    build_arrow_transform_strategy, build_source_and_target_schema, ArrowColumnTransformStrategy,
    ColumnSpec, TimeUnit,
};
use ldrs_delta::{ensure_table, DeltaMergeSink, DeltaOverwriteSink, OperationConfig};
use ldrs_parquet::{default_writer_props, with_bloom_filters, ParquetSink};
use ldrs_postgres::{build_pg_pool, PgLoad, PgSink};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::phase::PhaseOutput;
use crate::plan::{DeltaMode, DestSpec, Task};
use crate::sink::{
    abort_all, drive, finish_all, ArrowStdoutSink, BatchTransform, Sink, Transforms,
};
use crate::source::open_source;

/// Run one resolved task: open the source, build the sinks against its schema, drive the stream
/// into them, settle the source, and finish. Returns the phase output, or `None` when the source
/// produced no schema (nothing to load). Errors on a stream, settlement, or sink-build failure.
pub async fn run_task(
    cloud_io_rt: &tokio::runtime::Handle,
    pg_pools: &HashMap<String, Pool>,
    task: Task,
) -> Result<Option<PhaseOutput>, anyhow::Error> {
    let src = open_source(task.source, cloud_io_rt).await?;
    let cleanup_handle = src.cleanup_handle;
    match src.schema {
        Some(schema) => {
            // Resolve every destination before opening any of them, so a later failure cannot
            // leave an earlier one's table already created.
            let (resolved, per_dest) = resolve_dests(task.dests, &src.source_cols, &schema)?;
            // Destinations that resolved to the same columns share one transform.
            let transforms = match transform_is_shared(&resolved) {
                true => Transforms::Shared(per_dest.into_iter().next().flatten()),
                false => Transforms::PerDest(per_dest),
            };
            let mut sinks = build_sinks(resolved, pg_pools, cloud_io_rt).await?;
            match drive(src.stream_type, &mut sinks, &transforms).await {
                Ok(rows) => {
                    // Settle the source before committing. A spawned source that dies mid-stream
                    // leaves a truncated but error-free batch stream, so its exit status is the only
                    // integrity signal; check it before finish_all so partial data is never committed.
                    if let Err(src_err) = reap_source(cleanup_handle).await {
                        abort_all(sinks).await;
                        return Err(src_err);
                    }
                    let destinations = finish_all(sinks, rows).await?;
                    Ok(Some(PhaseOutput {
                        name: task.name,
                        source_files: src.source_files,
                        success: destinations.iter().all(|d| d.succeeded()),
                        rows,
                        destinations,
                    }))
                }
                Err(e) => {
                    abort_all(sinks).await;
                    // Prefer the sink error: it names exactly what failed.
                    if let Err(child_err) = reap_source(cleanup_handle).await {
                        debug!("source cleanup after sink failure: {child_err}");
                    }
                    Err(e)
                }
            }
        }
        None => {
            warn!("No schema found, most likely the load failed or no Arrow Record Batches were produced.");
            // No sinks were built; surface a source failure if the handle reports one.
            reap_source(cleanup_handle).await?;
            Ok(None)
        }
    }
}

/// What a destination resolved to against the source schema
pub struct ResolvedDest {
    spec: DestSpec,
    target_cols: Vec<ColumnSpec>,
    out_schema: SchemaRef,
}

/// Resolve every destination against the source schema
pub fn resolve_dests(
    dests: Vec<DestSpec>,
    source_cols: &[ColumnSpec],
    schema: &SchemaRef,
) -> Result<(Vec<ResolvedDest>, Vec<Option<BatchTransform>>), anyhow::Error> {
    dests
        .into_iter()
        .map(|dest| resolve_dest(dest, source_cols, schema))
        .collect::<Result<Vec<_>, _>>()
        .map(|pairs| pairs.into_iter().unzip())
}

fn resolve_dest(
    dest: DestSpec,
    source_cols: &[ColumnSpec],
    schema: &SchemaRef,
) -> Result<(ResolvedDest, Option<BatchTransform>), anyhow::Error> {
    let columns = match &dest {
        // Delta stores timestamps at microsecond precision
        DestSpec::Delta(delta) => {
            timestamps_as_micros(schema, delta.columns.clone(), delta.truncate_timestamps)?
        }
        other => other.columns().to_vec(),
    };
    let (target_cols, out_schema, transform) = resolve_transform(source_cols, columns, schema)?;
    Ok((
        ResolvedDest {
            spec: dest,
            target_cols,
            out_schema,
        },
        transform,
    ))
}

/// Whether every destination resolved to the same target columns
pub fn transform_is_shared(resolved: &[ResolvedDest]) -> bool {
    resolved.len() > 1
        && resolved
            .windows(2)
            .all(|w| w[0].target_cols == w[1].target_cols)
}

/// Construct one sink per resolved destination
pub async fn build_sinks(
    resolved: Vec<ResolvedDest>,
    pg_pools: &HashMap<String, Pool>,
    cloud_io: &tokio::runtime::Handle,
) -> Result<Vec<Sink>, anyhow::Error> {
    let mut built = Vec::with_capacity(resolved.len());
    for dest in resolved {
        match build_sink(dest, pg_pools, cloud_io).await {
            Ok(sink) => built.push(sink),
            Err(e) => {
                abort_all(built).await;
                return Err(e);
            }
        }
    }
    Ok(built)
}

/// Open the stores, create the table, and construct the writer.
async fn build_sink(
    resolved: ResolvedDest,
    pg_pools: &HashMap<String, Pool>,
    cloud_io: &tokio::runtime::Handle,
) -> Result<Sink, anyhow::Error> {
    let ResolvedDest {
        spec,
        target_cols,
        out_schema,
        ..
    } = resolved;
    match spec {
        DestSpec::Pg(pg) => {
            // PG keeps `target_cols` for COPY encoding; the cast runs in the executor.
            let target_and_cols = (target_cols.clone(), pg.target.clone());
            let load = PgLoad {
                role: pg.role,
                before: pg.before,
                load_table: pg.load_table,
                after: pg.after,
                cols: target_cols,
                target: pg.target,
            };
            let conn = pool_for(pg_pools, &pg.conn_url)?.get().await?;
            let sink = PgSink::open(conn, load).await?;
            Ok(Sink::Pg(sink, target_and_cols))
        }
        DestSpec::Pq(pq) => {
            let props = with_bloom_filters(default_writer_props(), pq.bloom_filters);
            let sink = ParquetSink::new(
                &pq.url,
                out_schema,
                pq.max_rows,
                pq.max_bytes,
                pq.namer,
                Some(props),
            )?;
            Ok(Sink::Pq(sink, (target_cols, pq.target, pq.url)))
        }
        DestSpec::Delta(delta) => {
            let table_config = OperationConfig::new(&delta.engine_info);
            ensure_table(&delta.table_path, &out_schema, &table_config).await?;
            let sink = match delta.mode {
                DeltaMode::Overwrite {
                    max_rows,
                    max_bytes,
                } => Sink::DeltaOverwrite(
                    DeltaOverwriteSink::new(
                        &delta.table_path,
                        out_schema,
                        max_rows,
                        max_bytes,
                        &table_config,
                        cloud_io,
                    )?,
                    (target_cols, delta.target, delta.table_path),
                ),
                DeltaMode::Merge(merge_config) => Sink::DeltaMerge(
                    DeltaMergeSink::new(
                        &delta.table_path,
                        out_schema,
                        merge_config,
                        &table_config,
                        cloud_io,
                    )?,
                    (target_cols, delta.target, delta.table_path),
                ),
            };
            Ok(sink)
        }
        DestSpec::Arrow(arrow) => {
            let _ = arrow;
            let sink = ArrowStdoutSink::new(io::stdout(), out_schema)?;
            Ok(Sink::Arrow(sink))
        }
    }
}

/// Force every timestamp column to microseconds, required by delta.
fn timestamps_as_micros(
    schema: &SchemaRef,
    mut columns: Vec<ColumnSpec>,
    truncate: bool,
) -> Result<Vec<ColumnSpec>, anyhow::Error> {
    for field in schema.fields() {
        let DataType::Timestamp(unit, tz) = field.data_type() else {
            continue;
        };
        if matches!(unit, ArrowTimeUnit::Nanosecond) && !truncate {
            anyhow::bail!(
                "column '{}' is a nanosecond timestamp and delta stores microseconds; set 'truncate_timestamps: true' on the destination to truncate",
                field.name()
            );
        }
        let name = field.name().clone();
        let spec = match tz {
            Some(_) => ColumnSpec::TimestampTz {
                name,
                time_unit: TimeUnit::Micros,
            },
            None => ColumnSpec::Timestamp {
                name,
                time_unit: TimeUnit::Micros,
            },
        };
        match columns
            .iter()
            .position(|c| c.name().eq_ignore_ascii_case(field.name()))
        {
            Some(index) => columns[index] = spec,
            None => columns.push(spec),
        }
    }
    Ok(columns)
}

/// Resolve a destination's column specs into its target columns, output schema, and the transform
/// the executor will run (`None` when no cast is needed).
fn resolve_transform(
    source_cols: &[ColumnSpec],
    columns: Vec<ColumnSpec>,
    schema: &SchemaRef,
) -> Result<(Vec<ColumnSpec>, SchemaRef, Option<BatchTransform>), anyhow::Error> {
    let (target_cols, strategies) = column_helper(source_cols.to_vec(), columns, schema)?;
    if strategies.iter().any(|s| s.is_some()) {
        let out_schema = Arc::new(Schema::new(
            target_cols
                .iter()
                .map(|col| col.to_arrow_field())
                .collect::<Vec<_>>(),
        ));
        Ok((
            target_cols,
            out_schema.clone(),
            Some((strategies, out_schema)),
        ))
    } else {
        Ok((target_cols, schema.clone(), None))
    }
}

fn column_helper(
    source_cols: Vec<ColumnSpec>,
    dest_cols: Vec<ColumnSpec>,
    schema: &SchemaRef,
) -> Result<
    (
        Vec<ColumnSpec>,
        Vec<std::option::Option<ArrowColumnTransformStrategy>>,
    ),
    anyhow::Error,
> {
    let (src_cols, target_cols) =
        build_source_and_target_schema(schema, source_cols, vec![dest_cols])?;
    let strategies: Vec<Option<ArrowColumnTransformStrategy>> = src_cols
        .iter()
        .zip(target_cols.iter())
        .zip(schema.fields().iter())
        .map(|((source, target), field)| {
            build_arrow_transform_strategy(source, target, field.data_type())
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((target_cols, strategies))
}

/// Await a spawned source's cleanup task, if any, and report whether it settled cleanly. A
/// truncated stream reads without error, so a non-zero child exit surfaces only here. `None` (e.g.
/// the file source) settles clean. Also reaps the child on every path.
async fn reap_source(
    handle: Option<JoinHandle<Result<(), anyhow::Error>>>,
) -> Result<(), anyhow::Error> {
    match handle {
        Some(h) => match h.await {
            Ok(inner) => inner,
            Err(e) => Err(anyhow::anyhow!("ldrs-sf task panicked: {}", e)),
        },
        None => Ok(()),
    }
}

/// Build one lazy pool per distinct connection URL. `build_pg_pool` opens no connections, so a URL
/// no task ends up using costs only a few structs; connections materialize at first checkout and
/// stay warm across tasks. Duplicate URLs collapse to one pool.
pub fn build_pools(urls: &[String]) -> Result<HashMap<String, Pool>, anyhow::Error> {
    let mut pools = HashMap::new();
    for url in urls {
        if !pools.contains_key(url) {
            pools.insert(url.clone(), build_pg_pool(url)?);
        }
    }
    Ok(pools)
}

/// Return the pool for `url`. A hit clones the shared, warm pool; a miss builds a task-local pool
fn pool_for(pools: &HashMap<String, Pool>, url: &str) -> Result<Pool, anyhow::Error> {
    match pools.get(url) {
        Some(pool) => Ok(pool.clone()),
        None => build_pg_pool(url),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_schema::Field;

    fn schema_of(unit: ArrowTimeUnit, tz: Option<&str>) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("ts", DataType::Timestamp(unit, tz.map(Into::into)), true),
        ]))
    }

    fn unit_of(columns: &[ColumnSpec], name: &str) -> Option<TimeUnit> {
        columns.iter().find_map(|c| match c {
            ColumnSpec::Timestamp { name: n, time_unit }
            | ColumnSpec::TimestampTz { name: n, time_unit }
                if n == name =>
            {
                Some(time_unit.clone())
            }
            _ => None,
        })
    }

    #[test]
    fn millis_widen_to_micros() {
        let columns =
            timestamps_as_micros(&schema_of(ArrowTimeUnit::Millisecond, None), vec![], false)
                .unwrap();
        assert_eq!(unit_of(&columns, "ts"), Some(TimeUnit::Micros));
    }

    #[test]
    fn seconds_widen_to_micros() {
        let columns =
            timestamps_as_micros(&schema_of(ArrowTimeUnit::Second, None), vec![], false).unwrap();
        assert_eq!(unit_of(&columns, "ts"), Some(TimeUnit::Micros));
    }

    /// A tz-carrying column has to stay tz-carrying, or the delta type flips to timestamp_ntz.
    #[test]
    fn a_zoned_timestamp_stays_zoned() {
        let columns = timestamps_as_micros(
            &schema_of(ArrowTimeUnit::Millisecond, Some("UTC")),
            vec![],
            false,
        )
        .unwrap();
        assert!(matches!(
            columns.iter().find(|c| c.name() == "ts"),
            Some(ColumnSpec::TimestampTz { .. })
        ));
    }

    #[test]
    fn nanos_are_refused_without_the_flag() {
        let err = timestamps_as_micros(&schema_of(ArrowTimeUnit::Nanosecond, None), vec![], false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("'ts'"), "{err}");
        assert!(err.contains("truncate_timestamps"), "{err}");
    }

    #[test]
    fn nanos_truncate_when_the_flag_is_set() {
        let columns =
            timestamps_as_micros(&schema_of(ArrowTimeUnit::Nanosecond, None), vec![], true)
                .unwrap();
        assert_eq!(unit_of(&columns, "ts"), Some(TimeUnit::Micros));
    }

    /// A declared unit is replaced rather than duplicated
    #[test]
    fn a_declared_unit_is_replaced_not_duplicated() {
        let declared = vec![ColumnSpec::Timestamp {
            name: "ts".to_string(),
            time_unit: TimeUnit::Millis,
        }];
        let columns = timestamps_as_micros(
            &schema_of(ArrowTimeUnit::Millisecond, None),
            declared,
            false,
        )
        .unwrap();
        assert_eq!(columns.iter().filter(|c| c.name() == "ts").count(), 1);
        assert_eq!(unit_of(&columns, "ts"), Some(TimeUnit::Micros));
    }

    /// An already-micros column still gets a spec; the transform diff turns it into no work.
    #[test]
    fn micros_needs_no_cast() {
        let schema = schema_of(ArrowTimeUnit::Microsecond, None);
        let columns = timestamps_as_micros(&schema, vec![], false).unwrap();
        let (_, _, transform) = resolve_transform(&[], columns, &schema).unwrap();
        assert!(transform.is_none());
    }

    use crate::plan::{DeltaDest, PqDest};

    fn delta_dest() -> DestSpec {
        DestSpec::Delta(DeltaDest {
            table_path: "file:///tmp/t".to_string(),
            mode: DeltaMode::Overwrite {
                max_rows: None,
                max_bytes: None,
            },
            columns: vec![],
            target: "t".to_string(),
            truncate_timestamps: false,
            engine_info: "ldrs-test".to_string(),
        })
    }

    fn pq_dest() -> DestSpec {
        DestSpec::Pq(PqDest {
            url: "file:///tmp/t".to_string(),
            namer: Box::new(|_| Ok("part-0.parquet".to_string())),
            bloom_filters: vec![],
            max_rows: None,
            max_bytes: None,
            columns: vec![],
            target: "t".to_string(),
        })
    }

    /// Delta coerces timestamps to micros and parquet does not
    #[test]
    fn a_delta_and_pq_fanout_over_millis_does_not_share_a_transform() {
        let schema = schema_of(ArrowTimeUnit::Millisecond, None);
        let (resolved, per_dest) =
            resolve_dests(vec![pq_dest(), delta_dest()], &[], &schema).unwrap();

        assert!(!transform_is_shared(&resolved));
        assert!(per_dest[0].is_none(), "parquet keeps the source unit");
        assert!(per_dest[1].is_some(), "delta must cast millis to micros");
    }

    #[test]
    fn a_fanout_over_micros_still_shares() {
        let schema = schema_of(ArrowTimeUnit::Microsecond, None);
        let (resolved, _) = resolve_dests(vec![pq_dest(), delta_dest()], &[], &schema).unwrap();
        assert!(transform_is_shared(&resolved));
    }

    // build_pg_pool is lazy (no connection), so these exercise the map offline.

    #[test]
    fn build_pools_dedups_by_url() {
        let pools = build_pools(&[
            "postgresql://localhost/db1".to_string(),
            "postgresql://localhost/db1".to_string(),
            "postgresql://localhost/db2".to_string(),
        ])
        .unwrap();
        assert_eq!(pools.len(), 2, "duplicate URLs collapse to one pool");
    }

    #[test]
    fn pool_for_falls_back_on_miss_without_mutating() {
        let pools = build_pools(&["postgresql://localhost/db1".to_string()]).unwrap();
        // hit: served from the shared map
        pool_for(&pools, "postgresql://localhost/db1").unwrap();
        // miss: a task-local pool is built; the shared map is untouched
        pool_for(&pools, "postgresql://localhost/db2").unwrap();
        assert_eq!(pools.len(), 1, "a miss does not mutate the shared map");
    }
}
