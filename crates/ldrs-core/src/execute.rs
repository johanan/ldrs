//! The execution side of a task: build sinks from resolved destination specs (computing the
//! schema-derived target columns and cast), and the shared pg pool map they draw connections from.

use std::collections::HashMap;
use std::io::{self, IsTerminal};
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use deadpool_postgres::Pool;
use ldrs_arrow::{
    build_arrow_transform_strategy, build_source_and_target_schema, ArrowColumnTransformStrategy,
    ColumnSpec,
};
use ldrs_delta::{ensure_table, DeltaMergeSink, DeltaOverwriteSink};
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
    // Matching destinations share one transform; otherwise each casts its own.
    let shared = all_columns_match(&task.dests);
    let src = open_source(task.source, cloud_io_rt).await?;
    let cleanup_handle = src.cleanup_handle;
    match src.schema {
        Some(schema) => {
            let built = build_sinks(task.dests, &src.source_cols, &schema, pg_pools).await?;
            let (mut sinks, per_dest): (Vec<Sink>, Vec<Option<BatchTransform>>) =
                built.into_iter().unzip();
            let transforms = if shared {
                Transforms::Shared(per_dest.into_iter().next().flatten())
            } else {
                Transforms::PerDest(per_dest)
            };
            match drive(src.stream_type, &mut sinks, &transforms).await {
                Ok(rows) => {
                    // Settle the source before committing. A spawned source that dies mid-stream
                    // leaves a truncated but error-free batch stream, so its exit status is the only
                    // integrity signal; check it before finish_all so partial data is never committed.
                    if let Err(src_err) = reap_source(cleanup_handle).await {
                        abort_all(sinks).await;
                        return Err(src_err);
                    }
                    let destinations = finish_all(sinks).await?;
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

/// Build one sink per resolved destination spec, paired with the transform it needs (the executor
/// owns the cast; sinks are passthrough writers). Aborts already-built sinks if a later one fails.
pub async fn build_sinks(
    dests: Vec<DestSpec>,
    source_cols: &[ColumnSpec],
    schema: &SchemaRef,
    pg_pools: &HashMap<String, Pool>,
) -> Result<Vec<(Sink, Option<BatchTransform>)>, anyhow::Error> {
    let mut built = Vec::with_capacity(dests.len());
    for dest in dests {
        match build_sink(dest, source_cols, schema, pg_pools).await {
            Ok(pair) => built.push(pair),
            Err(e) => {
                abort_all(built.into_iter().map(|(sink, _)| sink).collect()).await;
                return Err(e);
            }
        }
    }
    Ok(built)
}

/// Build one sink from its resolved plan spec: compute the schema-derived target columns and the
/// cast, then construct the sink. The schema-dependent half of destination handling.
async fn build_sink(
    dest: DestSpec,
    source_cols: &[ColumnSpec],
    schema: &SchemaRef,
    pg_pools: &HashMap<String, Pool>,
) -> Result<(Sink, Option<BatchTransform>), anyhow::Error> {
    match dest {
        DestSpec::Pg(pg) => {
            // PG keeps `target_cols` for COPY encoding; the cast runs in the executor.
            let (target_cols, _out_schema, transform) =
                resolve_transform(source_cols, pg.columns, schema)?;
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
            Ok((Sink::Pg(sink), transform))
        }
        DestSpec::Pq(pq) => {
            let (_target_cols, out_schema, transform) =
                resolve_transform(source_cols, pq.columns, schema)?;
            let props = with_bloom_filters(default_writer_props(), pq.bloom_filters);
            let sink = ParquetSink::new(
                &pq.url,
                out_schema,
                pq.max_rows,
                pq.max_bytes,
                pq.namer,
                Some(props),
            )?;
            Ok((Sink::Pq(sink), transform))
        }
        DestSpec::Delta(delta) => {
            let (_target_cols, out_schema, transform) =
                resolve_transform(source_cols, delta.columns, schema)?;
            ensure_table(&delta.table_path, &out_schema).await?;
            let sink = match delta.mode {
                DeltaMode::Overwrite {
                    max_rows,
                    max_bytes,
                } => Sink::DeltaOverwrite(DeltaOverwriteSink::new(
                    &delta.table_path,
                    out_schema,
                    max_rows,
                    max_bytes,
                )?),
                DeltaMode::Merge(merge_config) => Sink::DeltaMerge(DeltaMergeSink::new(
                    &delta.table_path,
                    out_schema,
                    merge_config,
                )?),
            };
            Ok((sink, transform))
        }
        DestSpec::Arrow(arrow) => {
            if io::stdout().is_terminal() {
                return Err(anyhow::Error::msg("Outputting Arrow IPC Stream to stdout is not supported in a terminal. Please redirect the output to a file or pipe it to another command."));
            }
            let (_target_cols, out_schema, transform) =
                resolve_transform(source_cols, arrow.columns, schema)?;
            let sink = ArrowStdoutSink::new(io::stdout(), out_schema)?;
            Ok((Sink::Arrow(sink), transform))
        }
    }
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

/// Whether every destination resolved to the same column specs. If they all match, the source
/// columns are the same too, so one shared transform serves them all.
fn all_columns_match(dests: &[DestSpec]) -> bool {
    dests.len() > 1 && dests.windows(2).all(|w| w[0].columns() == w[1].columns())
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
