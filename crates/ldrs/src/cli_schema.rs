use anyhow::bail;
use schemars::SchemaGenerator;
use serde_json::{json, Value};

use crate::{
    delta::DeltaDestination,
    file_source::FileSource,
    ldrs_config::config::{ArrowDestination, LdrsConfig},
    ldrs_snowflake::snowflake_source::SFSource,
    parquet::ParquetDestination,
    postgres::postgres_destination::PgDestination,
};

fn usage_block() -> Value {
    json!({
        "yaml_config": "The schemas under `sources` and `destinations` describe the per-table block shape used inside `tables:` in YAML config files. The full YAML envelope is documented under `yaml_config`. Defaults are merged into each table block; later table-level values override.",
        "run_command": "`ldrs run` accepts a single block. Three layers, top wins: first-class flags (--src/--dest/--name/--sql) > --opt key=value pairs > --config-inline YAML. --opt is flat string=string only; complex (nested/list) fields require --config-inline.",
        "namespacing": "If a kind has a `namespace` field, that string is an optional prefix on any of its type-specific fields (e.g., `pg.merge_keys` and `merge_keys` are equivalent for pg). Universal block fields (`name`, `src`, `dest`) are never namespaced. Used to disambiguate when source and destination contribute overlapping field names to the same block.",
        "columns": "The `columns` field, when present on a destination kind, declares column transforms (rename/cast/projection). It is always a destination-side field — sources never accept `columns`.",
        "env_vars": {
            "LDRS_SRC": "URL for the source location. The scheme infers the source kind when --src is not passed: `snowflake://...` → sf, `delta+<scheme>://...` → delta, `postgres://...` → pg, object-store URLs (`s3://`, `gs://`, `az://`, `file://`, ...) → file.",
            "LDRS_DEST": "URL for the destination location. Same scheme→kind inference as LDRS_SRC.",
            "LDRS_PARAM_<NAME>[_<TYPE>]": "SQL parameter bindings consumed by query-shaped sources (e.g. sf.query). `LDRS_PARAM_P1=42` binds parameter `P1`. Append a type suffix to coerce: `LDRS_PARAM_P1_INT=42`.",
            "LDRS_TEMPL_<NAME>": "Handlebars template variable. `LDRS_TEMPL_BUCKET=my-bucket` makes `{{ bucket }}` available inside templated string fields.",
        },
        "templating": "String fields can contain handlebars templates rendered against the execution context. `{{ name }}` expands to the name property; custom variables come from `LDRS_TEMPL_<NAME>` env vars. Common use: `filename: \"{{ name }}/{{ name }}.snappy.parquet\"` builds a per-table path.",
        "examples": {
            "snowflake_to_local_parquet": "LDRS_DEST=file:///tmp/probe ldrs run --src sf.query --dest pq --name probe --sql 'SELECT 1 AS x' --opt filename=probe.snappy.parquet",
            "pipe_arrow_to_duckdb": "ldrs run --src sf.query --dest arrow --name probe --sql 'SELECT 1 AS x' | duckdb -c \"INSTALL nanoarrow FROM community; LOAD nanoarrow; FROM read_arrow('/dev/stdin')\"",
            "pipe_arrow_to_pyarrow": "ldrs run --src sf.query --dest arrow --name probe --sql 'SELECT 1' | python3 -c 'import pyarrow.ipc, sys; print(pyarrow.ipc.open_stream(sys.stdin.buffer).read_all())'"
        }
    })
}

pub fn build_full() -> Value {
    let mut g = SchemaGenerator::default();
    let yaml_config = g.subschema_for::<LdrsConfig>();
    let sources = json!({
        "file": { "schema": g.subschema_for::<FileSource>() },
        "sf": { "namespace": "sf", "schema": g.subschema_for::<SFSource>() },
    });
    let destinations = json!({
        "pg": { "namespace": "pg", "schema": g.subschema_for::<PgDestination>() },
        "pq": { "namespace": "pq", "schema": g.subschema_for::<ParquetDestination>() },
        "delta": { "namespace": "delta", "schema": g.subschema_for::<DeltaDestination>() },
        "arrow": { "schema": g.subschema_for::<ArrowDestination>() },
    });
    json!({
        "usage": usage_block(),
        "$defs": g.take_definitions(true),
        "yaml_config": yaml_config,
        "sources": sources,
        "destinations": destinations,
    })
}

pub fn build_one(kind: &str) -> Result<Value, anyhow::Error> {
    let mut g = SchemaGenerator::default();
    let source = match kind {
        "file" => Some(json!({ "schema": g.subschema_for::<FileSource>() })),
        "sf" => Some(json!({ "namespace": "sf", "schema": g.subschema_for::<SFSource>() })),
        _ => None,
    };
    let destination = match kind {
        "pg" => Some(json!({ "namespace": "pg", "schema": g.subschema_for::<PgDestination>() })),
        "pq" => {
            Some(json!({ "namespace": "pq", "schema": g.subschema_for::<ParquetDestination>() }))
        }
        "delta" => {
            Some(json!({ "namespace": "delta", "schema": g.subschema_for::<DeltaDestination>() }))
        }
        "arrow" => Some(json!({ "schema": g.subschema_for::<ArrowDestination>() })),
        _ => None,
    };
    if source.is_none() && destination.is_none() {
        bail!("unknown kind: {}", kind);
    }
    Ok(json!({
        "usage": usage_block(),
        "$defs": g.take_definitions(true),
        "kind": kind,
        "source": source,
        "destination": destination,
    }))
}
