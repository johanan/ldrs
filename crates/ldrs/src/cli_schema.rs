use clap::Subcommand;
use ldrs_arrow::{ColumnSpec, ColumnType};
use schemars::{json_schema, JsonSchema, Schema, SchemaGenerator};
use serde_json::{json, Value};

use crate::{
    delta::DeltaDestination,
    file_source::FileSource,
    ldrs_config::config::{ArrowDestination, LdrsConfig},
    ldrs_snowflake::snowflake_source::SFSource,
    parquet::ParquetDestination,
    postgres::postgres_destination::PgDestination,
};

/// Placeholder schema for `columns:` fields. Keeps `ColumnSpec` out of every
/// per-kind dump; the real variant schema lives in `ldrs schema columns`.
pub fn columns_schema(_: &mut SchemaGenerator) -> Schema {
    json_schema!({
        "type": "array",
        "description": "Column transforms (rename/cast/projection). Run `ldrs schema columns` for the variant schema."
    })
}

/// Placeholder schema for `param_keys:` (`Option<Vec<ColumnType>>`). Keeps
/// `ColumnType` out of the pg dump; variants live in `ldrs schema columns`.
pub fn param_keys_schema(_: &mut SchemaGenerator) -> Schema {
    json_schema!({
        "type": ["array", "null"],
        "description": "Positional column types for prepared-statement parameters. Run `ldrs schema columns` for the variant schema; see `ldrs schema usage` for LDRS_PARAM binding rules."
    })
}

fn usage_block() -> Value {
    json!({
        "yaml_config": "The schemas under `sources` and `destinations` describe the per-table block shape used inside `tables:` in YAML config files. The full YAML envelope is documented under `yaml_config`. Defaults are merged into each table block; later table-level values override.",
        "run_command": "`ldrs run` accepts a single block (one source -> one destination, Arrow-pipeable). Three layers, top wins: first-class flags (--src/--dest/--name/--sql) > --opt key=value pairs > --config-inline YAML. --opt is flat string=string only; complex (nested/list) fields require --config-inline. A block is version-free: `version:`/`destinations:` apply only to multi-table config files (`ldrs ld`). Run `ldrs schema <kind>` to see a block's fields.",
        "ld_command": "`ldrs ld --config <file>` runs a multi-table YAML config: each entry under `tables:` runs in order. Use `version: 2` for the nested `destinations:` form: one source fans out to many destinations. A top-level `destinations:` list is the shared default for tables that don't declare their own; `src_defaults` merges into each table's source block, and each table's `name`/`columns` are inherited into its destination blocks. `--select t1,t2` (comma-separated) runs only the named tables. A single `LDRS_SRC`/`LDRS_DEST` locates all tables; set `LDRS_SRC_<NAME>`/`LDRS_DEST_<NAME>` to point individual tables elsewhere (see env_vars). The flat single-`dest:` form (version 1) is deprecated.",
        "namespacing": "If a kind has a `namespace` field, that string is an optional prefix on any of its type-specific fields (e.g., `pg.merge_keys` and `merge_keys` are equivalent for pg). Universal block fields (`name`, `src`, `dest`) are never namespaced. Used to disambiguate when source and destination contribute overlapping field names to the same block.",
        "columns": "The `columns` field, when present on a destination kind, declares column transforms (rename/cast/projection). It is always a destination-side field, sources never accept `columns`. Run `ldrs schema columns` for the variant schema.",
        "param_keys": "Positional column types for a destination's prepared statement (currently pg.delete_insert's DELETE WHERE clause). Values come from `LDRS_PARAM_*` env vars, bound positionally in lexicographic order of the env-var name; the count of types here must match the count of bound values. When present, this overrides the per-position type hint from the `LDRS_PARAM_<NAME>_<TYPE>` env-var suffix. Run `ldrs schema columns` for the type variants.",
        "dotenv": "A `.env` file is loaded automatically, the working directory and its parents are searched up to root. Values do not override variables already set in the environment. ldrs only reads variables beginning with `LDRS_`; anything else is ignored.",
        "logging": "Log level is set by RUST_LOG (default `info`), read from the environment or `.env`, independent of the LDRS_ prefix rule. Examples: `RUST_LOG=error`, `RUST_LOG=debug`, `RUST_LOG=ldrs=debug` (per-target). Logs are written to stderr.",
        "env_vars": {
            "LDRS_SRC_<NAME>": "Per-table source URL; highest precedence. `<NAME>` is the block's `name`.",
            "LDRS_SRC_<KIND>": "Per-kind source URL (LDRS_SRC_FILE, LDRS_SRC_SF, LDRS_SRC_PG, LDRS_SRC_PQ, LDRS_SRC_DELTA); used when no per-name var matches.",
            "LDRS_SRC": "Global source URL fallback. Required, one tier must resolve, in order LDRS_SRC_<NAME> → LDRS_SRC_<KIND> → LDRS_SRC. The URL is the base location, combined with the table's name/filename; its scheme also infers the source kind when --src is not passed: `snowflake://...` → sf, `delta+<scheme>://...` → delta, `postgres://...` → pg, object-store URLs (`s3://`, `gs://`, `az://`, `file://`, ...) → file.",
            "LDRS_DEST[_<NAME>|_<KIND>]": "Destination location. Mirrors the LDRS_SRC_<NAME> → LDRS_SRC_<KIND> → LDRS_DEST chain exactly, with the same scheme→kind inference. Required.",
            "LDRS_PARAM_<NAME>[_<TYPE>]": "SQL parameter bindings consumed by query-shaped sources (e.g. sf.query). `LDRS_PARAM_P1=42` binds parameter `P1`. Append a type suffix to coerce: `LDRS_PARAM_P1_INT=42`.",
            "LDRS_TEMPL_<NAME>": "Handlebars template variable. `LDRS_TEMPL_BUCKET=my-bucket` makes `{{ bucket }}` available inside templated string fields.",
        },
        "per_table_overrides": "SRC, DEST, TEMPL, and PARAM env vars can be scoped to one table by inserting the block's `name` (uppercased, `.` becomes `_`) after the prefix: `LDRS_TEMPL_PUBLIC_USERS_BUCKET` overrides `LDRS_TEMPL_BUCKET` only for the table named `public.users`. The unscoped variable is the default for all tables.",
        "templating": "String fields can contain handlebars templates rendered against the execution context. `{{ name }}` expands to the name property; custom variables come from `LDRS_TEMPL_<NAME>` env vars. Common use: `filename: \"{{ name }}/{{ name }}.snappy.parquet\"` builds a per-table path. A `{{ now_timestamp }}` helper emits the current Unix epoch seconds.",
        "examples": {
            "snowflake_to_local_parquet": "LDRS_DEST=file:///tmp/probe ldrs run --src sf.query --dest pq --name probe --sql 'SELECT 1 AS x' --opt filename=probe.snappy.parquet",
            "pipe_arrow_to_duckdb": "ldrs run --src sf.query --dest arrow --name probe --sql 'SELECT 1 AS x' | duckdb -c \"INSTALL nanoarrow FROM community; LOAD nanoarrow; FROM read_arrow('/dev/stdin')\"",
            "pipe_arrow_to_pyarrow": "ldrs run --src sf.query --dest arrow --name probe --sql 'SELECT 1' | python3 -c 'import pyarrow.ipc, sys; print(pyarrow.ipc.open_stream(sys.stdin.buffer).read_all())'",
            "parameterized_sf_query": "LDRS_PARAM_P1=42 LDRS_PARAM_P2=2026-01-01 ldrs run --src sf.query --dest pq --name probe --sql 'SELECT * FROM t WHERE org_id = ? AND created_at >= ?' --config-inline 'param_keys: [P1, P2]' --opt filename=probe.parquet Note: env vars bind positionally in lexicographic order of the name, so zero-pad past nine (P01..P10) since lex order sorts P10 between P1 and P2"
        }
    })
}

/// The `ldrs schema <…>` subcommands. Single source of truth for what kinds
/// exist: clap generates the discovery help from these variants, and `build`
/// matches them exhaustively, so the two cannot drift.
#[derive(Subcommand)]
pub enum SchemaCommands {
    /// File (object-store) source block
    File,
    /// Snowflake source block
    Sf,
    /// Postgres destination block
    Pg,
    /// Parquet destination block
    Pq,
    /// Delta Lake destination block
    Delta,
    /// Arrow IPC stdout destination block
    Arrow,
    /// Column transform + param type vocabulary (ColumnSpec / ColumnType)
    Columns,
    /// YAML config file envelope (LdrsConfig)
    Yaml,
    /// Env vars, templating, namespacing, and worked examples
    Usage,
}

/// Render the schema document for a single subcommand.
pub fn build(command: &SchemaCommands) -> Value {
    match command {
        SchemaCommands::File => source_doc::<FileSource>("file", None),
        SchemaCommands::Sf => source_doc::<SFSource>("sf", Some("sf")),
        SchemaCommands::Pg => dest_doc::<PgDestination>("pg", Some("pg")),
        SchemaCommands::Pq => dest_doc::<ParquetDestination>("pq", Some("pq")),
        SchemaCommands::Delta => dest_doc::<DeltaDestination>("delta", Some("delta")),
        SchemaCommands::Arrow => dest_doc::<ArrowDestination>("arrow", None),
        SchemaCommands::Columns => build_columns(),
        SchemaCommands::Yaml => build_yaml(),
        SchemaCommands::Usage => build_usage(),
    }
}

fn named_block(schema: Schema, namespace: Option<&str>) -> Value {
    match namespace {
        Some(ns) => json!({ "namespace": ns, "schema": schema }),
        None => json!({ "schema": schema }),
    }
}

fn source_doc<T: JsonSchema>(kind: &str, namespace: Option<&str>) -> Value {
    let mut g = SchemaGenerator::default();
    let block = named_block(g.subschema_for::<T>(), namespace);
    json!({
        "$defs": g.take_definitions(true),
        "kind": kind,
        "source": block,
        "columns_ref": "ldrs schema columns",
        "usage_ref": "ldrs schema usage",
    })
}

fn dest_doc<T: JsonSchema>(kind: &str, namespace: Option<&str>) -> Value {
    let mut g = SchemaGenerator::default();
    let block = named_block(g.subschema_for::<T>(), namespace);
    json!({
        "$defs": g.take_definitions(true),
        "kind": kind,
        "destination": block,
        "columns_ref": "ldrs schema columns",
        "usage_ref": "ldrs schema usage",
    })
}

/// The column transform + param type vocabulary the target of the
/// `columns`/`param_keys` placeholders in every per-kind dump.
pub fn build_columns() -> Value {
    let mut g = SchemaGenerator::default();
    let column_spec = g.subschema_for::<ColumnSpec>();
    let column_type = g.subschema_for::<ColumnType>();
    json!({
        "$defs": g.take_definitions(true),
        "column_spec": column_spec,
        "column_type": column_type,
        "usage_ref": "ldrs schema usage",
    })
}

/// The YAML config file envelope (`LdrsConfig`). Per-table blocks are opaque.
pub fn build_yaml() -> Value {
    let mut g = SchemaGenerator::default();
    let yaml_config = g.subschema_for::<LdrsConfig>();
    json!({
        "$defs": g.take_definitions(true),
        "yaml_config": yaml_config,
        "note": "Each entry under `tables:` is a source block; its destinations come from the table's or the top-level `destinations:` list (each entry a destination block). Block shapes are opaque here and depend on the src/dest kind: see `ldrs schema <kind>`.",
        "usage_ref": "ldrs schema usage",
    })
}

/// Env vars, templating, namespacing, and worked examples the context that
/// used to ride on every dump.
pub fn build_usage() -> Value {
    usage_block()
}
