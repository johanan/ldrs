use clap::Subcommand;
use ldrs_arrow::{ColumnSpec, ColumnType};
use ldrs_core::phase::PhaseOutput;
use schemars::{json_schema, JsonSchema, Schema, SchemaGenerator};
use serde_json::{json, Value};

use crate::{
    delta::DeltaDestination,
    file_source::FileSource,
    finalize::FinalizeItem,
    ldrs_config::config::{ArrowDestination, LdrsConfig},
    ldrs_duckdb::duckdb_source::DuckDbSource,
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
        "ld_command": "`ldrs ld --config <file>` runs a multi-table YAML config: each entry under `tables:` runs in order. Use `version: 2` for the nested `destinations:` form: one source fans out to many destinations. A top-level `destinations:` list is the shared default for tables that don't declare their own; `src_defaults` merges into each table's source block, and each table's `name`/`columns` are inherited into its destination blocks. `--select t1,t2` (comma-separated) runs only the named tables. A single `LDRS_SRC`/`LDRS_DEST` locates all tables; set `LDRS_SRC_<NAME>`/`LDRS_DEST_<NAME>` to point individual tables elsewhere (see env_vars). A table (or the top level) may also declare a `finalize:` list: post-load Lua handlers that run against a resolved target after the load (v2/nested only; see `ldrs schema finalize`). The flat single-`dest:` form (version 1) is deprecated.",
        "namespacing": "If a kind has a `namespace` field, that string is an optional prefix on any of its type-specific fields (e.g., `pg.merge_keys` and `merge_keys` are equivalent for pg). Universal block fields (`name`, `src`, `dest`) are never namespaced. Used to disambiguate when source and destination contribute overlapping field names to the same block.",
        "columns": "The `columns` field, when present on a destination kind, declares column transforms (rename/cast/projection). It is always a destination-side field, sources never accept `columns`. Run `ldrs schema columns` for the variant schema.",
        "param_keys": "Positional column types for a destination's prepared statement (currently pg.delete_insert's DELETE WHERE clause). Values come from `LDRS_PARAM_*` env vars, bound positionally in lexicographic order of the env-var name; the count of types here must match the count of bound values. When present, this overrides the per-position type hint from the `LDRS_PARAM_<NAME>_<TYPE>` env-var suffix. Run `ldrs schema columns` for the type variants.",
        "dotenv": "A `.env` file is loaded automatically, the working directory and its parents are searched up to root. Values do not override variables already set in the environment. ldrs only reads variables beginning with `LDRS_`; anything else is ignored.",
        "logging": "Log level is set by RUST_LOG (default `info`), read from the environment or `.env`, independent of the LDRS_ prefix rule. Examples: `RUST_LOG=error`, `RUST_LOG=debug`, `RUST_LOG=ldrs=debug` (per-target). Logs are written to stderr. A spawned child's stderr (ldrs-sf, duckdb) is captured rather than passed through: when the child fails it is quoted in the error, so a rejected query comes back with the engine's own message; when it succeeds, anything it wrote appears at debug level.",
        "duckdb_secrets": "Managed mode (`duckdb.query`) derives a `CREATE SECRET` from the src URL's scheme, scoped to that URL. No credential material is written into SQL: the secret uses `PROVIDER credential_chain`, which resolves credentials inside the child from its environment. The non-secret addressing it needs is read from the same variables object_store reads, so one set of cloud env vars configures a source and a destination alike. A read pointing outside the src URL is not covered by that scope.",
        "duckdb_extensions": "ldrs emits `LOAD nanoarrow` and never runs `INSTALL`. Provider extensions (azure, httpfs, and the rest) are duckdb's to resolve on use, per how your duckdb is installed and configured. To take that over, `INSTALL` in `pre_sql` or use `duckdb.raw`.",
        "env_vars": {
            "LDRS_SRC_<NAME>": "Per-table source URL; highest precedence. `<NAME>` is the block's `name`.",
            "LDRS_SRC_<KIND>": "Per-kind source URL (LDRS_SRC_FILE, LDRS_SRC_SF, LDRS_SRC_DUCKDB, LDRS_SRC_PG, LDRS_SRC_PQ, LDRS_SRC_DELTA); used when no per-name var matches.",
            "LDRS_SRC": "Global source URL fallback. Required for most kinds, one tier must resolve, in order LDRS_SRC_<NAME> → LDRS_SRC_<KIND> → LDRS_SRC. The URL is the base location, combined with the table's name/filename; its scheme also infers the source kind when --src is not passed: `snowflake://...` → sf, `delta+<scheme>://...` → delta, object-store URLs (`s3://`, `gs://`, `az://`, `file://`, ...) → file. A `postgres://` URL infers `pg`, which is a destination kind only: to read from Postgres, declare `--src duckdb.query` and address the table as `pg.<schema>.<table>`. A duckdb source may omit the URL entirely when its query needs no read location.",
            "LDRS_DEST[_<NAME>|_<KIND>]": "Destination location. Mirrors the LDRS_SRC_<NAME> → LDRS_SRC_<KIND> → LDRS_DEST chain exactly, with the same scheme→kind inference. Required.",
            "LDRS_PARAM_<NAME>[_<TYPE>]": "SQL parameter bindings consumed by query-shaped sources (e.g. sf.query). `LDRS_PARAM_P1=42` binds parameter `P1`. Append a type suffix to coerce: `LDRS_PARAM_P1_INT=42`. A duckdb source does not bind these positionally: it passes them to the child untouched and the query reads one by name with `getenv('LDRS_PARAM_P1')`, so no `param_keys` and no name scoping.",
            "LDRS_DUCKDB_BIN[_<NAME>]": "Path to the `duckdb` binary for a duckdb source. Defaults to `duckdb` on PATH. Per-table via the name suffix, so one table can pin a different build.",
            "LDRS_DUCKDB_DB[_<NAME>]": "Database file for a duckdb source to attach, passed as duckdb's positional argument. Absent runs in memory; present also adds `-readonly`, since a source only reads.",
            "LDRS_TEMPL_<NAME>": "Handlebars template variable. `LDRS_TEMPL_BUCKET=my-bucket` makes `{{ bucket }}` available inside templated string fields.",
        },
        "per_table_overrides": "SRC, DEST, TEMPL, and PARAM env vars can be scoped to one table by inserting the block's `name` (uppercased, `.` becomes `_`) after the prefix: `LDRS_TEMPL_PUBLIC_USERS_BUCKET` overrides `LDRS_TEMPL_BUCKET` only for the table named `public.users`. The unscoped variable is the default for all tables.",
        "templating": "String fields can contain handlebars templates rendered against the execution context. `{{ name }}` expands to the name property; custom variables come from `LDRS_TEMPL_<NAME>` env vars. Common use: `filename: \"{{ name }}/{{ name }}.snappy.parquet\"` builds a per-table path. Helpers are registered too: `{{ now_timestamp }}` emits Unix epoch **seconds**, so two runs in the same second render the same value; use `{{ now_timestamp_ms }}` where a filename has to stay distinct at a faster cadence. Rendering is strict, and a failed render names every bound variable and every registered helper in the error.",
        "examples": {
            "snowflake_to_local_parquet": "LDRS_DEST=file:///tmp/probe ldrs run --src sf.query --dest pq --name probe --sql 'SELECT 1 AS x' --opt filename=probe.snappy.parquet",
            "pipe_arrow_to_duckdb": "ldrs run --src sf.query --dest arrow --name probe --sql 'SELECT 1 AS x' | duckdb -c \"INSTALL nanoarrow FROM community; LOAD nanoarrow; FROM read_arrow('/dev/stdin')\"",
            "pipe_arrow_to_pyarrow": "ldrs run --src sf.query --dest arrow --name probe --sql 'SELECT 1' | python3 -c 'import pyarrow.ipc, sys; print(pyarrow.ipc.open_stream(sys.stdin.buffer).read_all())'",
            "parameterized_sf_query": "LDRS_PARAM_P1=42 LDRS_PARAM_P2=2026-01-01 ldrs run --src sf.query --dest pq --name probe --sql 'SELECT * FROM t WHERE org_id = ? AND created_at >= ?' --config-inline 'param_keys: [P1, P2]' --opt filename=probe.parquet Note: values bind in the order listed in param_keys; without param_keys they fall back to lexicographic order of the env-var name",
            "duckdb_csv_to_parquet": "LDRS_SRC=s3://lake/events/ LDRS_DEST=file:///tmp/out ldrs run --src duckdb.query --dest pq --name events --sql \"SELECT * FROM read_csv('{{ src_url }}*.csv')\" --opt filename=events.parquet Note: needs `duckdb` on PATH with the `nanoarrow` community extension installed; the src URL binds as {{ src_url }} and derives the object-store credentials",
            "duckdb_postgres_to_parquet": "LDRS_SRC=postgres://reader@db.internal/app LDRS_DEST=file:///tmp/out ldrs run --src duckdb.query --dest pq --name public.orders --sql 'SELECT * FROM pg.{{ name }}' --opt filename=orders.parquet Note: the connection is attached read-only as `pg` and its values travel in the child environment, so no credential reaches the SQL",
            "duckdb_param_by_name": "LDRS_PARAM_SINCE=2026-01-01 ldrs run --src duckdb.query --dest arrow --name events --sql \"SELECT * FROM read_parquet('{{ src_url }}*.parquet') WHERE day >= getenv('LDRS_PARAM_SINCE')\"",
            "duckdb_raw_preconfigured": "LDRS_DEST=file:///tmp/out ldrs run --src duckdb.raw --dest pq --name events --sql \"SELECT * FROM read_csv('az://lake/events/*.csv')\" --opt filename=events.parquet Note: raw runs against your duckdb as configured, with ldrs adding only nanoarrow and the Arrow wrapper."
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
    /// DuckDB source block
    Duckdb,
    /// Postgres destination block
    Pg,
    /// Parquet destination block
    Pq,
    /// Delta Lake destination block
    Delta,
    /// Arrow IPC stdout destination block
    Arrow,
    /// Post-load finalize item block (`run:` selects the kind, e.g. sf)
    Finalize,
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
        SchemaCommands::Duckdb => source_doc::<DuckDbSource>("duckdb", Some("duckdb")),
        SchemaCommands::Pg => dest_doc::<PgDestination>("pg", Some("pg")),
        SchemaCommands::Pq => dest_doc::<ParquetDestination>("pq", Some("pq")),
        SchemaCommands::Delta => dest_doc::<DeltaDestination>("delta", Some("delta")),
        SchemaCommands::Arrow => dest_doc::<ArrowDestination>("arrow", None),
        SchemaCommands::Finalize => finalize_doc(),
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

/// The post-load finalize item block (`FinalizeItem`). `run:` selects the kind;
/// the remaining fields are that kind's config. A v2/nested-only feature.
fn finalize_doc() -> Value {
    let mut g = SchemaGenerator::default();
    let block = g.subschema_for::<FinalizeItem>();
    let phase = g.subschema_for::<PhaseOutput>();
    json!({
        "$defs": g.take_definitions(true),
        "kind": "finalize",
        "finalize": block,
        "phase": phase,
        "handler": "The `lua` file must define `finalize(phase)`: called once per item with the run's output (`phase`, schema below), it returns the command list run against the item's target. Each destination carries its resolved `target`, post-cast `columns`, and (for URL-backed destinations) `full_url`; Parquet lists its written files; a Delta `result` carries the commit op (`overwrite`/`merge`, with merge stats including `skipped` for an idempotent no-op). Helpers: `outputs_of(phase, kind)` returns the destinations of a kind as a list, `render(template)` renders a config/identity template, `parse_path(pattern, path)` extracts named segments from a path, `parse_url(url)` decomposes a URL into scheme/host/path.",
        "execution": "Every returned command runs in order, stopping at the first error; each statement's result set is info-logged under phase=\"finalize\".",
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
        "note": "Each entry under `tables:` is a source block; its destinations come from the table's or the top-level `destinations:` list (each entry a destination block). Block shapes are opaque here and depend on the src/dest kind: see `ldrs schema <kind>`. A table's or the top-level `finalize:` list holds post-load items: see `ldrs schema finalize`.",
        "usage_ref": "ldrs schema usage",
    })
}

/// Env vars, templating, namespacing, and worked examples the context that
/// used to ride on every dump.
pub fn build_usage() -> Value {
    usage_block()
}
