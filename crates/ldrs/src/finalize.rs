//! Post-load finalize: run user-authored commands against a resolved target after a load.
//!
//! Each finalize item names a target kind (`run:`) and a Lua handler; the handler is given the
//! run's [`PhaseOutput`](crate::phase::PhaseOutput) and returns a command list, which is executed
//! against that item's resolved connection. Items are independent (none sees another's result), so
//! they run concurrently and their failures are collected, not short-circuited. Within one item the
//! command list is ordered and dependent, so execution stops at the first error.

use std::ffi::OsString;

use crate::ldrs_env::LdrsExecutionContext;
use crate::ldrs_snowflake::SnowflakeConnection;
use crate::lua_logic::UrlData;
use crate::path_pattern::{extracted_segments_to_value, PathPattern};
use ldrs_core::phase::PhaseOutput;
use mlua::{Lua, LuaOptions, LuaSerdeExt, StdLib};
use schemars::JsonSchema;
use serde::Deserialize;
use tracing::info;
use url::Url;

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
pub struct SfFinalize {
    #[serde(default)]
    pub target: Option<String>,
    pub lua: String,
}

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[serde(tag = "run", rename_all = "lowercase")]
pub enum FinalizeItem {
    Sf(SfFinalize),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum SfCommand {
    Sql(String),
}

/// A restricted Lua for finalize handlers: pure computation only (string / table / math), with no
/// `os` / `io` / `package` / `debug`.
fn finalize_lua() -> Result<Lua, mlua::Error> {
    Lua::new_with(
        StdLib::STRING | StdLib::TABLE | StdLib::MATH,
        LuaOptions::default(),
    )
}

/// `outputs_of(phase, kind)` returns the destinations of a given kind as a list (empty when none match).
const FINALIZE_PRELUDE: &str = r#"
function outputs_of(phase, kind)
    local out = {}
    for _, d in ipairs(phase.destinations) do
        if d.kind == kind then
            out[#out + 1] = d
        end
    end
    return out
end
"#;

pub fn call_finalize<T: serde::de::DeserializeOwned>(
    lua_path: &str,
    phase: &PhaseOutput,
    context: &LdrsExecutionContext<'_>,
) -> Result<Vec<T>, anyhow::Error> {
    let script = std::fs::read_to_string(lua_path)
        .map_err(|e| anyhow::anyhow!("failed to read finalize handler {lua_path}: {e}"))?;
    call_finalize_script(&script, lua_path, phase, context)
}

/// The Lua half, decoupled from the file read so it can be tested with an inline script.
fn call_finalize_script<T: serde::de::DeserializeOwned>(
    script: &str,
    source: &str,
    phase: &PhaseOutput,
    context: &LdrsExecutionContext<'_>,
) -> Result<Vec<T>, anyhow::Error> {
    let lua =
        finalize_lua().map_err(|e| anyhow::anyhow!("failed to initialize finalize lua: {e}"))?;
    lua.load(FINALIZE_PRELUDE)
        .exec()
        .map_err(|e| anyhow::anyhow!("failed to load finalize prelude: {e}"))?;
    lua.load(script)
        .exec()
        .map_err(|e| anyhow::anyhow!("lua error in {source}: {e}"))?;
    let func: mlua::Function = lua
        .globals()
        .get("finalize")
        .map_err(|_| anyhow::anyhow!("no `finalize` function defined in {source}"))?;
    let phase_value = lua
        .to_value(phase)
        .map_err(|e| anyhow::anyhow!("failed to serialize phase for {source}: {e}"))?;

    let ret: mlua::Value = lua
        .scope(|scope| {
            let render = scope.create_function(|_, template: String| {
                context
                    .render_template(&template)
                    .map_err(|e| mlua::Error::RuntimeError(format!("{e:#}")))
            })?;
            lua.globals().set("render", render)?;
            let parse_path = scope.create_function(|lua, (pattern, path): (String, String)| {
                let pat = PathPattern::new(&pattern)
                    .map_err(|e| mlua::Error::RuntimeError(format!("{e:#}")))?;
                let extracted = pat
                    .parse_path(&path)
                    .map_err(|e| mlua::Error::RuntimeError(format!("{e:#}")))?;
                lua.to_value(&extracted_segments_to_value(&extracted))
            })?;
            lua.globals().set("parse_path", parse_path)?;
            let parse_url = scope.create_function(|lua, url: String| {
                let parsed =
                    Url::parse(&url).map_err(|e| mlua::Error::RuntimeError(format!("{e:#}")))?;
                lua.to_value(&UrlData::from(parsed))
            })?;
            lua.globals().set("parse_url", parse_url)?;
            func.call::<mlua::Value>(phase_value)
        })
        .map_err(|e| anyhow::anyhow!("finalize() failed in {source}: {e}"))?;
    // A handler with no relevant work can `return {}` (or nothing) — both mean an empty command list.
    if ret.is_nil() {
        return Ok(Vec::new());
    }
    lua.from_value(ret)
        .map_err(|e| anyhow::anyhow!("finalize() in {source} did not return a command list: {e}"))
}

/// Run an ordered command list against Snowflake via `ldrs-sf exec` (a single spawn).
pub fn run_sf(
    conn: &SnowflakeConnection,
    commands: Vec<SfCommand>,
    ambient: Vec<(String, OsString)>,
) -> Result<(), String> {
    let statements: Vec<String> = commands
        .into_iter()
        .map(|SfCommand::Sql(sql)| sql)
        .collect();
    let output = conn
        .exec(&statements, ambient)
        .map_err(|e| format!("{e:#}"))?;
    info!(phase = "finalize", "sf finalize result: {output}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldrs_env::setup_handlebars;
    use ldrs_arrow::ColumnSpec;
    use ldrs_core::phase::{DeltaCommit, DestinationOutcome, PhaseOutput};

    fn delta_phase() -> PhaseOutput {
        PhaseOutput {
            name: "public.users".to_string(),
            source_files: None,
            success: true,
            rows: 10,
            destinations: vec![DestinationOutcome::Delta {
                target: "public.users".to_string(),
                full_url: "az://curated/acme/users".to_string(),
                columns: vec![
                    ColumnSpec::BigInt {
                        name: "id".to_string(),
                    },
                    ColumnSpec::Varchar {
                        name: "email".to_string(),
                        length: 255,
                    },
                ],
                result: Ok(DeltaCommit::Overwrite),
            }],
        }
    }

    fn test_handlebars() -> handlebars::Handlebars<'static> {
        let mut hb = handlebars::Handlebars::new();
        setup_handlebars(&mut hb);
        hb
    }

    #[test]
    fn finalize_reads_tagged_phase_and_returns_sql() {
        // The handler self-selects by kind (the `#[serde(tag = "kind")]` projection) and builds SQL
        // from the destination's location — plain strings that deserialize to SfCommand::Sql.
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            function finalize(phase)
                local cmds = {}
                for _, d in ipairs(phase.destinations) do
                    if d.kind == "delta" then
                        table.insert(cmds, "CREATE EXTERNAL TABLE t LOCATION '" .. d.full_url .. "'")
                        table.insert(cmds, "ALTER EXTERNAL TABLE t REFRESH")
                    end
                end
                return cmds
            end
        "#;
        let cmds = call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx).unwrap();
        assert_eq!(cmds.len(), 2);
        match &cmds[0] {
            SfCommand::Sql(sql) => assert!(sql.contains("az://curated/acme/users")),
        }
    }

    #[test]
    fn finalize_columns_reach_the_handler() {
        // The post-cast columns are on each destination: a handler builds typed DDL from them.
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            function finalize(phase)
                local cols = {}
                for _, d in ipairs(phase.destinations) do
                    for _, c in ipairs(d.columns) do
                        if c.type == "varchar" then
                            table.insert(cols, c.name .. " VARCHAR(" .. c.length .. ")")
                        else
                            table.insert(cols, c.name)
                        end
                    end
                end
                return { "COLS " .. table.concat(cols, ", ") }
            end
        "#;
        let cmds = call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx).unwrap();
        match &cmds[0] {
            SfCommand::Sql(sql) => assert_eq!(sql, "COLS id, email VARCHAR(255)"),
        }
    }

    #[test]
    fn finalize_outputs_of_filters_by_kind() {
        // `outputs_of` returns the destinations of a kind as a list; empty (not nil) when none match.
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            function finalize(phase)
                local deltas = outputs_of(phase, "delta")
                local pgs = outputs_of(phase, "pg")
                return { #deltas .. ":" .. deltas[1].target .. ":" .. #pgs }
            end
        "#;
        let cmds = call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx).unwrap();
        match &cmds[0] {
            SfCommand::Sql(sql) => assert_eq!(sql, "1:public.users:0"),
        }
    }

    #[test]
    fn finalize_sees_skipped_merge() {
        // A source-watermark idempotent skip must be distinguishable from a real merge so the
        // handler can decline to re-run downstream work.
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let phase = PhaseOutput {
            name: "public.users".to_string(),
            source_files: None,
            success: true,
            rows: 10,
            destinations: vec![DestinationOutcome::Delta {
                target: "public.users".to_string(),
                full_url: "az://curated/acme/users".to_string(),
                columns: vec![],
                result: Ok(DeltaCommit::Merge {
                    skipped: true,
                    skipped_version: Some(42),
                    source_rows: 10,
                    matched_rows: 0,
                    inserted_rows: 0,
                    files_scanned: 0,
                    files_written: 0,
                }),
            }],
        };
        let script = r#"
            function finalize(phase)
                local c = phase.destinations[1].result.Ok
                if c.op == "merge" and c.skipped then
                    return { "SKIP " .. c.skipped_version }
                end
                return { "RUN" }
            end
        "#;
        let cmds = call_finalize_script::<SfCommand>(script, "test", &phase, &ctx).unwrap();
        match &cmds[0] {
            SfCommand::Sql(sql) => assert_eq!(sql, "SKIP 42"),
        }
    }

    #[test]
    fn finalize_parse_path_extracts_segments() {
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            function finalize(phase)
                local seg = parse_path("{env}/{schema}.{table}", "prod/public.users")
                return { seg.env .. ":" .. seg.schema .. ":" .. seg.table }
            end
        "#;
        let cmds = call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx).unwrap();
        match &cmds[0] {
            SfCommand::Sql(sql) => assert_eq!(sql, "prod:public:users"),
        }
    }

    #[test]
    fn finalize_parse_url_decomposes_url() {
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            function finalize(phase)
                local u = parse_url("s3://my-bucket/prod/users")
                return { u.scheme .. ":" .. u.host }
            end
        "#;
        let cmds = call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx).unwrap();
        match &cmds[0] {
            SfCommand::Sql(sql) => assert_eq!(sql, "s3:my-bucket"),
        }
    }

    #[test]
    fn finalize_render_reaches_task_context() {
        // `render` is the handler's route to identity/config: `table_of "public.users"` → "users".
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            function finalize(phase)
                return { "CREATE TABLE " .. render("{{ table_of name }}") }
            end
        "#;
        let cmds = call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx).unwrap();
        match &cmds[0] {
            SfCommand::Sql(sql) => assert_eq!(sql, "CREATE TABLE users"),
        }
    }

    #[test]
    fn finalize_no_relevant_destination_returns_empty() {
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            function finalize(phase)
                local cmds = {}
                for _, d in ipairs(phase.destinations) do
                    if d.kind == "pg" then table.insert(cmds, "SELECT 1") end
                end
                return cmds
            end
        "#;
        let cmds = call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx).unwrap();
        assert!(cmds.is_empty());
    }

    #[test]
    fn finalize_with_no_return_is_empty() {
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = "function finalize(phase) end";
        let cmds = call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx).unwrap();
        assert!(cmds.is_empty());
    }

    #[test]
    fn finalize_rejects_unrecognized_command_shape() {
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            function finalize(phase)
                return { "CREATE TABLE t", { unexpected = "shape" } }
            end
        "#;
        let err =
            call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx).unwrap_err();
        assert!(err.to_string().contains("did not return a command list"));
    }

    #[test]
    fn finalize_os_is_unavailable() {
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = "function finalize(phase) return { os.getenv('HOME') } end";
        let err =
            call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx).unwrap_err();
        assert!(err.to_string().contains("finalize() failed"));
    }
}
