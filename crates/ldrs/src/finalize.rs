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
    /// Lua module files loadable via `require` in this item's handler
    #[serde(default)]
    pub lua_modules: Vec<String>,
}

#[derive(Debug, PartialEq, Deserialize, JsonSchema)]
#[serde(tag = "run", rename_all = "lowercase")]
pub enum FinalizeItem {
    Sf(SfFinalize),
}

impl FinalizeItem {
    pub fn lua_modules(&self) -> &[String] {
        match self {
            FinalizeItem::Sf(sf) => &sf.lua_modules,
        }
    }
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

/// A `require` for any modules declared in the config
const FINALIZE_PRELUDE: &str = r#"
local loaded = {}
function require(name)
    if loaded[name] ~= nil then return loaded[name] end
    local src = __ldrs_sources[name]
        or error("module '" .. name .. "' not declared in lua_modules")
    local ret = load(src, "@" .. name)()
    if ret == nil then ret = true end
    loaded[name] = ret
    return ret
end
function __ldrs_seed(api)
    loaded.ldrs = api
end
"#;

const OUTPUTS_OF_SRC: &str = r#"
return function(phase, kind)
    local out = {}
    for _, d in ipairs(phase.destinations) do
        if d.kind == kind then
            out[#out + 1] = d
        end
    end
    return out
end
"#;

/// Reserved module name: `require "ldrs"` always resolves to the host api table.
pub const LDRS_MODULE: &str = "ldrs";

/// The `ldrs` module's surface. Each variant's doc comment is the schema doc line
/// (`EnumMessage`), its name derives from the variant (`IntoStaticStr`, snake_case), and
/// iteration (`EnumIter`) drives both the api table build and the schema docs
#[derive(Clone, Copy, strum::EnumIter, strum::EnumMessage, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum LdrsApi {
    /// outputs_of(phase, kind): the destinations of a kind, as a list (empty when none match)
    OutputsOf,
    /// render(template): render a config/identity template
    Render,
    /// parse_path(pattern, path): named segments extracted from a path
    ParsePath,
    /// parse_url(url): a URL decomposed into scheme/host/path
    ParseUrl,
}

impl LdrsApi {
    pub fn name(self) -> &'static str {
        self.into()
    }

    pub fn doc(self) -> &'static str {
        strum::EnumMessage::get_documentation(&self)
            .unwrap_or_default()
            .trim()
    }
}

/// Read the declared module files into `(stem, source)` pairs: the run-level list first,
/// then the item's list, an item stem replacing a run-level one.
pub fn build_sources(
    run_modules: &[String],
    item_modules: &[String],
) -> Result<Vec<(String, String)>, anyhow::Error> {
    let mut sources: Vec<(String, String)> = Vec::new();
    for path in run_modules.iter().chain(item_modules) {
        let stem = module_stem(path)?;
        let src = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read lua module {path}: {e}"))?;
        match sources.iter_mut().find(|(s, _)| *s == stem) {
            Some(entry) => entry.1 = src,
            None => sources.push((stem, src)),
        }
    }
    Ok(sources)
}

/// A module's `require` name: its file stem.
pub fn module_stem(path: &str) -> Result<String, anyhow::Error> {
    std::path::Path::new(path)
        .file_stem()
        .and_then(|s| s.to_str())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("lua module path '{path}' has no file stem"))
}

pub fn call_finalize<T: serde::de::DeserializeOwned>(
    lua_path: &str,
    phase: &PhaseOutput,
    context: &LdrsExecutionContext<'_>,
    sources: &[(String, String)],
) -> Result<Vec<T>, anyhow::Error> {
    let script = std::fs::read_to_string(lua_path)
        .map_err(|e| anyhow::anyhow!("failed to read finalize handler {lua_path}: {e}"))?;
    call_finalize_script(&script, lua_path, phase, context, sources)
}

/// The Lua half, decoupled from the file read so it can be tested with an inline script.
fn call_finalize_script<T: serde::de::DeserializeOwned>(
    script: &str,
    source: &str,
    phase: &PhaseOutput,
    context: &LdrsExecutionContext<'_>,
    sources: &[(String, String)],
) -> Result<Vec<T>, anyhow::Error> {
    let lua =
        finalize_lua().map_err(|e| anyhow::anyhow!("failed to initialize finalize lua: {e}"))?;
    lua.load(FINALIZE_PRELUDE)
        .exec()
        .map_err(|e| anyhow::anyhow!("failed to load finalize prelude: {e}"))?;
    let sources_tbl = lua
        .create_table()
        .map_err(|e| anyhow::anyhow!("failed to build lua module sources: {e}"))?;
    for (stem, src) in sources {
        sources_tbl
            .set(stem.as_str(), src.as_str())
            .map_err(|e| anyhow::anyhow!("failed to build lua module sources: {e}"))?;
    }
    lua.globals()
        .set("__ldrs_sources", sources_tbl)
        .map_err(|e| anyhow::anyhow!("failed to build lua module sources: {e}"))?;
    let phase_value = lua
        .to_value(phase)
        .map_err(|e| anyhow::anyhow!("failed to serialize phase for {source}: {e}"))?;

    // The handler script runs inside the scope so a top-level `require "ldrs"` resolves
    let ret: mlua::Value = lua
        .scope(|scope| {
            let api = lua.create_table()?;
            for entry in <LdrsApi as strum::IntoEnumIterator>::iter() {
                let f = match entry {
                    LdrsApi::OutputsOf => lua.load(OUTPUTS_OF_SRC).eval::<mlua::Function>()?,
                    LdrsApi::Render => scope.create_function(|_, template: String| {
                        context
                            .render_template(&template)
                            .map_err(|e| mlua::Error::RuntimeError(format!("{e:#}")))
                    })?,
                    LdrsApi::ParsePath => {
                        scope.create_function(|lua, (pattern, path): (String, String)| {
                            let pat = PathPattern::new(&pattern)
                                .map_err(|e| mlua::Error::RuntimeError(format!("{e:#}")))?;
                            let extracted = pat
                                .parse_path(&path)
                                .map_err(|e| mlua::Error::RuntimeError(format!("{e:#}")))?;
                            lua.to_value(&extracted_segments_to_value(&extracted))
                        })?
                    }
                    LdrsApi::ParseUrl => scope.create_function(|lua, url: String| {
                        let parsed = Url::parse(&url)
                            .map_err(|e| mlua::Error::RuntimeError(format!("{e:#}")))?;
                        lua.to_value(&UrlData::from(parsed))
                    })?,
                };
                api.set(entry.name(), f)?;
            }
            let seed: mlua::Function = lua.globals().get("__ldrs_seed")?;
            seed.call::<()>(api)?;
            lua.load(script).exec()?;
            let func: mlua::Function = lua.globals().get("finalize").map_err(|_| {
                mlua::Error::RuntimeError("no `finalize` function defined".to_string())
            })?;
            func.call::<mlua::Value>(phase_value)
        })
        .map_err(|e| anyhow::anyhow!("finalize in {source}: {e}"))?;
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
        let cmds =
            call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &[]).unwrap();
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
        let cmds =
            call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &[]).unwrap();
        match &cmds[0] {
            SfCommand::Sql(sql) => assert_eq!(sql, "COLS id, email VARCHAR(255)"),
        }
    }

    #[test]
    fn finalize_outputs_of_filters_by_kind() {
        // `ldrs.outputs_of` returns the destinations of a kind as a list; empty (not nil) when none match.
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            local ldrs = require "ldrs"
            function finalize(phase)
                local deltas = ldrs.outputs_of(phase, "delta")
                local pgs = ldrs.outputs_of(phase, "pg")
                return { #deltas .. ":" .. deltas[1].target .. ":" .. #pgs }
            end
        "#;
        let cmds =
            call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &[]).unwrap();
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
        let cmds = call_finalize_script::<SfCommand>(script, "test", &phase, &ctx, &[]).unwrap();
        match &cmds[0] {
            SfCommand::Sql(sql) => assert_eq!(sql, "SKIP 42"),
        }
    }

    #[test]
    fn finalize_parse_path_extracts_segments() {
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            local ldrs = require "ldrs"
            function finalize(phase)
                local seg = ldrs.parse_path("{env}/{schema}.{table}", "prod/public.users")
                return { seg.env .. ":" .. seg.schema .. ":" .. seg.table }
            end
        "#;
        let cmds =
            call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &[]).unwrap();
        match &cmds[0] {
            SfCommand::Sql(sql) => assert_eq!(sql, "prod:public:users"),
        }
    }

    #[test]
    fn finalize_parse_url_decomposes_url() {
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            local ldrs = require "ldrs"
            function finalize(phase)
                local u = ldrs.parse_url("s3://my-bucket/prod/users")
                return { u.scheme .. ":" .. u.host }
            end
        "#;
        let cmds =
            call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &[]).unwrap();
        match &cmds[0] {
            SfCommand::Sql(sql) => assert_eq!(sql, "s3:my-bucket"),
        }
    }

    #[test]
    fn finalize_render_reaches_task_context() {
        // `ldrs.render` is the handler's route to identity/config: `table_of "public.users"` → "users".
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            local ldrs = require "ldrs"
            function finalize(phase)
                return { "CREATE TABLE " .. ldrs.render("{{ table_of name }}") }
            end
        "#;
        let cmds =
            call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &[]).unwrap();
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
        let cmds =
            call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &[]).unwrap();
        assert!(cmds.is_empty());
    }

    #[test]
    fn finalize_with_no_return_is_empty() {
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = "function finalize(phase) end";
        let cmds =
            call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &[]).unwrap();
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
        let err = call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &[])
            .unwrap_err();
        assert!(err.to_string().contains("did not return a command list"));
    }

    #[test]
    fn finalize_os_is_unavailable() {
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = "function finalize(phase) return { os.getenv('HOME') } end";
        let err = call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &[])
            .unwrap_err();
        assert!(err.to_string().contains("finalize in test"));
    }

    #[test]
    fn ldrs_api_entries_are_documented() {
        // The doc comment is the schema doc line; a variant without one would emit "".
        for entry in <LdrsApi as strum::IntoEnumIterator>::iter() {
            assert!(
                !entry.doc().is_empty(),
                "LdrsApi::{} has no doc comment",
                entry.name()
            );
        }
    }

    #[test]
    fn finalize_requires_a_declared_module() {
        // A module chunk returning a table is bound to its stem, `require`-style.
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let module = r#"
            local M = {}
            function M.greet(name) return "hello " .. name end
            return M
        "#;
        let script = r#"
            local util = require "util"
            function finalize(phase)
                return { util.greet("finalize") }
            end
        "#;
        let sources = vec![("util".to_string(), module.to_string())];
        let cmds =
            call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &sources)
                .unwrap();
        match &cmds[0] {
            SfCommand::Sql(sql) => assert_eq!(sql, "hello finalize"),
        }
    }

    #[test]
    fn finalize_module_can_require_ldrs() {
        // Modules load lazily during the finalize call, so the host api is available to them.
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let module = r#"
            local ldrs = require "ldrs"
            local M = {}
            function M.deltas(phase) return ldrs.outputs_of(phase, "delta") end
            return M
        "#;
        let script = r#"
            local util = require "util"
            function finalize(phase)
                return { "COUNT " .. #util.deltas(phase) }
            end
        "#;
        let sources = vec![("util".to_string(), module.to_string())];
        let cmds =
            call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &sources)
                .unwrap();
        match &cmds[0] {
            SfCommand::Sql(sql) => assert_eq!(sql, "COUNT 1"),
        }
    }

    #[test]
    fn finalize_undeclared_module_errors() {
        let hb = test_handlebars();
        let ctx = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let script = r#"
            local missing = require "missing"
            function finalize(phase) return {} end
        "#;
        let err = call_finalize_script::<SfCommand>(script, "test", &delta_phase(), &ctx, &[])
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("module 'missing' not declared in lua_modules"));
    }

    #[test]
    fn build_sources_item_overrides_run_level_by_stem() {
        let dir = std::env::temp_dir().join(format!("ldrs-mod-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir(&dir).unwrap();
        let run = dir.join("util.lua");
        let item_dir = dir.join("item");
        std::fs::create_dir(&item_dir).unwrap();
        let item = item_dir.join("util.lua");
        std::fs::write(&run, "return { from = 'run' }").unwrap();
        std::fs::write(&item, "return { from = 'item' }").unwrap();

        let sources = build_sources(
            &[run.to_string_lossy().into_owned()],
            &[item.to_string_lossy().into_owned()],
        )
        .unwrap();
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].0, "util");
        assert!(sources[0].1.contains("item"));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
