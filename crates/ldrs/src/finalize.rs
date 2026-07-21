//! Post-load finalize: run user-authored commands against a resolved target after a load.
//!
//! Each finalize item names a target kind (`run:`) and a Lua handler; the handler is given the
//! run's [`PhaseOutput`](crate::phase::PhaseOutput) and returns a command list, which is executed
//! against that item's resolved connection. Items are independent (none sees another's result), so
//! they run concurrently and their failures are collected, not short-circuited. Within one item the
//! command list is ordered and dependent, so execution stops at the first error.

use crate::ldrs_env::LdrsExecutionContext;
use crate::ldrs_snowflake::SnowflakeConnection;
use ldrs_core::phase::PhaseOutput;
use mlua::{Lua, LuaOptions, LuaSerdeExt, StdLib};
use schemars::JsonSchema;
use serde::Deserialize;
use tracing::info;

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
pub fn run_sf(conn: &SnowflakeConnection, commands: Vec<SfCommand>) -> Result<(), String> {
    let statements: Vec<String> = commands
        .into_iter()
        .map(|SfCommand::Sql(sql)| sql)
        .collect();
    let output = conn.exec(&statements).map_err(|e| format!("{e:#}"))?;
    info!(phase = "finalize", "sf finalize result: {output}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldrs_env::setup_handlebars;
    use ldrs_core::phase::{DeltaStrategy, DestinationOutcome, PhaseOutput};

    fn delta_phase() -> PhaseOutput {
        PhaseOutput {
            name: "public.users".to_string(),
            source_files: None,
            success: true,
            rows: 10,
            destinations: vec![DestinationOutcome::Delta {
                location: "az://curated/acme/users".to_string(),
                strategy: DeltaStrategy::Overwrite,
                result: Ok(()),
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
                        table.insert(cmds, "CREATE EXTERNAL TABLE t LOCATION '" .. d.location .. "'")
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
