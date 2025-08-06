use clap::Args;
use serde::{Deserialize, Serialize};

use crate::path_pattern::{build_module_path_from_pattern, PathPattern};
use crate::{path_pattern, storage::StorageProvider};

#[derive(Args, Debug)]
pub struct LuaArgs {
    /// Lua module paths (can be specified multiple times)
    #[arg(long = "lua-module", action = clap::ArgAction::Append)]
    pub lua_modules: Option<Vec<String>>,

    /// Lua modules as JSON array (alternative to --lua-module)
    #[arg(long = "lua-modules-json")]
    pub lua_modules_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleConfig {
    pub modules: Vec<String>,
}

impl TryFrom<LuaArgs> for ModuleConfig {
    type Error = anyhow::Error;

    fn try_from(args: LuaArgs) -> Result<Self, Self::Error> {
        let modules = match (args.lua_modules, args.lua_modules_json) {
            // Both specified - error
            (Some(_), Some(_)) => {
                return Err(anyhow::anyhow!(
                    "Cannot specify both --lua-module and --lua-modules-json"
                ));
            }

            // Repeating --lua-module args
            (Some(modules), None) => {
                if modules.is_empty() {
                    return Err(anyhow::anyhow!("No lua modules specified"));
                }
                modules
            }

            // JSON array
            (None, Some(json_str)) => {
                let parsed: Vec<String> = serde_json::from_str(&json_str)
                    .map_err(|e| anyhow::anyhow!("Invalid JSON in --lua-modules-json: {}", e))?;

                if parsed.is_empty() {
                    return Err(anyhow::anyhow!("Empty module list in JSON"));
                }
                parsed
            }

            // Neither specified - use default
            (None, None) => {
                return Err(anyhow::anyhow!(
                    "No Lua modules specified. Use --lua-module or --lua-modules-json"
                ));
            }
        };

        Ok(ModuleConfig { modules })
    }
}

pub fn modules_from_args<'a>(
    args: LuaArgs,
    file_url: &'a str,
    pattern_str: &'a str,
) -> Result<(PathPattern<'a>, StorageProvider, Vec<String>), anyhow::Error> {
    let module_config = ModuleConfig::try_from(args)?;

    let pattern = path_pattern::PathPattern::new(&pattern_str)?;

    let storage = StorageProvider::try_from_string(file_url)?;

    let mut all_paths = Vec::new();
    for module in &module_config.modules {
        let paths = build_module_path_from_pattern(&pattern, module);
        all_paths.extend(paths);
    }

    Ok((pattern, storage, all_paths))
}
