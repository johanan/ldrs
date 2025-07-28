pub mod nom_pattern;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::storage::{azure::AzureUrl, StorageProvider};
use crate::types::{
    parquet_types::ParquetSchema, ColumnMapping, ColumnSchema, TableSchema, TimeUnit,
};
use arrow::datatypes::SchemaRef;
use clap::Args;
use mlua::{FromLua, Lua, LuaSerdeExt};
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlData {
    pub full_url: String,
    pub scheme: String,
    pub host: Option<String>,
    pub path: String,
    pub filename: Option<String>,
    pub extension: Option<String>,
    pub file_stem: Option<String>,
    pub full_extension: Option<String>,
    pub base_name: Option<String>,
    pub query: Option<String>,
    pub fragment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureData {
    pub storage_account: String,
    pub container: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalData {
    pub url: String,
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StorageData {
    #[serde(rename = "azure")]
    Azure(AzureData),
    #[serde(rename = "local")]
    Local(LocalData),
}

#[derive(Args, Debug)]
pub struct LuaArgs {
    /// Lua module paths (can be specified multiple times)
    #[arg(long = "lua-module", action = clap::ArgAction::Append)]
    pub lua_modules: Option<Vec<String>>,

    /// Lua modules as JSON array (alternative to --lua-module)
    #[arg(long = "lua-modules-json")]
    pub lua_modules_json: Option<String>,

    /// The file URL to process
    #[arg(long)]
    pub file_url: String,

    /// Pattern for semantic path matching
    #[arg(long)]
    pub pattern: String,
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

impl From<Url> for UrlData {
    fn from(url: Url) -> Self {
        let path = Path::new(url.path());
        let filename = path
            .file_name()
            .and_then(osstr_to_string)
            .map(|s| s.to_string());
        let extension = path
            .extension()
            .and_then(osstr_to_string)
            .map(|s| s.to_string());
        let file_stem = path
            .file_stem()
            .and_then(osstr_to_string)
            .map(|s| s.to_string());
        let full_extension = filename
            .as_ref()
            .and_then(|f| full_extension(f))
            .map(|s| s.to_string());
        let base_name = filename
            .as_ref()
            .and_then(|f| base_name(f))
            .map(|s| s.to_string());

        UrlData {
            full_url: url.to_string(),
            scheme: url.scheme().to_string(),
            host: url.host_str().map(|s| s.to_string()),
            path: url.path().to_string(),
            filename,
            extension,
            file_stem,
            full_extension,
            base_name,
            query: url.query().map(|s| s.to_string()),
            fragment: url.fragment().map(|s| s.to_string()),
        }
    }
}

impl From<AzureUrl> for AzureData {
    fn from(azure_url: AzureUrl) -> Self {
        AzureData {
            storage_account: azure_url.storage_account.clone(),
            container: azure_url.container.clone(),
            path: azure_url.path.clone(),
        }
    }
}

impl From<StorageProvider> for StorageData {
    fn from(provider: StorageProvider) -> Self {
        match provider {
            StorageProvider::Azure(azure_url) => StorageData::Azure(AzureData::from(azure_url)),
            StorageProvider::Local(url, path) => StorageData::Local(LocalData {
                url: url.to_string(),
                path: path.to_string(),
            }),
        }
    }
}

fn osstr_to_string(os_str: &std::ffi::OsStr) -> Option<&str> {
    os_str.to_str()
}

fn full_extension(file_name: &str) -> Option<&str> {
    file_name.find('.').map(|pos| &file_name[pos + 1..])
}

fn base_name(file_name: &str) -> Option<&str> {
    file_name.find('.').map(|pos| &file_name[..pos])
}

pub fn build_module_path_from_pattern(
    pattern: &nom_pattern::PathPattern,
    base_module: &str,
) -> Vec<String> {
    let mut module_components: Vec<String> = vec![base_module.to_string()];

    // Build module path using segment groups to preserve compound names
    for segment_group in &pattern.segment_groups {
        if segment_group.len() == 1 {
            // Simple segment - can avoid allocation for most cases
            match &segment_group[0] {
                nom_pattern::PatternSegment::Named(name) => {
                    module_components.push(name.to_string());
                }
                nom_pattern::PatternSegment::Literal(literal) => {
                    module_components.push(literal.to_string());
                }
                nom_pattern::PatternSegment::Wildcard => {
                    break;
                }
                nom_pattern::PatternSegment::Placeholder => {
                    // Placeholder segment should be ignored
                }
            }
        } else {
            // Compound segment - concatenate all parts
            let compound_parts: Vec<&str> = segment_group
                .iter()
                .filter_map(|segment| {
                    match segment {
                        nom_pattern::PatternSegment::Named(name) => Some(*name),
                        nom_pattern::PatternSegment::Literal(literal) => Some(*literal),
                        _ => None, // Skip wildcards/placeholders in compound segments
                    }
                })
                .collect();

            if !compound_parts.is_empty() {
                module_components.push(compound_parts.join(""));
            }
        }
    }

    let mut paths = Vec::new();

    // Hierarchical: module/schema_table/load_type/init.lua
    let mut hierarchical = PathBuf::from_iter(&module_components);
    hierarchical.push("init.lua");
    paths.push(hierarchical.to_string_lossy().to_string());

    // Context-named: module/schema_table/load_type.lua
    if module_components.len() > 1 {
        let mut context = PathBuf::from_iter(&module_components);
        let context_with_extension = format!("{}.lua", context.to_string_lossy());
        paths.push(context_with_extension);
    }

    // Flat: module/schema_table_load_type.lua
    if module_components.len() > 1 {
        let semantic_parts = &module_components[1..];
        let flat_name = format!("{}.lua", semantic_parts.join("_"));
        let flat = PathBuf::from(module_components[0].clone()).join(flat_name);
        paths.push(flat.to_string_lossy().to_string());
    }

    paths
}

pub fn modules_from_args(args: LuaArgs) -> Result<Vec<String>, anyhow::Error> {
    let file_url = args.file_url.clone();
    let pattern_str = args.pattern.clone();
    let module_config = ModuleConfig::try_from(args)?;

    // Parse the pattern
    let pattern = nom_pattern::PathPattern::new(&pattern_str)?;

    // Parse the storage provider from the URL (for future use)
    let _storage = StorageProvider::try_from_string(file_url.as_str())?;

    // Build module lookups for each module using the pattern
    let mut all_paths = Vec::new();
    for module in &module_config.modules {
        let paths = build_module_path_from_pattern(&pattern, module);
        all_paths.extend(paths);
    }

    Ok(all_paths)
}

pub struct LuaFunctionLoader {
    lua: Lua,
    loaded_files: HashSet<String>,
}

impl LuaFunctionLoader {
    pub fn new() -> Result<Self, mlua::Error> {
        Ok(LuaFunctionLoader {
            lua: Lua::new(),
            loaded_files: HashSet::new(),
        })
    }

    pub fn setup_execution_context<'a>(
        &'a mut self,
        parquet_schema: Option<ParquetSchema<'a>>,
    ) -> Result<(), anyhow::Error> {
        // Set global parquet_schema value
        let value = match parquet_schema {
            Some(schema) => self
                .lua
                .to_value(&schema)
                .map_err(|e| anyhow::anyhow!("Failed to serialize parquet schema: {}", e))?,
            None => mlua::Value::Nil,
        };

        self.lua
            .globals()
            .set("parquet_schema", value)
            .map_err(|e| anyhow::anyhow!("Failed to set parquet_schema global: {}", e))?;
        Ok(())
    }

    pub fn find_and_load_function(
        &mut self,
        module_paths: &[String],
        function_name: &str,
    ) -> Result<Option<mlua::Function>, anyhow::Error> {
        // First-wins: check paths in order
        for path in module_paths {
            // Check existence first (cheap syscall)
            if !Path::new(path).exists() {
                continue;
            }

            // Load file if not already cached
            if !self.loaded_files.contains(path) {
                let content = std::fs::read_to_string(path)
                    .map_err(|e| anyhow::anyhow!("Failed to read {}: {}", path, e))?;

                // Fail fast on syntax errors
                self.lua
                    .load(&content)
                    .exec()
                    .map_err(|e| anyhow::anyhow!("Lua syntax error in {}: {}", path, e))?;

                self.loaded_files.insert(path.clone());
            }

            // Check if function exists (first-wins)
            if let Ok(func) = self.lua.globals().get::<mlua::Function>(function_name) {
                return Ok(Some(func));
            }
        }

        Ok(None) // Function not found in any file
    }

    pub fn call_lua_function<T: serde::de::DeserializeOwned>(
        &mut self,
        func: mlua::Function,
        url_data: &UrlData,
        storage_data: &StorageData,
        segments_value: &serde_json::Value,
        schema: Option<&SchemaRef>,
        context: &serde_json::Value,
    ) -> Result<T, anyhow::Error> {
        let url_value = self
            .lua
            .to_value(url_data)
            .map_err(|e| anyhow::anyhow!("Failed to serialize UrlData: {}", e))?;
        let storage_value = self
            .lua
            .to_value(storage_data)
            .map_err(|e| anyhow::anyhow!("Failed to serialize StorageData: {}", e))?;
        let segments_lua_value = self
            .lua
            .to_value(segments_value)
            .map_err(|e| anyhow::anyhow!("Failed to serialize segments: {}", e))?;
        let schema_value = match schema {
            Some(s) => self
                .lua
                .to_value(s)
                .map_err(|e| anyhow::anyhow!("Failed to serialize Schema: {}", e))?,
            None => mlua::Value::Nil,
        };
        let context_value = self
            .lua
            .to_value(context)
            .map_err(|e| anyhow::anyhow!("Failed to serialize context: {}", e))?;

        let lua_result: mlua::Value = func
            .call((
                url_value,
                storage_value,
                segments_lua_value,
                schema_value,
                context_value,
            ))
            .map_err(|e| anyhow::anyhow!("Lua call failed: {}", e))?;
        let result: T = self
            .lua
            .from_value(lua_result)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize result: {}", e))?;
        Ok(result)
    }

    pub fn call_process<T: serde::de::DeserializeOwned>(
        &mut self,
        module_paths: &[String],
        url_data: &UrlData,
        storage_data: &StorageData,
        segments_value: &serde_json::Value,
        schema: Option<&SchemaRef>,
        context: &serde_json::Value,
    ) -> Result<T, anyhow::Error> {
        let func = self
            .find_and_load_function(module_paths, "process")?
            .ok_or_else(|| anyhow::anyhow!("Process function not found in any module"))?;

        self.call_lua_function(
            func,
            url_data,
            storage_data,
            segments_value,
            schema,
            context,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Simple test result type for lua_logic tests
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestResult {
        pub sql: Vec<String>,
        pub schema: TableSchema,
    }

    #[test]
    fn test_build_module_path_from_pattern() {
        let pattern = nom_pattern::PathPattern::new("{environment}/{dataset}/{format}/*").unwrap();
        let paths = build_module_path_from_pattern(&pattern, "company_mod");

        // Should generate three variations
        assert_eq!(paths.len(), 3);

        // Check hierarchical path
        assert!(paths.contains(&"company_mod/environment/dataset/format/init.lua".to_string()));

        // Check context-named path
        assert!(paths.contains(&"company_mod/environment/dataset/format.lua".to_string()));

        // Check flat path
        assert!(paths.contains(&"company_mod/environment_dataset_format.lua".to_string()));
    }

    #[test]
    fn test_build_module_path_from_pattern_with_literal() {
        let pattern = nom_pattern::PathPattern::new("{environment}/{dataset}/streaming/*").unwrap();
        let paths = build_module_path_from_pattern(&pattern, "base_mod");

        // Should use literal "streaming" in paths
        assert!(paths.contains(&"base_mod/environment/dataset/streaming/init.lua".to_string()));
        assert!(paths.contains(&"base_mod/environment/dataset/streaming.lua".to_string()));
        assert!(paths.contains(&"base_mod/environment_dataset_streaming.lua".to_string()));
    }

    #[test]
    fn test_build_module_path_from_pattern_single_segment() {
        let pattern = nom_pattern::PathPattern::new("{pipeline}/*").unwrap();
        let paths = build_module_path_from_pattern(&pattern, "test_mod");

        // Should generate paths for single semantic segment
        assert!(paths.contains(&"test_mod/pipeline/init.lua".to_string()));
        assert!(paths.contains(&"test_mod/pipeline.lua".to_string()));
        assert!(paths.contains(&"test_mod/pipeline.lua".to_string())); // Flat same as context for single
    }

    #[test]
    fn test_call_pre_exec_with_three_parameters() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        use url::Url;

        // Create test URL data
        let test_url =
            Url::parse("https://myaccount.blob.core.windows.net/container/path/file.parquet")
                .unwrap();
        let url_data = UrlData::from(test_url);

        // Create test storage data (Azure)
        let storage_data = StorageData::Azure(AzureData {
            storage_account: "myaccount".to_string(),
            container: "container".to_string(),
            path: "path/file.parquet".to_string(),
        });

        // Create test extracted segments
        let pattern = nom_pattern::PathPattern::new("{environment}/{schema_table}/*").unwrap();
        let path = "/prod/public.users/2025/06/25/file.parquet";
        let extracted = pattern.parse_path(path).unwrap();
        let segments_value = nom_pattern::extracted_segments_to_value(&extracted);

        // Create test schema with id column as int
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        // Create empty context for pre_exec
        let context = serde_json::json!({});

        // Create Lua loader and load a simple test function
        let mut loader = LuaFunctionLoader::new().unwrap();

        // Load a simple Lua function that returns PreExecResult
        let lua_script = r#"
            function process(url_data, storage_data, segments, schema, context)
                return {
                    sql = {"-- setup SQL"},
                    schema = {
                        table_name = "temp_users_123",
                        columns = {
                            {name = "id", column_type = "Uuid", conversion = "string_to_uuid"},
                            {name = "metadata", column_type = "jsonb"}
                        }
                    },
                }
            end
        "#;

        loader.lua.load(lua_script).exec().unwrap();
        let func = loader
            .lua
            .globals()
            .get::<mlua::Function>("process")
            .unwrap();

        // Test the call
        let result = loader
            .call_lua_function::<TestResult>(
                func,
                &url_data,
                &storage_data,
                &segments_value,
                Some(&schema),
                &context,
            )
            .unwrap();

        assert_eq!(result.sql, vec!["-- setup SQL"]);
        assert_eq!(result.schema.table_name, "temp_users_123");
        assert_eq!(result.schema.columns.len(), 2);
        assert_eq!(result.schema.columns[0].name, "id");
        assert_eq!(result.schema.columns[0].column_type, "Uuid".to_string());
        // assert_eq!(
        //     result.schema.columns[0].conversion,
        //     Some("string_to_uuid".to_string())
        // );
        assert_eq!(result.schema.columns[1].name, "metadata");
        // assert_eq!(
        //     result.schema.columns[1].override_type,
        //     Some("jsonb".to_string())
        // );
    }
}
