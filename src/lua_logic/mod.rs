use std::collections::HashSet;
use std::path::Path;

use crate::storage::{azure::AzureUrl, StorageProvider};
use crate::types::parquet_types::ParquetSchema;
use arrow::datatypes::SchemaRef;
use mlua::{Lua, LuaSerdeExt};
use serde::{Deserialize, Serialize};
use tracing::debug;
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
        debug!(
            "Calling process function with module paths: {:?}",
            module_paths
        );
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
    use crate::{path_pattern, types::TableSchema};

    // Simple test result type for lua_logic tests
    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestResult {
        pub sql: Vec<String>,
        pub schema: TableSchema,
    }

    #[test]
    fn test_call_process_with_three_parameters() {
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
        let pattern = path_pattern::PathPattern::new("{environment}/{schema_table}/*").unwrap();
        let path = "/prod/public.users/2025/06/25/file.parquet";
        let extracted = pattern.parse_path(path).unwrap();
        let segments_value = path_pattern::extracted_segments_to_value(&extracted);

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
