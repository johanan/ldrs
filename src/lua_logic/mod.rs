pub mod nom_pattern;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::storage::{azure::AzureUrl, StorageProvider};
use clap::Args;
use mlua::{Lua, LuaSerdeExt};
use serde::{Deserialize, Serialize};
use tracing::info;
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

impl From<&AzureUrl> for AzureData {
    fn from(azure_url: &AzureUrl) -> Self {
        AzureData {
            storage_account: azure_url.storage_account.clone(),
            container: azure_url.container.clone(),
            path: azure_url.path.clone(),
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

pub fn get_path_segments(path: &str) -> Vec<&str> {
    // Split path and filter out empty segments and filename
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    // Return all segments except the last one (which should be the filename)
    if segments.is_empty() {
        vec![]
    } else {
        segments[..segments.len() - 1].to_vec()
    }
}

fn build_module_file_paths(
    module_name: &str,
    function_components: &[&str],
    path_segments: &[&str],
    extension: Option<&str>,
) -> Vec<String> {
    let mut paths = Vec::new();

    // Build components with extension if provided
    let mut components_with_ext = function_components.to_vec();
    if let Some(ext) = extension {
        components_with_ext.push(ext);
    }

    if !path_segments.is_empty() {
        for segment in path_segments {
            let mut extended_components = components_with_ext.clone();
            extended_components.push(segment);
            let extended_flat_name = extended_components.join("_");

            let extended_base_path: PathBuf = [module_name]
                .iter()
                .chain(extended_components.iter())
                .collect();

            let mut extended_hierarchical = extended_base_path.clone();
            extended_hierarchical.push("init.lua");
            paths.push(extended_hierarchical.to_string_lossy().to_string());

            let mut extended_context = extended_base_path.clone();
            extended_context.set_extension("lua");
            paths.push(extended_context.to_string_lossy().to_string());

            let extended_flat_path =
                PathBuf::from(module_name).join(format!("{}.lua", extended_flat_name));
            paths.push(extended_flat_path.to_string_lossy().to_string());
        }
    }

    for i in (1..=components_with_ext.len()).rev() {
        let components = &components_with_ext[0..i];
        let flat_name = components.join("_");

        // Build base path using collect()
        let base_path: PathBuf = [module_name].iter().chain(components.iter()).collect();

        // Hierarchical patterns: module/azure/myaccount/init.lua
        let mut hierarchical_path = base_path.clone();
        hierarchical_path.push("init.lua");
        paths.push(hierarchical_path.to_string_lossy().to_string());

        // Context-named patterns: module/azure/myaccount.lua
        let mut context_path = base_path.clone();
        context_path.set_extension("lua");
        paths.push(context_path.to_string_lossy().to_string());

        // Flat patterns: module/azure_myaccount_datacontainer.lua
        let flat_path = PathBuf::from(module_name).join(format!("{}.lua", flat_name));
        paths.push(flat_path.to_string_lossy().to_string());
    }

    paths
}

pub fn build_module_lookups_for_url<'a>(
    modules: &'a [String],
    storage_provider: &'a StorageProvider,
) -> Vec<String> {
    let mut all_paths = Vec::new();
    let storage_url = storage_provider.get_url();
    let url_data = UrlData::from(storage_url);

    // Build function components based on storage type
    let (function_components, path_segments, extension): (
        Vec<&'a str>,
        Vec<&'a str>,
        Option<String>,
    ) = match storage_provider {
        StorageProvider::Azure(azure_url) => {
            // Build components directly from azure_url (lives as long as 'a)
            let components: Vec<&str> =
                vec!["azure", &azure_url.storage_account, &azure_url.container];

            // Get path segments directly from azure_url
            let segments = get_path_segments(&azure_url.path);

            (components, segments, url_data.extension.clone())
        }
        StorageProvider::Local(_, _) => {
            let components: Vec<&str> = vec!["local"];

            // Get extension (owned to avoid lifetime issues)
            let ext = url_data.extension.clone();

            // Local storage doesn't use path segments for now
            (components, vec![], ext)
        }
    };

    // Build file paths for each module (most specific modules first)
    for module in modules.iter().rev() {
        // Generate full hierarchy paths (most specific)
        let file_paths = build_module_file_paths(
            module,
            &function_components,
            &path_segments,
            extension.as_deref(),
        );
        all_paths.extend(file_paths);

        // Generate simpler scheme + extension paths (e.g., azure/parquet/init.lua)
        if let Some(ext) = &extension {
            let scheme = match storage_provider {
                StorageProvider::Azure(_) => "azure",
                StorageProvider::Local(_, _) => "local",
            };
            let simple_components = vec![scheme];
            let simple_paths = build_module_file_paths(module, &simple_components, &[], Some(ext));
            all_paths.extend(simple_paths);
        }
    }

    all_paths
}

pub fn build_module_lookups_with_semantic_segments(
    modules: &[String],
    semantic_segments: &[String],
    wildcard_segments: &[String],
    extension: Option<&str>,
) -> Vec<String> {
    let mut all_paths = Vec::new();

    // Convert to string slices for existing function
    let function_components: Vec<&str> = semantic_segments.iter().map(|s| s.as_str()).collect();
    let path_segments: Vec<&str> = wildcard_segments.iter().map(|s| s.as_str()).collect();

    // Build file paths for each module (most specific modules first)
    for module in modules.iter().rev() {
        let file_paths =
            build_module_file_paths(module, &function_components, &path_segments, extension);
        all_paths.extend(file_paths);
    }

    all_paths
}

// pub fn extract_semantic_segments(
//     pattern: &PathPattern,
//     path: &str,
// ) -> Result<(Vec<String>, Vec<String>), anyhow::Error> {
//     let extracted = pattern.extract(path)?;

//     // Build semantic segments in pattern order
//     let mut semantic_segments = Vec::new();
//     for segment in &pattern.segments {
//         if let PatternSegment::Named(name) = segment {
//             if let Some(value) = extracted.named_segments.get(name) {
//                 semantic_segments.push(value.clone());
//             }
//         }
//     }

//     Ok((semantic_segments, extracted.wildcard_segments))
// }

pub fn modules_from_args(args: LuaArgs) -> Result<Vec<String>, anyhow::Error> {
    let file_url = args.file_url.clone();
    let module_config = ModuleConfig::try_from(args)?;

    // Parse the storage provider from the URL
    let storage = StorageProvider::try_from_string(file_url.as_str())?;

    // Build module lookups
    let file_paths = build_module_lookups_for_url(&module_config.modules, &storage);

    Ok(file_paths)
}

pub struct LuaFunctionLoader {
    lua: Lua,
    loaded_files: HashSet<String>,
}

// impl LuaFunctionLoader {
//     pub fn new() -> Result<Self, mlua::Error> {
//         Ok(LuaFunctionLoader {
//             lua: Lua::new(),
//             loaded_files: HashSet::new(),
//         })
//     }

//     pub fn find_and_load_function(
//         &mut self,
//         module_paths: &[String],
//         function_name: &str,
//     ) -> Result<Option<mlua::Function>, anyhow::Error> {
//         for path in module_paths {
//             // Check if file exists first (cheap filesystem check)
//             if !Path::new(path).exists() {
//                 continue;
//             }

//             // Load file if not already loaded
//             if !self.loaded_files.contains(path) {
//                 let content = std::fs::read_to_string(path)?;
//                 self.lua.load(&content).exec()?;
//                 self.loaded_files.insert(path.clone());
//             }

//             // Check if function exists in global scope
//             if let Ok(func) = self.lua.globals().get::<_, mlua::Function>(function_name) {
//                 return Ok(Some(func));
//             }
//         }

//         Ok(None)
//     }

//     pub fn call_pre_exec(
//         &mut self,
//         func: mlua::Function,
//         url_data: &UrlData,
//     ) -> Result<Vec<String>, anyhow::Error> {
//         let result: Vec<String> = func.call(url_data)?;
//         Ok(result)
//     }

//     pub fn call_exec(
//         &mut self,
//         func: mlua::Function,
//         url_data: &UrlData,
//     ) -> Result<Vec<String>, anyhow::Error> {
//         let result: Vec<String> = func.call(url_data)?;
//         Ok(result)
//     }

//     pub fn call_post_exec(
//         &mut self,
//         func: mlua::Function,
//         url_data: &UrlData,
//     ) -> Result<Vec<String>, anyhow::Error> {
//         let result: Vec<String> = func.call(url_data)?;
//         Ok(result)
//     }

//     pub fn load_pre_exec(
//         &mut self,
//         module_paths: &[String],
//     ) -> Result<Option<mlua::Function>, anyhow::Error> {
//         self.find_and_load_function(module_paths, "pre_exec")
//     }

//     pub fn load_exec(
//         &mut self,
//         module_paths: &[String],
//     ) -> Result<Option<mlua::Function>, anyhow::Error> {
//         self.find_and_load_function(module_paths, "exec")
//     }

//     pub fn load_post_exec(
//         &mut self,
//         module_paths: &[String],
//     ) -> Result<Option<mlua::Function>, anyhow::Error> {
//         self.find_and_load_function(module_paths, "post_exec")
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_module_file_paths_single_component() {
        let paths = build_module_file_paths("base_azure", &["azure"], &[], None);

        assert_eq!(paths.len(), 3);
        assert!(paths.contains(&"base_azure/azure/init.lua".to_string()));
        assert!(paths.contains(&"base_azure/azure.lua".to_string()));
        assert!(paths.contains(&"base_azure/azure.lua".to_string())); // Flat pattern same as context in this case
    }

    #[test]
    fn test_build_module_file_paths_multiple_components() {
        let paths = build_module_file_paths(
            "company_mod",
            &["azure", "myaccount", "datacontainer"],
            &[],
            None,
        );

        // Should generate paths in order of specificity (most to least)
        let expected_paths = vec![
            "company_mod/azure/myaccount/datacontainer/init.lua",
            "company_mod/azure/myaccount/datacontainer.lua",
            "company_mod/azure_myaccount_datacontainer.lua",
            "company_mod/azure/myaccount/init.lua",
            "company_mod/azure/myaccount.lua",
            "company_mod/azure_myaccount.lua",
            "company_mod/azure/init.lua",
            "company_mod/azure.lua",
            "company_mod/azure.lua", // Flat pattern same as context for single component
        ];

        assert_eq!(paths.len(), expected_paths.len());
        for expected in expected_paths {
            assert!(
                paths.contains(&expected.to_string()),
                "Missing path: {}",
                expected
            );
        }
    }

    #[test]
    fn test_build_module_file_paths_with_file_extension() {
        let paths = build_module_file_paths(
            "staging_env",
            &["azure", "myaccount", "datacontainer"],
            &[],
            Some("csv"),
        );

        // Check most specific paths are generated
        assert!(
            paths.contains(&"staging_env/azure/myaccount/datacontainer/csv/init.lua".to_string())
        );
        assert!(paths.contains(&"staging_env/azure/myaccount/datacontainer/csv.lua".to_string()));
        assert!(paths.contains(&"staging_env/azure_myaccount_datacontainer_csv.lua".to_string()));

        // Check fallback paths are generated
        assert!(paths.contains(&"staging_env/azure/myaccount/datacontainer/init.lua".to_string()));
        assert!(paths.contains(&"staging_env/azure/init.lua".to_string()));
    }

    #[test]
    fn test_build_module_file_paths_empty_components() {
        let paths = build_module_file_paths("base_mod", &[], &[], None);

        // Should return empty vec for empty components
        assert!(paths.is_empty());
    }

    #[test]
    fn test_build_module_file_paths_ordering() {
        let paths = build_module_file_paths("test_mod", &["azure", "account"], &[], None);

        // Most specific should come first
        assert_eq!(paths[0], "test_mod/azure/account/init.lua");
        assert_eq!(paths[1], "test_mod/azure/account.lua");
        assert_eq!(paths[2], "test_mod/azure_account.lua");

        // Less specific should come after
        assert!(paths[3].starts_with("test_mod/azure/"));
        assert!(paths[4].starts_with("test_mod/azure"));
    }

    #[test]
    fn test_build_module_file_paths_with_path_segments() {
        let paths = build_module_file_paths(
            "env_mod",
            &["azure", "myaccount", "datacontainer"],
            &["staging", "raw"],
            Some("csv"),
        );

        // Should include original paths
        assert!(paths.contains(&"env_mod/azure/myaccount/datacontainer/csv/init.lua".to_string()));

        // Should include path segment extensions
        assert!(paths
            .contains(&"env_mod/azure/myaccount/datacontainer/csv/staging/init.lua".to_string()));
        assert!(
            paths.contains(&"env_mod/azure/myaccount/datacontainer/csv/staging.lua".to_string())
        );
        assert!(
            paths.contains(&"env_mod/azure_myaccount_datacontainer_csv_staging.lua".to_string())
        );

        assert!(
            paths.contains(&"env_mod/azure/myaccount/datacontainer/csv/raw/init.lua".to_string())
        );
        assert!(paths.contains(&"env_mod/azure/myaccount/datacontainer/csv/raw.lua".to_string()));
        assert!(paths.contains(&"env_mod/azure_myaccount_datacontainer_csv_raw.lua".to_string()));

        // Most specific (with path segments) should come first
        assert!(paths[0].contains("staging") || paths[0].contains("raw"));
    }

    #[test]
    fn test_build_module_file_paths_with_empty_path_segments() {
        let paths = build_module_file_paths("test_mod", &["azure"], &[], None);
        let paths_with_empty = build_module_file_paths("test_mod", &["azure"], &[], None);

        // Should be identical when path_segments is empty
        assert_eq!(paths, paths_with_empty);
    }

    #[test]
    fn test_build_module_file_paths_with_compound_extension() {
        let paths = build_module_file_paths(
            "prod_mod",
            &["azure", "myaccount", "datacontainer"],
            &["staging"],
            Some("csv_gz"),
        );

        // Should handle compound extensions properly
        assert!(paths.contains(
            &"prod_mod/azure/myaccount/datacontainer/csv_gz/staging/init.lua".to_string()
        ));
        assert!(paths
            .contains(&"prod_mod/azure/myaccount/datacontainer/csv_gz/staging.lua".to_string()));
        assert!(paths
            .contains(&"prod_mod/azure_myaccount_datacontainer_csv_gz_staging.lua".to_string()));

        // Should also include base compound extension paths
        assert!(
            paths.contains(&"prod_mod/azure/myaccount/datacontainer/csv_gz/init.lua".to_string())
        );
        assert!(paths.contains(&"prod_mod/azure/myaccount/datacontainer/csv_gz.lua".to_string()));
        assert!(paths.contains(&"prod_mod/azure_myaccount_datacontainer_csv_gz.lua".to_string()));

        // Verify compound extension comes from function components, not filename parsing
        assert!(!paths.iter().any(|p| p.contains("csv.gz")));
    }

    #[test]
    fn test_build_module_lookups_for_azure_url() {
        let url_string =
            "https://myaccount.blob.core.windows.net/datacontainer/staging/raw/data.csv.gz";
        let storage = StorageProvider::try_from_string(url_string).unwrap();
        let modules = vec!["base_azure".to_string(), "company_mod".to_string()];

        let paths = build_module_lookups_for_url(&modules, &storage);

        // Should have paths from both modules
        assert!(paths.iter().any(|p| p.contains("base_azure")));
        assert!(paths.iter().any(|p| p.contains("company_mod")));

        // Should include path segment extensions (most specific)
        assert!(paths.iter().any(|p| p.contains("csv_gz_staging")));
        assert!(paths.iter().any(|p| p.contains("csv_gz_raw")));

        // Should include base paths (less specific)
        assert!(paths
            .iter()
            .any(|p| p.contains("company_mod/azure/myaccount/datacontainer/csv_gz/init.lua")));

        // Most specific (company_mod) should come first since modules are reversed
        assert!(paths[0].contains("company_mod"));
    }

    #[test]
    fn test_build_module_lookups_for_local_url() {
        let url_string = "file:///home/data/file.csv";
        let storage = StorageProvider::try_from_string(url_string).unwrap();
        let modules = vec!["base_local".to_string()];

        let paths = build_module_lookups_for_url(&modules, &storage);

        assert!(paths
            .iter()
            .any(|p| p.contains("base_local/local/csv/init.lua")));
        assert!(paths.iter().any(|p| p.contains("base_local/local_csv.lua")));
    }

    #[test]
    fn test_build_module_lookups_from_args() {
        let args = LuaArgs {
            lua_modules: Some(vec!["base_azure".to_string(), "company_mod".to_string()]),
            lua_modules_json: None,
            file_url: "https://myaccount.blob.core.windows.net/datacontainer/data.csv".to_string(),
        };

        let paths = modules_from_args(args).unwrap();

        // Should have paths from both modules
        assert!(paths.iter().any(|p| p.contains("base_azure")));
        assert!(paths.iter().any(|p| p.contains("company_mod")));
        assert!(paths.iter().any(|p| p.contains("myaccount")));
        assert!(paths.iter().any(|p| p.contains("datacontainer")));
    }

    #[test]
    fn test_build_module_lookups_from_args_json() {
        let args = LuaArgs {
            lua_modules: None,
            lua_modules_json: Some(r#"["base_azure", "prod_env"]"#.to_string()),
            file_url: "https://myaccount.blob.core.windows.net/datacontainer/staging/data.csv.gz"
                .to_string(),
        };

        let paths = modules_from_args(args).unwrap();

        // Should handle compound extensions and path segments
        assert!(paths.iter().any(|p| p.contains("base_azure")));
        assert!(paths.iter().any(|p| p.contains("prod_env")));
        assert!(paths.iter().any(|p| p.contains("csv.gz")));
        assert!(paths.iter().any(|p| p.contains("staging")));
    }
}
