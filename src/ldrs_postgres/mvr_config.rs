use crate::types::MvrColumn;
use anyhow::{bail, Context, Result};
use clap::Args;
use serde::Deserialize;
use std::convert::TryFrom;
use std::fs;
use tracing::warn;

#[derive(Deserialize, Debug, PartialEq)]
pub struct MvrToPGItemParsed {
    pub select_all: Option<String>,
    pub sql: Option<String>,
    #[serde(default)]
    pub columns: Vec<MvrColumn>,
    pub table: Option<String>,
    pub post_sql: Option<String>,
    pub role: Option<String>,
    pub batch_size: Option<usize>,
}

#[derive(Deserialize, Debug, PartialEq)]
pub struct MvrToPGParsed {
    pub role: Option<String>,
    pub batch_size: Option<usize>,
    pub tables: Vec<MvrToPGItemParsed>,
}

#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct MvrToPGConfig {
    pub sql: String,
    pub columns: Vec<MvrColumn>,
    pub table: String,
    pub post_sql: Option<String>,
    pub role: Option<String>,
    pub batch_size: Option<usize>,
}

#[derive(Args)]
pub struct MvrConfig {
    #[arg(long, short)]
    pub config: String,
    #[arg(long, short)]
    pub role: Option<String>,
    #[arg(long, short)]
    pub batch_size: Option<usize>,
}

/// Process YAML content with overrides and return the final configurations
pub fn process_yaml_with_overrides(
    yaml_content: &str,
    role_cli: Option<String>,
    batch_size_cli: Option<usize>,
) -> Result<Vec<MvrToPGConfig>> {
    let parsed_config: MvrToPGParsed =
        serde_yaml::from_str(&yaml_content).with_context(|| "Failed to parse YAML content")?;

    let mut configs = parsed_config
        .tables
        .into_iter()
        .map(|item| {
            MvrToPGConfig::try_from(item).with_context(|| "Failed to process table configuration")
        })
        .collect::<Result<Vec<_>>>()?;

    for config in &mut configs {
        // role priority: CLI args then values from parsed config
        if role_cli.is_some() {
            config.role = role_cli.clone();
        } else if config.role.is_none() {
            config.role = parsed_config.role.clone();
        }

        // same priority for batch size
        if let Some(bs) = batch_size_cli {
            config.batch_size = Some(bs);
        } else if config.batch_size.is_none() {
            config.batch_size = parsed_config.batch_size;
        }
    }

    Ok(configs)
}

/// Load and parse the MVR configuration file
pub fn load_config(config_args: &MvrConfig) -> Result<Vec<MvrToPGConfig>> {
    // Read the config file
    let config_content = fs::read_to_string(&config_args.config)
        .with_context(|| format!("Failed to read config file: {}", config_args.config))?;

    // Process the YAML with overrides from command-line arguments
    process_yaml_with_overrides(
        &config_content,
        config_args.role.clone(),
        config_args.batch_size,
    )
}

impl TryFrom<MvrToPGItemParsed> for MvrToPGConfig {
    type Error = anyhow::Error;

    fn try_from(item: MvrToPGItemParsed) -> Result<Self, Self::Error> {
        // Determine SQL - prioritize explicit sql if both are present
        let sql = match (&item.sql, &item.select_all) {
            (Some(s), Some(table_name)) => {
                warn!("Both 'sql' and 'select_all' specified for a table configuration. Using 'sql' and ignoring 'select_all: {}'", table_name);
                s.clone()
            }
            (Some(s), None) => s.clone(),
            (None, Some(table_name)) => format!("SELECT * FROM {}", table_name),
            _ => bail!("Neither 'sql' nor 'select_all' specified for a table configuration"),
        };

        // Get table name - required field
        let table = match &item.table {
            Some(t) => t.clone(),
            None => bail!("Table destination must be specified"),
        };

        Ok(MvrToPGConfig {
            sql,
            columns: item.columns,
            table,
            post_sql: item.post_sql,
            role: item.role,
            batch_size: item.batch_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_yaml_processing() {
        let yaml = r#"
            role: global_role
            batch_size: 500
            tables:
              - sql: "SELECT * FROM source_table"
                table: destination_table
                columns:
                  - name: id
                    type: int4
        "#;

        let configs = process_yaml_with_overrides(yaml, None, None).unwrap();

        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].sql, "SELECT * FROM source_table");
        assert_eq!(configs[0].table, "destination_table");
        assert_eq!(configs[0].columns.len(), 1);
        assert_eq!(configs[0].columns[0].name, "id");
        assert_eq!(configs[0].columns[0].ty, "int4");
        assert_eq!(configs[0].role, Some("global_role".to_string()));
        assert_eq!(configs[0].batch_size, Some(500));
    }

    #[test]
    fn test_select_all_conversion() {
        let yaml = r#"
            tables:
              - select_all: source_table
                table: destination_table
        "#;

        let configs = process_yaml_with_overrides(yaml, None, None).unwrap();

        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].sql, "SELECT * FROM source_table");
        assert_eq!(configs[0].table, "destination_table");
    }

    #[test]
    fn test_cli_overrides() {
        let yaml = r#"
            role: global_role
            batch_size: 500
            tables:
              - sql: SELECT * FROM source_table
                table: destination_table
        "#;

        let configs =
            process_yaml_with_overrides(yaml, Some("cli_role".to_string()), Some(1000)).unwrap();

        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].role, Some("cli_role".to_string()));
        assert_eq!(configs[0].batch_size, Some(1000));
    }

    #[test]
    fn test_multiple_tables() {
        let yaml = r#"
            role: global_role
            tables:
              - sql: "SELECT * FROM table1"
                table: dest1
              - sql: "SELECT * FROM table2"
                table: dest2
        "#;

        let configs = process_yaml_with_overrides(yaml, None, None).unwrap();

        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0].sql, "SELECT * FROM table1");
        assert_eq!(configs[0].table, "dest1");
        assert_eq!(configs[1].sql, "SELECT * FROM table2");
        assert_eq!(configs[1].table, "dest2");
    }

    #[test]
    fn test_table_level_overrides() {
        let yaml = r#"
            role: global_role
            batch_size: 500
            tables:
              - sql: "SELECT * FROM table1"
                table: dest1
                role: table_role
                batch_size: 200
                columns:
                  - name: id
                    type: int4
        "#;

        let configs = process_yaml_with_overrides(yaml, None, None).unwrap();

        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].role, Some("table_role".to_string()));
        assert_eq!(configs[0].batch_size, Some(200));
    }

    #[test]
    fn test_priority_override() {
        let yaml = r#"
            role: global_role
            batch_size: 500
            tables:
              - sql: "SELECT * FROM table1"
                table: dest1
                role: table_role
                batch_size: 200
                columns:
                  - name: id
                    type: int4
        "#;

        let configs =
            process_yaml_with_overrides(yaml, Some("cli_role".to_string()), Some(1000)).unwrap();

        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].role, Some("cli_role".to_string())); // CLI overrides table
        assert_eq!(configs[0].batch_size, Some(1000)); // CLI overrides table
    }

    #[test]
    fn test_missing_sql_and_select_all() {
        let yaml = r#"
            tables:
              - table: destination_table
                columns:
                  - name: id
                    type: int4
        "#;

        let result = process_yaml_with_overrides(yaml, None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_table() {
        let yaml = r#"
            tables:
              - sql: "SELECT * FROM source_table"
                columns:
                  - name: id
                    type: int4
        "#;

        let result = process_yaml_with_overrides(yaml, None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_both_sql_and_select_all() {
        let yaml = r#"
            tables:
              - sql: "SELECT * FROM source_table"
                select_all: other_table
                table: destination_table
        "#;

        let configs = process_yaml_with_overrides(yaml, None, None).unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].sql, "SELECT * FROM source_table");
    }
}
