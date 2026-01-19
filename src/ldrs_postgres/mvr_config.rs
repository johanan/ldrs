use crate::types::ColumnSpec;
use anyhow::{bail, Context, Result};
use clap::Args;
use serde::Deserialize;
use std::convert::TryFrom;
use std::fs;
use tracing::warn;

/// Load and parse the MVR configuration file

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
                    type: integer
        "#;

        let configs = process_yaml_with_overrides(yaml, None, None).unwrap();

        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].sql, "SELECT * FROM source_table");
        assert_eq!(configs[0].table, "destination_table");
        assert_eq!(configs[0].columns.len(), 1);
        match &configs[0].columns[0] {
            ColumnSpec::Integer { name, .. } => assert_eq!(name, "id"),
            _ => panic!("Expected Integer column type"),
        }
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
