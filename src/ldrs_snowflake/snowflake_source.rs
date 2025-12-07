use serde::{Deserialize, Serialize};
use serde_yaml::Value;

const SF_NAMESPACE: &str = "sf";

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct SFBindVar {
    name: String,
    value: String,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct SFQuery {
    sql: String,
    name: String,
    #[serde(default)]
    bind_vars: Vec<SFBindVar>,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct SFTable {
    name: String,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "src")]
pub enum SFSource {
    #[serde(rename = "sf.query")]
    Query(SFQuery),
    #[serde(rename = "sf.table")]
    Table(SFTable),
}

pub fn from_serde_yaml(yaml: Value, tag: &str) -> Result<SFSource, anyhow::Error> {
    let (namespace, variant) = tag
        .split_once('.')
        .ok_or_else(|| anyhow::Error::msg("Invalid tag format, expected 'sf.<variant>'"))?;

    let name = yaml
        .get("sf.name")
        .or(yaml.get("name"))
        .map(|v| v.as_str().unwrap().to_string());
    match variant {
        "query" => {
            let sql = yaml
                .get("sf.sql")
                .or(yaml.get("sql"))
                .map(|v| v.as_str().unwrap().to_string());
            match (sql, name) {
                (Some(sql), Some(name)) => Ok(SFSource::Query(SFQuery {
                    sql,
                    name,
                    bind_vars: Vec::new(),
                })),
                _ => Err(anyhow::Error::msg("Missing 'sql' or 'name' key")),
            }
        }
        "table" => match name {
            Some(name) => Ok(SFSource::Table(SFTable { name })),
            None => Err(anyhow::Error::msg("Missing 'name' key")),
        },
        _ => Err(anyhow::Error::msg("Invalid variant")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snowflake_serde_parse() {
        let table_yaml = r#"
src: sf.table
name: my_table
"#;

        let table: SFSource = serde_yaml::from_str(table_yaml).unwrap();
        let table_struct = SFSource::Table(SFTable {
            name: "my_table".to_string(),
        });
        assert_eq!(table, table_struct);

        let sql_yaml = r#"
src: sf.query
sql: "SELECT * FROM users WHERE created_at > '2024-01-01'"
name: recent_users
"#;

        let query: SFSource = serde_yaml::from_str(sql_yaml).unwrap();
        let query_struct = SFSource::Query(SFQuery {
            sql: "SELECT * FROM users WHERE created_at > '2024-01-01'".to_string(),
            name: "recent_users".to_string(),
            bind_vars: Vec::new(),
        });
        assert_eq!(query, query_struct);
    }

    #[test]
    fn test_snowflake_serde_parse_ns() {
        let ns_table_yaml = r#"
sf.name: my_table
"#;

        let table_value: Value = serde_yaml::from_str(ns_table_yaml).unwrap();
        let table = from_serde_yaml(table_value, "sf.table").unwrap();
        let table_struct = SFSource::Table(SFTable {
            name: "my_table".to_string(),
        });
        assert_eq!(table, table_struct);

        let ns_query_yaml = r#"
sf.sql: "SELECT * FROM users WHERE created_at > '2024-01-01'"
sf.name: recent_users
"#;

        let query_value: Value = serde_yaml::from_str(ns_query_yaml).unwrap();
        let query = from_serde_yaml(query_value, "sf.query").unwrap();
        let query_struct = SFSource::Query(SFQuery {
            sql: "SELECT * FROM users WHERE created_at > '2024-01-01'".to_string(),
            name: "recent_users".to_string(),
            bind_vars: Vec::new(),
        });
        assert_eq!(query, query_struct);
    }
}
