use serde::{Deserialize, Serialize};
use serde_yaml::Value;

use crate::{
    ldrs_env::{get_env_values_by_keys, get_params_for_stmt_with_default},
    types::ColumnType,
};

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct SFQuery {
    pub sql: String,
    pub name: String,
    #[serde(default)]
    pub param_keys: Option<Vec<String>>,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct SFTable {
    pub name: String,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "src")]
pub enum SFSource {
    #[serde(rename = "sf.query")]
    Query(SFQuery),
    #[serde(rename = "sf.table")]
    Table(SFTable),
}

impl SFSource {
    pub fn get_name(&self) -> &str {
        match self {
            SFSource::Query(query) => &query.name,
            SFSource::Table(table) => &table.name,
        }
    }

    pub fn get_param_keys(&self) -> Option<&Vec<String>> {
        match self {
            SFSource::Query(query) => query.param_keys.as_ref(),
            SFSource::Table(_) => None,
        }
    }

    pub fn try_get_env_params(
        &self,
        name: &str,
        env_params: &[(String, String, Option<ColumnType>)],
    ) -> Result<Vec<(String, Option<ColumnType>)>, anyhow::Error> {
        match self {
            SFSource::Query(query) => match query.param_keys.as_ref() {
                None => Ok(get_params_for_stmt_with_default(Some(name), &env_params)),
                Some(keys) => {
                    let matched_keys = get_env_values_by_keys(&keys, &env_params);
                    // check that matched_keys is not fewer and if so return error
                    anyhow::ensure!(
                        matched_keys.len() == keys.len(),
                        "Not enough or too many environment variables. Expected {} but got {}",
                        keys.len(),
                        matched_keys.len()
                    );
                    Ok(matched_keys)
                }
            },
            SFSource::Table(_) => Ok(Vec::new()),
        }
    }
}

pub fn from_serde_yaml(yaml: &Value, tag: Option<&str>) -> Result<SFSource, anyhow::Error> {
    let name = yaml
        .get("sf.name")
        .or(yaml.get("name"))
        .and_then(|v| v.as_str());
    let sql = yaml
        .get("sf.sql")
        .or(yaml.get("sql"))
        .and_then(|v| v.as_str());
    let param_keys = yaml
        .get("sf.param_keys")
        .or(yaml.get("param_keys"))
        .and_then(|v| Vec::<String>::deserialize(v).ok());

    match (name, sql) {
        (Some(name), Some(sql)) => {
            // Both present - must be a query
            match tag {
                Some("sf.query") | Some("sf") | None => Ok(SFSource::Query(SFQuery {
                    sql: sql.to_string(),
                    name: name.to_string(),
                    param_keys,
                })),
                Some("sf.table") => Err(anyhow::Error::msg("Tag 'sf.table' but sql is present")),
                Some(_) => Err(anyhow::Error::msg("Invalid tag")),
            }
        }
        (Some(name), None) => {
            // Only name - must be a table
            match tag {
                Some("sf.table") | Some("sf") | None => Ok(SFSource::Table(SFTable {
                    name: name.to_string(),
                })),
                Some("sf.query") => Err(anyhow::Error::msg("Tag 'sf.query' but sql is missing")),
                Some(_) => Err(anyhow::Error::msg("Invalid tag")),
            }
        }
        (None, _) => Err(anyhow::Error::msg("Missing 'name' key")),
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
            param_keys: None,
        });
        assert_eq!(query, query_struct);
    }

    #[test]
    fn test_snowflake_serde_parse_ns() {
        let ns_table_yaml = r#"
sf.name: my_table
"#;

        let table_value: Value = serde_yaml::from_str(ns_table_yaml).unwrap();
        let table = from_serde_yaml(&table_value, Some("sf.table")).unwrap();
        let table_infer = from_serde_yaml(&table_value, None).unwrap();

        let table_struct = SFSource::Table(SFTable {
            name: "my_table".to_string(),
        });
        assert_eq!(table, table_struct);
        assert_eq!(table_infer, table_struct);

        let ns_query_yaml = r#"
sf.sql: "SELECT * FROM users WHERE created_at > '2024-01-01'"
sf.name: recent_users
"#;

        let query_value: Value = serde_yaml::from_str(ns_query_yaml).unwrap();
        let query = from_serde_yaml(&query_value, Some("sf.query")).unwrap();
        let query_infer = from_serde_yaml(&query_value, None).unwrap();

        let query_struct = SFSource::Query(SFQuery {
            sql: "SELECT * FROM users WHERE created_at > '2024-01-01'".to_string(),
            name: "recent_users".to_string(),
            param_keys: None,
        });
        assert_eq!(query, query_struct);
        assert_eq!(query_infer, query_struct);
    }

    #[test]
    fn test_try_get_env_params() {
        let default_env_params = vec![
            ("USER".to_string(), "user1".to_string(), None),
            ("CREATED".to_string(), "created1".to_string(), None),
            (
                "OTHER_QUERY_USER".to_string(),
                "other_user".to_string(),
                None,
            ),
        ];

        let sf_none = SFSource::Query(SFQuery {
            sql: "SELECT * FROM users WHERE user_id = ? and created_at > ?".to_string(),
            name: "RECENT_USERS".to_string(),
            param_keys: None,
        });

        let sf_user_key = SFSource::Query(SFQuery {
            sql: "SELECT * FROM users WHERE user_id = ?".to_string(),
            name: "RECENT_USERS".to_string(),
            param_keys: Some(vec!["USER".to_string()]),
        });

        let sf_other_user_key = SFSource::Query(SFQuery {
            sql: "SELECT * FROM users WHERE user_id = ?".to_string(),
            name: "RECENT_USERS".to_string(),
            param_keys: Some(vec!["OTHER_QUERY_USER".to_string()]),
        });

        let name = "RECENT_USERS".to_string();
        // no keys and params do not have name in them then default to vars not namepsaced
        let params = sf_none
            .try_get_env_params(&name, &default_env_params)
            .unwrap();
        assert_eq!(
            params,
            vec![("user1".to_string(), None), ("created1".to_string(), None),]
        );

        // specific keys match
        let params = sf_user_key
            .try_get_env_params(&name, &default_env_params)
            .unwrap();
        assert_eq!(params, vec![("user1".to_string(), None),]);

        // target other keys
        let params = sf_other_user_key
            .try_get_env_params(&name, &default_env_params)
            .unwrap();
        assert_eq!(params, vec![("other_user".to_string(), None),]);

        let env_params = vec![
            (
                "RECENT_USERS_USER".to_string(),
                "user_recent".to_string(),
                None,
            ),
            (
                "RECENT_USERS_CREATED".to_string(),
                "created_recent".to_string(),
                None,
            ),
            (
                "OTHER_QUERY_USER".to_string(),
                "other_user".to_string(),
                None,
            ),
        ];

        // now match based on named keys
        let params = sf_none.try_get_env_params(&name, &env_params).unwrap();
        assert_eq!(
            params,
            vec![
                ("user_recent".to_string(), None),
                ("created_recent".to_string(), None)
            ]
        );

        // target other keys
        let params = sf_other_user_key
            .try_get_env_params(&name, &env_params)
            .unwrap();
        assert_eq!(params, vec![("other_user".to_string(), None),]);

        // should fail with specific key of USER as the wanted and found are different
        let params = sf_user_key.try_get_env_params(&name, &env_params);
        assert_eq!(params.is_err(), true);
    }
}
