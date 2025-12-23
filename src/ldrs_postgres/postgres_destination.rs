use crate::ldrs_postgres::ColumnType;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

const PG_NAMESPACE: &str = "pg";

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct PgCommon {
    pub name: String,
    #[serde(default)]
    pub pre_sql: Vec<String>,
    #[serde(default)]
    pub post_sql: Vec<String>,
    pub role: Option<String>,
    pub columns: Vec<ColumnType>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct PgDeleteInsert {
    pub name: String,
    #[serde(default)]
    pub pre_sql: Vec<String>,
    #[serde(default)]
    pub post_sql: Vec<String>,
    pub role: Option<String>,
    pub columns: Vec<ColumnType>,
    pub delete_keys: Vec<String>,
    pub on_conflict: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct PgMerge {
    pub name: String,
    #[serde(default)]
    pub pre_sql: Vec<String>,
    #[serde(default)]
    pub post_sql: Vec<String>,
    pub role: Option<String>,
    pub columns: Vec<ColumnType>,
    pub merge_keys: Vec<String>,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "dest")]
pub enum PgDestination {
    #[serde(rename = "pg.drop_replace")]
    DropReplace(PgCommon),
    #[serde(rename = "pg.truncate_insert")]
    TruncateInsert(PgCommon),
    #[serde(rename = "pg.merge")]
    Merge(PgMerge),
    #[serde(rename = "pg.delete_insert")]
    DeleteInsert(PgDeleteInsert),
}

fn get_either<'a>(yaml: &'a Value, ns_tag: &'a str, tag: &'a str) -> Option<&'a Value> {
    yaml.get(ns_tag).or(yaml.get(tag))
}

pub fn from_serde_yaml(yaml: &Value, tag: Option<&str>) -> Result<PgDestination, anyhow::Error> {
    let name = yaml
        .get("name")
        .and_then(|v| String::deserialize(v).ok())
        .ok_or(anyhow::anyhow!("Missing name"))?;
    // get common values
    let pre_sql = get_either(yaml, "pg.pre_sql", "pre_sql")
        .and_then(|v| Vec::<String>::deserialize(v).ok())
        .unwrap_or_default();
    let post_sql = get_either(yaml, "pg.post_sql", "post_sql")
        .and_then(|v| Vec::<String>::deserialize(v).ok())
        .unwrap_or_default();
    let role = get_either(yaml, "pg.role", "role").and_then(|v| String::deserialize(v).ok());
    let columns = get_either(yaml, "pg.columns", "columns")
        .and_then(|v| Vec::<ColumnType>::deserialize(v).ok())
        .unwrap_or_default();
    // get discriminators
    let merge_keys = get_either(yaml, "pg.merge_keys", "merge_keys")
        .and_then(|v| Vec::<String>::deserialize(v).ok());
    let delete_keys = get_either(yaml, "pg.delete_keys", "delete_keys")
        .and_then(|v| Vec::<String>::deserialize(v).ok());
    let on_conflict =
        get_either(yaml, "pg.on_conflict", "on_conflict").and_then(|v| String::deserialize(v).ok());

    // create destination
    match (merge_keys, delete_keys) {
        (Some(_), Some(_)) => Err(anyhow::anyhow!("Both merge_keys and delete_keys are set")),
        (Some(merge_keys), None) => match tag {
            Some("pg.merge") | None => Ok(PgDestination::Merge(PgMerge {
                name,
                pre_sql,
                post_sql,
                role,
                columns,
                merge_keys,
            })),
            _ => Err(anyhow::anyhow!("Invalid tag for merge destination")),
        },
        (None, Some(delete_keys)) => match tag {
            Some("pg.delete_insert") | None => Ok(PgDestination::DeleteInsert(PgDeleteInsert {
                name,
                pre_sql,
                post_sql,
                role,
                columns,
                delete_keys,
                on_conflict,
            })),
            _ => Err(anyhow::anyhow!("Invalid tag for delete_insert destination")),
        },
        (None, None) => match tag {
            Some("pg.drop_replace") => Ok(PgDestination::DropReplace(PgCommon {
                name,
                pre_sql,
                post_sql,
                role,
                columns,
            })),
            Some("pg.truncate_insert") => Ok(PgDestination::TruncateInsert(PgCommon {
                name,
                pre_sql,
                post_sql,
                role,
                columns,
            })),
            _ => Err(anyhow::anyhow!(
                "Must specify a tag for drop_replace or truncate_insert destination"
            )),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_destination() {
        let yaml = r#"
dest: pg.drop_replace
name: my_table
pre_sql:
  - CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);
post_sql:
  - DROP TABLE IF EXISTS my_table;
role: my_role
columns: []
"#;

        let pg_drop_replace: PgDestination = serde_yaml::from_str(yaml).unwrap();
        let pg_drop_replace_parse = from_serde_yaml(
            &(serde_yaml::from_str(yaml).unwrap()),
            Some("pg.drop_replace"),
        )
        .unwrap();

        let expected = PgDestination::DropReplace(PgCommon {
            name: "my_table".to_string(),
            pre_sql: vec![
                "CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);".to_string(),
            ],
            post_sql: vec!["DROP TABLE IF EXISTS my_table;".to_string()],
            role: Some("my_role".to_string()),
            columns: vec![],
        });

        assert_eq!(pg_drop_replace, expected);
        assert_eq!(pg_drop_replace_parse, expected);
    }

    #[test]
    fn test_pg_destination_truncate_insert() {
        let yaml = r#"
dest: pg.truncate_insert
name: my_table
pre_sql:
  - CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);
post_sql:
  - DROP TABLE IF EXISTS my_table;
role: my_role
columns: []
"#;

        let pg_truncate_insert: PgDestination = serde_yaml::from_str(yaml).unwrap();
        let pg_truncate_insert_parse = from_serde_yaml(
            &(serde_yaml::from_str(yaml).unwrap()),
            Some("pg.truncate_insert"),
        )
        .unwrap();

        let expected = PgDestination::TruncateInsert(PgCommon {
            name: "my_table".to_string(),
            pre_sql: vec![
                "CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);".to_string(),
            ],
            post_sql: vec!["DROP TABLE IF EXISTS my_table;".to_string()],
            role: Some("my_role".to_string()),
            columns: vec![],
        });

        assert_eq!(pg_truncate_insert, expected);
        assert_eq!(pg_truncate_insert_parse, expected);
    }

    #[test]
    fn test_pg_delete_insert() {
        let yaml = r#"
dest: pg.delete_insert
name: my_table
pre_sql:
  - CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);
post_sql:
  - DROP TABLE IF EXISTS my_table;
role: my_role
columns: []
delete_keys: [id]
"#;

        let pg_delete_insert: PgDestination = serde_yaml::from_str(yaml).unwrap();
        let pg_delete_insert_parse = from_serde_yaml(
            &(serde_yaml::from_str(yaml).unwrap()),
            Some("pg.delete_insert"),
        )
        .unwrap();

        let pg_delete_insert_infer =
            from_serde_yaml(&(serde_yaml::from_str(yaml).unwrap()), None).unwrap();

        let expected = PgDestination::DeleteInsert(PgDeleteInsert {
            name: "my_table".to_string(),
            pre_sql: vec![
                "CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);".to_string(),
            ],
            post_sql: vec!["DROP TABLE IF EXISTS my_table;".to_string()],
            role: Some("my_role".to_string()),
            columns: vec![],
            delete_keys: vec!["id".to_string()],
            on_conflict: None,
        });

        assert_eq!(pg_delete_insert, expected);
        assert_eq!(pg_delete_insert_parse, expected);
        assert_eq!(pg_delete_insert_infer, expected);
    }

    #[test]
    fn test_merge() {
        let yaml = r#"
dest: pg.merge
name: my_table
pre_sql:
  - CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);
post_sql:
  - DROP TABLE IF EXISTS my_table;
role: my_role
columns: []
merge_keys: [id]
"#;

        let pg_merge: PgDestination = serde_yaml::from_str(yaml).unwrap();
        let pg_merge_parsed =
            from_serde_yaml(&(serde_yaml::from_str(yaml).unwrap()), Some("pg.merge")).unwrap();

        let pg_merge_inferred =
            from_serde_yaml(&(serde_yaml::from_str(yaml).unwrap()), None).unwrap();

        let expected = PgDestination::Merge(PgMerge {
            name: "my_table".to_string(),
            pre_sql: vec![
                "CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);".to_string(),
            ],
            post_sql: vec!["DROP TABLE IF EXISTS my_table;".to_string()],
            role: Some("my_role".to_string()),
            columns: vec![],
            merge_keys: vec!["id".to_string()],
        });

        assert_eq!(pg_merge, expected);
        assert_eq!(pg_merge_parsed, expected);
        assert_eq!(pg_merge_inferred, expected);
    }
}
