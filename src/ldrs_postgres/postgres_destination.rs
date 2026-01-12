use crate::{
    ldrs_postgres::{
        postgres_execution::{PgAction, PgDestCommand, PgMergeConfig, PgPreparedStmt},
        ColumnType,
    },
    types::{ColumnSchema, ColumnSpec},
};
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

const PG_NAMESPACE: &str = "pg";

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct PgCommon {
    pub name: String,
    pub pre_sql: Option<String>,
    pub post_sql: Option<String>,
    pub role: Option<String>,
    pub columns: Vec<ColumnSpec>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct PgDeleteInsert {
    pub name: String,
    pub pre_sql: Option<String>,
    pub post_sql: Option<String>,
    pub role: Option<String>,
    pub columns: Vec<ColumnSpec>,
    pub delete_keys: Vec<String>,
    pub param_keys: Option<Vec<ColumnType>>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct PgMerge {
    pub name: String,
    pub pre_sql: Option<String>,
    pub post_sql: Option<String>,
    pub role: Option<String>,
    pub columns: Vec<ColumnSpec>,
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

impl PgDestination {
    pub fn get_name(&self) -> &str {
        match self {
            PgDestination::DropReplace(common) => &common.name,
            PgDestination::TruncateInsert(common) => &common.name,
            PgDestination::Merge(merge) => &merge.name,
            PgDestination::DeleteInsert(delete_insert) => &delete_insert.name,
        }
    }

    pub fn get_columns(&self) -> Vec<ColumnSchema> {
        let cols = match self {
            PgDestination::DropReplace(common) => &common.columns,
            PgDestination::TruncateInsert(common) => &common.columns,
            PgDestination::Merge(merge) => &merge.columns,
            PgDestination::DeleteInsert(delete_insert) => &delete_insert.columns,
        };
        cols.iter().map(ColumnSchema::from).collect::<Vec<_>>()
    }
}

impl PgDestination {
    pub fn to_pg_commands(&self) -> Vec<PgDestCommand> {
        let role = match self {
            PgDestination::DropReplace(common) => common.role.as_ref(),
            PgDestination::TruncateInsert(common) => common.role.as_ref(),
            PgDestination::Merge(merge) => merge.role.as_ref(),
            PgDestination::DeleteInsert(delete_insert) => delete_insert.role.as_ref(),
        }
        .map(|role| PgDestCommand::Sql(format!("SET ROLE {}", role)));

        let pre_sql = match self {
            PgDestination::DropReplace(common) => common.pre_sql.as_ref(),
            PgDestination::TruncateInsert(common) => common.pre_sql.as_ref(),
            PgDestination::Merge(merge) => merge.pre_sql.as_ref(),
            PgDestination::DeleteInsert(delete_insert) => delete_insert.pre_sql.as_ref(),
        }
        .map(|sql| PgDestCommand::Sql(sql.clone()));

        let post_sql = match self {
            PgDestination::DropReplace(common) => common.post_sql.as_ref(),
            PgDestination::TruncateInsert(common) => common.post_sql.as_ref(),
            PgDestination::Merge(merge) => merge.post_sql.as_ref(),
            PgDestination::DeleteInsert(delete_insert) => delete_insert.post_sql.as_ref(),
        }
        .map(|sql| PgDestCommand::Sql(sql.clone()));

        let start = role.into_iter().chain(pre_sql.into_iter());

        match self {
            PgDestination::DropReplace(_) => start
                .chain([
                    PgDestCommand::Sql("CREATE SCHEMA IF NOT EXISTS {{ schema }};".to_string()),
                    PgDestCommand::Action(PgAction::CreateTable("{{ load_table }}".to_string())),
                    PgDestCommand::Load("{{ load_table }}".to_string()),
                    PgDestCommand::Sql(
                        r#"DROP TABLE IF EXISTS {{ name }};
                        SET search_path TO {{ schema }};
                        ALTER TABLE {{ load_table }} RENAME TO {{ table }};"#
                            .to_string(),
                    ),
                ])
                .chain(post_sql.into_iter())
                .collect::<Vec<PgDestCommand>>(),
            PgDestination::TruncateInsert(_) => start
                .chain([
                    PgDestCommand::Sql("CREATE SCHEMA IF NOT EXISTS {{ schema }};".to_string()),
                    PgDestCommand::Action(PgAction::CreateTable("{{ name }}".to_string())),
                    PgDestCommand::Sql("TRUNCATE TABLE {{ name }};".to_string()),
                    PgDestCommand::Load("{{ name }}".to_string()),
                ])
                .chain(post_sql.into_iter())
                .collect::<Vec<PgDestCommand>>(),
            PgDestination::DeleteInsert(del) => {
                let keys = del
                    .delete_keys
                    .iter()
                    .enumerate()
                    .map(|(i, k)| format!("{} = ${}", k, i + 1))
                    .collect::<Vec<String>>();

                let mut del_stmt = "DELETE FROM {{ name }} ".to_string();
                del_stmt.push_str(&format!("WHERE {}", keys.join(" AND ")));

                start
                    .chain([
                        PgDestCommand::Sql("CREATE SCHEMA IF NOT EXISTS {{ schema }};".to_string()),
                        PgDestCommand::Action(PgAction::CreateTable("{{ name }}".to_string())),
                        PgDestCommand::Prepared(PgPreparedStmt {
                            stmt: del_stmt,
                            key: Some("DEL".to_string()),
                            types: del.param_keys.clone(),
                        }),
                        PgDestCommand::Load("{{ name }}".to_string()),
                    ])
                    .chain(post_sql.into_iter())
                    .collect::<Vec<PgDestCommand>>()
            }
            PgDestination::Merge(merge) => start
                .chain([
                    PgDestCommand::Sql("CREATE SCHEMA IF NOT EXISTS {{ schema }};".to_string()),
                    PgDestCommand::Action(PgAction::CreateTable("{{ name }}".to_string())),
                    PgDestCommand::Action(PgAction::CreateTempTable(
                        "{{ load_table_name }}".to_string(),
                    )),
                    PgDestCommand::Load("{{ load_table_name }}".to_string()),
                    PgDestCommand::Action(PgAction::Merge(PgMergeConfig {
                        target: "{{ name }}".to_string(),
                        source: Some("{{ load_table_name }}".to_string()),
                        keys: merge.merge_keys.clone(),
                    })),
                ])
                .chain(post_sql.into_iter())
                .collect::<Vec<PgDestCommand>>(),
        }
    }
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
    let pre_sql =
        get_either(yaml, "pg.pre_sql", "pre_sql").and_then(|v| String::deserialize(v).ok());
    let post_sql =
        get_either(yaml, "pg.post_sql", "post_sql").and_then(|v| String::deserialize(v).ok());
    let role = get_either(yaml, "pg.role", "role").and_then(|v| String::deserialize(v).ok());
    let columns = get_either(yaml, "pg.columns", "columns")
        .and_then(|v| Vec::<ColumnSpec>::deserialize(v).ok())
        .unwrap_or_default();
    // get discriminators
    let merge_keys = get_either(yaml, "pg.merge_keys", "merge_keys")
        .and_then(|v| Vec::<String>::deserialize(v).ok());
    let delete_keys = get_either(yaml, "pg.delete_keys", "delete_keys")
        .and_then(|v| Vec::<String>::deserialize(v).ok());
    let param_keys = get_either(yaml, "pg.param_keys", "param_keys")
        .and_then(|v| Vec::<ColumnType>::deserialize(v).ok());

    // create destination
    match (merge_keys, delete_keys) {
        (Some(_), Some(_)) => Err(anyhow::anyhow!("Both merge_keys and delete_keys are set")),
        (Some(merge_keys), None) => match tag {
            Some("pg.merge") | Some("pg") | None => Ok(PgDestination::Merge(PgMerge {
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
            Some("pg.delete_insert") | Some("pg") | None => {
                Ok(PgDestination::DeleteInsert(PgDeleteInsert {
                    name,
                    pre_sql,
                    post_sql,
                    role,
                    columns,
                    delete_keys,
                    param_keys,
                }))
            }
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
pre_sql: CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);
post_sql: DROP TABLE IF EXISTS my_table;
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
            pre_sql: Some(
                "CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);".to_string(),
            ),
            post_sql: Some("DROP TABLE IF EXISTS my_table;".to_string()),
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
pre_sql: CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);
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
            pre_sql: Some(
                "CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);".to_string(),
            ),
            post_sql: None,
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
pre_sql: CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);
post_sql: DROP TABLE IF EXISTS my_table;
role: my_role
columns: []
delete_keys: [id]
param_keys: [Integer]
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
            pre_sql: Some(
                "CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);".to_string(),
            ),
            post_sql: Some("DROP TABLE IF EXISTS my_table;".to_string()),
            role: Some("my_role".to_string()),
            columns: vec![],
            delete_keys: vec!["id".to_string()],
            param_keys: Some(vec![ColumnType::Integer]),
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
            pre_sql: None,
            post_sql: None,
            role: Some("my_role".to_string()),
            columns: vec![],
            merge_keys: vec!["id".to_string()],
        });

        assert_eq!(pg_merge, expected);
        assert_eq!(pg_merge_parsed, expected);
        assert_eq!(pg_merge_inferred, expected);
    }
}
