use ldrs_arrow::ColumnSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize, JsonSchema)]
pub struct PgCommon {
    pub name: String,
    #[serde(default)]
    pub target: Option<String>,
    pub pre_sql: Option<String>,
    pub post_sql: Option<String>,
    pub role: Option<String>,
    #[serde(default)]
    #[schemars(schema_with = "crate::cli_schema::columns_schema")]
    pub columns: Vec<ColumnSpec>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize, JsonSchema)]
pub struct PgDeleteInsert {
    pub name: String,
    #[serde(default)]
    pub target: Option<String>,
    pub pre_sql: Option<String>,
    pub post_sql: Option<String>,
    pub role: Option<String>,
    #[serde(default)]
    #[schemars(schema_with = "crate::cli_schema::columns_schema")]
    pub columns: Vec<ColumnSpec>,
    pub delete_keys: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize, JsonSchema)]
pub struct PgMerge {
    pub name: String,
    #[serde(default)]
    pub target: Option<String>,
    pub pre_sql: Option<String>,
    pub post_sql: Option<String>,
    pub role: Option<String>,
    #[serde(default)]
    #[schemars(schema_with = "crate::cli_schema::columns_schema")]
    pub columns: Vec<ColumnSpec>,
    pub merge_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgPreparedStmt {
    pub stmt: String,
    /// The delete-key column names, in `$1..$n` order. Each resolves its bind value from the
    /// environment and its type from the load's columns.
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgMergeConfig {
    pub target: String,
    pub source: String,
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgDestCommand {
    CreateTable(String),
    CreateTempTable(String),
    Merge(PgMergeConfig),
    Load(String),
    Prepared(PgPreparedStmt),
    Sql(String),
}

pub fn validate_execution_plan(commands: &[PgDestCommand]) -> Result<(), anyhow::Error> {
    let load_actions = commands.iter().filter_map(|cmd| match cmd {
        PgDestCommand::Load(action) => Some(action),
        _ => None,
    });

    if load_actions.count() != 1 {
        Err(anyhow::Error::msg(
            "Execution plan must contain exactly one Load",
        ))
    } else {
        Ok(())
    }
}

pub struct PgPlan {
    pub before: Vec<PgDestCommand>,
    pub load_table: String,
    pub after: Vec<PgDestCommand>,
}

/// Split a command list around its single `Load`. Validates first, so the `Load` is
/// guaranteed present.
pub fn split_pg_plan(mut commands: Vec<PgDestCommand>) -> Result<PgPlan, anyhow::Error> {
    validate_execution_plan(&commands)?;
    let load_pos = commands
        .iter()
        .position(|c| matches!(c, PgDestCommand::Load(_)))
        .expect("validate_execution_plan ensures exactly one Load");
    let after = commands.split_off(load_pos + 1);
    let load_table = match commands.pop() {
        Some(PgDestCommand::Load(table)) => table,
        _ => unreachable!("position matched a Load command"),
    };
    Ok(PgPlan {
        before: commands,
        load_table,
        after,
    })
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "dest")]
#[schemars(
    description = "PostgreSQL destination. Writes rows to LDRS_DEST using the chosen sub-kind: merge / drop_replace / truncate_insert / delete_insert."
)]
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

    /// The optional `target` template; falls back to `name` at render time when absent.
    pub fn get_target(&self) -> Option<&str> {
        match self {
            PgDestination::DropReplace(common) => common.target.as_deref(),
            PgDestination::TruncateInsert(common) => common.target.as_deref(),
            PgDestination::Merge(merge) => merge.target.as_deref(),
            PgDestination::DeleteInsert(delete_insert) => delete_insert.target.as_deref(),
        }
    }

    pub fn get_columns(self) -> Vec<ColumnSpec> {
        match self {
            PgDestination::DropReplace(common) => common.columns,
            PgDestination::TruncateInsert(common) => common.columns,
            PgDestination::Merge(merge) => merge.columns,
            PgDestination::DeleteInsert(delete_insert) => delete_insert.columns,
        }
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
                    PgDestCommand::CreateTable("{{ load_table }}".to_string()),
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
                    PgDestCommand::CreateTable("{{ name }}".to_string()),
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
                        PgDestCommand::CreateTable("{{ name }}".to_string()),
                        PgDestCommand::Prepared(PgPreparedStmt {
                            stmt: del_stmt,
                            keys: del.delete_keys.clone(),
                        }),
                        PgDestCommand::Load("{{ name }}".to_string()),
                    ])
                    .chain(post_sql.into_iter())
                    .collect::<Vec<PgDestCommand>>()
            }
            PgDestination::Merge(merge) => start
                .chain([
                    PgDestCommand::Sql("CREATE SCHEMA IF NOT EXISTS {{ schema }};".to_string()),
                    PgDestCommand::CreateTable("{{ name }}".to_string()),
                    PgDestCommand::CreateTempTable("{{ load_table_name }}".to_string()),
                    PgDestCommand::Load("{{ load_table_name }}".to_string()),
                    PgDestCommand::Merge(PgMergeConfig {
                        target: "{{ name }}".to_string(),
                        source: "{{ load_table_name }}".to_string(),
                        keys: merge.merge_keys.clone(),
                    }),
                ])
                .chain(post_sql.into_iter())
                .collect::<Vec<PgDestCommand>>(),
        }
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
        let expected = PgDestination::DropReplace(PgCommon {
            name: "my_table".to_string(),
            target: None,
            pre_sql: Some(
                "CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);".to_string(),
            ),
            post_sql: Some("DROP TABLE IF EXISTS my_table;".to_string()),
            role: Some("my_role".to_string()),
            columns: vec![],
        });

        assert_eq!(pg_drop_replace, expected);
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
        let expected = PgDestination::TruncateInsert(PgCommon {
            name: "my_table".to_string(),
            target: None,
            pre_sql: Some(
                "CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);".to_string(),
            ),
            post_sql: None,
            role: Some("my_role".to_string()),
            columns: vec![],
        });

        assert_eq!(pg_truncate_insert, expected);
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
"#;

        let pg_delete_insert: PgDestination = serde_yaml::from_str(yaml).unwrap();
        let expected = PgDestination::DeleteInsert(PgDeleteInsert {
            name: "my_table".to_string(),
            target: None,
            pre_sql: Some(
                "CREATE TABLE IF NOT EXISTS my_table (id SERIAL PRIMARY KEY);".to_string(),
            ),
            post_sql: Some("DROP TABLE IF EXISTS my_table;".to_string()),
            role: Some("my_role".to_string()),
            columns: vec![],
            delete_keys: vec!["id".to_string()],
        });

        assert_eq!(pg_delete_insert, expected);
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
        let expected = PgDestination::Merge(PgMerge {
            name: "my_table".to_string(),
            target: None,
            pre_sql: None,
            post_sql: None,
            role: Some("my_role".to_string()),
            columns: vec![],
            merge_keys: vec!["id".to_string()],
        });

        assert_eq!(pg_merge, expected);
    }

    #[test]
    fn a_bare_drop_replace_block_parses() {
        let parsed: PgDestination = serde_yaml::from_str(
            r#"
dest: pg.drop_replace
name: my_table
"#,
        )
        .unwrap();
        assert_eq!(
            parsed,
            PgDestination::DropReplace(PgCommon {
                name: "my_table".to_string(),
                target: None,
                pre_sql: None,
                post_sql: None,
                role: None,
                columns: vec![],
            })
        );
    }

    #[test]
    fn a_stray_merge_keys_does_not_reshape_the_variant() {
        let parsed: PgDestination = serde_yaml::from_str(
            r#"
dest: pg.drop_replace
name: my_table
merge_keys: [id]
"#,
        )
        .unwrap();
        assert!(matches!(parsed, PgDestination::DropReplace(_)));
    }
}
