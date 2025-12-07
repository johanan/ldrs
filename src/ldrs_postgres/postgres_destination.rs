use crate::ldrs_postgres::ColumnType;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

#[derive(Debug, Deserialize)]
pub struct PgDropReplace {
    pub table: String,
    #[serde(default)]
    pub pre_sql: Vec<String>,
    #[serde(default)]
    pub post_sql: Vec<String>,
    pub role: Option<String>,
    pub columns: Vec<ColumnType>,
}
