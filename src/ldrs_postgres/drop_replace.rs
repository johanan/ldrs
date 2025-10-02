use crate::ldrs_postgres::ColumnSchema;
use serde_yaml::Value;

#[derive(Debug)]
pub struct PgDropReplace<'a> {
    pub table: String,
    pub pre_sql: String,
    pub post_sql: String,
    pub role: String,
    pub columns: Vec<ColumnSchema<'a>>,
}

impl PgDropReplace<'_> {
    pub fn validate_yaml(yaml: &Value) -> Result<Self, String> {
        let table = yaml["table"].as_str().ok_or("table not found")?.to_string();
        let pre_sql = yaml["pre_sql"]
            .as_str()
            .ok_or("pre_sql not found")?
            .to_string();
        let post_sql = yaml["post_sql"]
            .as_str()
            .ok_or("post_sql not found")?
            .to_string();
        let role = yaml["role"].as_str().ok_or("role not found")?.to_string();
        let columns = yaml
            .get("columns")
            .map(|cols| cols.as_sequence())
            .or((&Vec::<Value>::new()));

        Ok(PgDropReplace {
            table,
            pre_sql,
            post_sql,
            role,
            columns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
