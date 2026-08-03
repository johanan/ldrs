use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DuckDbBlock {
    pub name: String,
    #[schemars(
        description = "SQL to stream. Runs as `COPY (<sql>) TO '/dev/stdout' (FORMAT arrows)`, so it must be one SELECT expression: no trailing semicolon, no second statement. Setup such as `INSTALL` belongs in `pre_sql`. Handlebars-rendered; `{{ src_url }}` is the resolved LDRS_SRC location, bound verbatim, so a trailing slash can be ensured with `{{ ensure_trailing src_url }}`. Parameter values are read with `getenv('LDRS_PARAM_<NAME>')` rather than bound."
    )]
    pub sql: String,
    #[serde(default)]
    #[schemars(
        description = "Statements run after ldrs's overridable defaults (`.output`, and `SET TimeZone` in managed mode) and before the ones that may depend on them (`LOAD nanoarrow`, the derived secret or attach). So a `SET` here wins over ldrs's default, and an `INSTALL` here is in place before the `LOAD`."
    )]
    pub pre_sql: Option<String>,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "src")]
#[schemars(
    description = "DuckDB source. Spawns the `duckdb` CLI and reads its Arrow IPC output. Requires duckdb on PATH (or LDRS_DUCKDB_BIN) with the `nanoarrow` community extension installed. Both modes get `-bail`, `.output /dev/null`, `LOAD nanoarrow`, the Arrow COPY wrapper, `pre_sql`, `{{ src_url }}`, and the libpq vars derived from a `postgres://` URL. Managed mode adds `-no-init`, `SET TimeZone='UTC'`, and a `CREATE SECRET` or Postgres `ATTACH` derived from the src URL. `duckdb.raw` adds none of those, so your init file, extensions and stored secrets apply: use it when credential_chain cannot express the auth, or the session is already configured. Raw emits no `ATTACH`, so a `postgres://` URL leaves `pg.<schema>.<table>` an unknown catalog until you attach it in `pre_sql`."
)]
pub enum DuckDbSource {
    #[serde(rename = "duckdb.query")]
    Query(DuckDbBlock),
    #[serde(rename = "duckdb.raw")]
    Raw(DuckDbBlock),
}

impl DuckDbSource {
    pub fn block(&self) -> &DuckDbBlock {
        match self {
            DuckDbSource::Query(block) => block,
            DuckDbSource::Raw(block) => block,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.block().name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_kinds() {
        let query: DuckDbSource = serde_yaml::from_str(
            r#"
src: duckdb.query
name: events
sql: SELECT 1
"#,
        )
        .unwrap();
        assert!(matches!(query, DuckDbSource::Query(_)));
        assert_eq!(query.get_name(), "events");
        assert_eq!(query.block().pre_sql, None);

        let raw: DuckDbSource = serde_yaml::from_str(
            r#"
src: duckdb.raw
name: events
sql: SELECT 1
pre_sql: LOAD azure;
"#,
        )
        .unwrap();
        assert!(matches!(raw, DuckDbSource::Raw(_)));
        assert_eq!(raw.block().pre_sql.as_deref(), Some("LOAD azure;"));
    }
}
