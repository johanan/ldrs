use ldrs_arrow::ColumnSpec;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

fn default_app_id() -> String {
    "ldrs-merge-{{ name }}".to_string()
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DeltaCommon {
    pub name: String,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default, alias = "delta.columns")]
    #[schemars(schema_with = "crate::cli_schema::columns_schema")]
    pub columns: Vec<ColumnSpec>,
    #[serde(alias = "delta.max_rows")]
    pub max_rows: Option<usize>,
    #[serde(alias = "delta.max_bytes")]
    pub max_bytes: Option<usize>,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TxnMode {
    SourceWatermark,
    ProcessingTime,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DeltaMerge {
    #[serde(flatten)]
    pub common: DeltaCommon,
    #[serde(alias = "delta.merge_keys")]
    pub merge_keys: Vec<String>,
    #[serde(default, alias = "delta.allow_null_keys")]
    pub allow_null_keys: bool,
    #[serde(alias = "delta.txn_mode")]
    pub txn_mode: Option<TxnMode>,
    #[serde(alias = "delta.watermark_column")]
    pub watermark_column: Option<String>,
    #[serde(alias = "delta.batch_version")]
    pub batch_version: Option<String>,
    #[serde(default = "default_app_id", alias = "delta.app_id")]
    pub app_id: String,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "dest")]
#[schemars(
    description = "Delta Lake destination. Writes to a table at LDRS_DEST + name; pick delta.overwrite or delta.merge for the operation."
)]
pub enum DeltaDestination {
    #[serde(rename = "delta.overwrite")]
    Overwrite(DeltaCommon),
    #[serde(rename = "delta.merge")]
    Merge(DeltaMerge),
}

/// Raw (unrendered) txn config from YAML. String templates are rendered at dispatch
/// time against the execution context before constructing the runtime `TxnConfig`.
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub enum MergeTxnConfig {
    SourceWatermark {
        app_id: String,
        watermark_column: String,
    },
    ProcessingTime {
        app_id: String,
        batch_version: Option<String>,
    },
}

/// Look up a key under a namespaced form first (`prefix.key`), then fall back to bare `key`.
fn get_ns<'a>(value: &'a Value, prefix: &str, key: &str) -> Option<&'a Value> {
    let ns_key = format!("{}.{}", prefix, key);
    value.get(&ns_key).or(value.get(key))
}

pub fn validate(value: &Value, parsed: &DeltaDestination) -> Result<(), anyhow::Error> {
    match parsed {
        DeltaDestination::Overwrite(_) => {
            if get_ns(value, "delta", "merge_keys").is_some() {
                anyhow::bail!("merge_keys not allowed with dest: delta.overwrite");
            }
        }
        DeltaDestination::Merge(m) => {
            if m.merge_keys.is_empty() {
                anyhow::bail!("merge_keys cannot be empty");
            }
            if matches!(m.txn_mode, Some(TxnMode::SourceWatermark)) && m.watermark_column.is_none()
            {
                anyhow::bail!("source_watermark requires watermark_column");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_and_validate(yaml: &str) -> Result<DeltaDestination, anyhow::Error> {
        let value: Value = serde_yaml::from_str(yaml)?;
        let parsed: DeltaDestination = serde_yaml::from_value(value.clone())?;
        validate(&value, &parsed)?;
        Ok(parsed)
    }

    #[test]
    fn overwrite_bare() {
        let dest = parse_and_validate(
            r#"
dest: delta.overwrite
name: public.users
"#,
        )
        .unwrap();
        match dest {
            DeltaDestination::Overwrite(c) => assert_eq!(c.name, "public.users"),
            _ => panic!("expected Overwrite"),
        }
    }

    #[test]
    fn overwrite_namespaced_aliases() {
        let dest = parse_and_validate(
            r#"
dest: delta.overwrite
name: public.users
delta.max_rows: 1000
delta.max_bytes: 2000
"#,
        )
        .unwrap();
        match dest {
            DeltaDestination::Overwrite(c) => {
                assert_eq!(c.max_rows, Some(1000));
                assert_eq!(c.max_bytes, Some(2000));
            }
            _ => panic!("expected Overwrite"),
        }
    }

    #[test]
    fn merge_bare() {
        let dest = parse_and_validate(
            r#"
dest: delta.merge
name: public.users
merge_keys: [id]
"#,
        )
        .unwrap();
        match dest {
            DeltaDestination::Merge(m) => {
                assert_eq!(m.common.name, "public.users");
                assert_eq!(m.merge_keys, vec!["id".to_string()]);
                assert!(!m.allow_null_keys);
                assert!(m.txn_mode.is_none());
            }
            _ => panic!("expected Merge"),
        }
    }

    #[test]
    fn merge_namespaced_aliases() {
        let dest = parse_and_validate(
            r#"
dest: delta.merge
name: public.users
delta.merge_keys: [id, name]
delta.allow_null_keys: true
"#,
        )
        .unwrap();
        match dest {
            DeltaDestination::Merge(m) => {
                assert_eq!(m.merge_keys, vec!["id".to_string(), "name".to_string()]);
                assert!(m.allow_null_keys);
            }
            _ => panic!("expected Merge"),
        }
    }

    #[test]
    fn merge_source_watermark() {
        let dest = parse_and_validate(
            r#"
dest: delta.merge
name: public.users
merge_keys: [id]
txn_mode: source_watermark
watermark_column: updated_at
app_id: my-app
"#,
        )
        .unwrap();
        match dest {
            DeltaDestination::Merge(m) => {
                assert_eq!(m.txn_mode, Some(TxnMode::SourceWatermark));
                assert_eq!(m.watermark_column.as_deref(), Some("updated_at"));
                assert_eq!(m.app_id, "my-app");
            }
            _ => panic!("expected Merge"),
        }
    }

    #[test]
    fn merge_processing_time() {
        let dest = parse_and_validate(
            r#"
dest: delta.merge
name: public.users
merge_keys: [id]
txn_mode: processing_time
batch_version: "{{ run_id }}"
"#,
        )
        .unwrap();
        match dest {
            DeltaDestination::Merge(m) => {
                assert_eq!(m.txn_mode, Some(TxnMode::ProcessingTime));
                assert_eq!(m.batch_version.as_deref(), Some("{{ run_id }}"));
                assert_eq!(m.app_id, "ldrs-merge-{{ name }}");
            }
            _ => panic!("expected Merge"),
        }
    }

    #[test]
    fn stray_merge_keys_on_overwrite_errors() {
        let err = parse_and_validate(
            r#"
dest: delta.overwrite
name: public.users
merge_keys: [id]
"#,
        );
        assert!(err.is_err(), "expected validate to reject stray merge_keys");
    }

    #[test]
    fn empty_merge_keys_errors() {
        let err = parse_and_validate(
            r#"
dest: delta.merge
name: public.users
merge_keys: []
"#,
        );
        assert!(err.is_err(), "expected validate to reject empty merge_keys");
    }

    #[test]
    fn source_watermark_without_watermark_column_errors() {
        let err = parse_and_validate(
            r#"
dest: delta.merge
name: public.users
merge_keys: [id]
txn_mode: source_watermark
"#,
        );
        assert!(
            err.is_err(),
            "expected validate to require watermark_column"
        );
    }

    #[test]
    fn unknown_txn_mode_errors_at_parse() {
        let err = parse_and_validate(
            r#"
dest: delta.merge
name: public.users
merge_keys: [id]
txn_mode: bogus
"#,
        );
        assert!(err.is_err(), "expected serde to reject unknown txn_mode");
    }
}
