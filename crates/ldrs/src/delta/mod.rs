use ldrs_arrow::ColumnSpec;

use serde::{Deserialize, Serialize};
use serde_yaml::Value;

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct DeltaDestination {
    pub name: String,
    pub columns: Vec<ColumnSpec>,
    pub max_rows: Option<usize>,
    pub max_bytes: Option<usize>,
    pub mode: DeltaMode,
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub enum DeltaMode {
    Overwrite,
    Merge {
        merge_keys: Vec<String>,
        allow_null_keys: bool,
        txn: Option<MergeTxnConfig>,
    },
}

/// Raw (unrendered) txn config from YAML. String templates are rendered at dispatch
/// time against the execution context before constructing the runtime `TxnConfig`.
#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
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

impl TryFrom<&Value> for DeltaDestination {
    type Error = anyhow::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let name = value
            .get("name")
            .and_then(|v| String::deserialize(v).ok())
            .ok_or(anyhow::anyhow!("Missing name"))?;
        let columns = get_ns(value, "delta", "columns")
            .and_then(|c| Vec::<ColumnSpec>::deserialize(c).ok())
            .unwrap_or_default();
        let max_rows = get_ns(value, "delta", "max_rows").and_then(|v| usize::deserialize(v).ok());
        let max_bytes =
            get_ns(value, "delta", "max_bytes").and_then(|v| usize::deserialize(v).ok());

        // Merge mode detected by presence of merge_keys (namespaced or bare)
        let mode = if let Some(keys_value) = get_ns(value, "delta", "merge_keys") {
            let merge_keys = Vec::<String>::deserialize(keys_value)
                .map_err(|e| anyhow::anyhow!("merge_keys must be a list of strings: {}", e))?;
            if merge_keys.is_empty() {
                anyhow::bail!("merge_keys cannot be empty");
            }

            let allow_null_keys = get_ns(value, "delta", "allow_null_keys")
                .and_then(|v| bool::deserialize(v).ok())
                .unwrap_or(false);

            let app_id = get_ns(value, "delta", "app_id")
                .and_then(|v| String::deserialize(v).ok())
                .unwrap_or_else(|| "ldrs-merge-{{ name }}".to_string());

            let txn_mode =
                get_ns(value, "delta", "txn_mode").and_then(|v| String::deserialize(v).ok());

            let txn = match txn_mode.as_deref() {
                Some("source_watermark") => {
                    let watermark_column = get_ns(value, "delta", "watermark_column")
                        .and_then(|v| String::deserialize(v).ok())
                        .ok_or(anyhow::anyhow!(
                            "source_watermark requires watermark_column"
                        ))?;
                    Some(MergeTxnConfig::SourceWatermark {
                        app_id,
                        watermark_column,
                    })
                }
                Some("processing_time") => {
                    let batch_version = get_ns(value, "delta", "batch_version")
                        .and_then(|v| String::deserialize(v).ok());
                    Some(MergeTxnConfig::ProcessingTime {
                        app_id,
                        batch_version,
                    })
                }
                Some(other) => anyhow::bail!("unknown txn_mode: {}", other),
                None => None,
            };

            DeltaMode::Merge {
                merge_keys,
                allow_null_keys,
                txn,
            }
        } else {
            DeltaMode::Overwrite
        };

        Ok(DeltaDestination {
            name,
            columns,
            max_rows,
            max_bytes,
            mode,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> DeltaDestination {
        let value: Value = serde_yaml::from_str(yaml).unwrap();
        DeltaDestination::try_from(&value).unwrap()
    }

    #[test]
    fn overwrite_default_no_merge_keys() {
        let dest = parse("name: public.users");
        assert_eq!(dest.name, "public.users");
        assert_eq!(dest.mode, DeltaMode::Overwrite);
    }

    #[test]
    fn merge_mode_from_namespaced_keys() {
        let dest = parse(
            r#"
name: public.users
delta.merge_keys: [id]
"#,
        );
        match dest.mode {
            DeltaMode::Merge {
                merge_keys,
                allow_null_keys,
                txn,
            } => {
                assert_eq!(merge_keys, vec!["id".to_string()]);
                assert!(!allow_null_keys);
                assert!(txn.is_none());
            }
            _ => panic!("expected Merge mode"),
        }
    }

    #[test]
    fn merge_mode_from_bare_keys() {
        let dest = parse(
            r#"
name: public.users
merge_keys: [id, name]
"#,
        );
        match dest.mode {
            DeltaMode::Merge { merge_keys, .. } => {
                assert_eq!(merge_keys, vec!["id".to_string(), "name".to_string()]);
            }
            _ => panic!("expected Merge mode"),
        }
    }

    #[test]
    fn merge_source_watermark_parses() {
        let dest = parse(
            r#"
name: public.users
delta.merge_keys: [id]
delta.txn_mode: source_watermark
delta.watermark_column: updated_at
delta.app_id: "my-app"
"#,
        );
        match dest.mode {
            DeltaMode::Merge {
                txn:
                    Some(MergeTxnConfig::SourceWatermark {
                        app_id,
                        watermark_column,
                    }),
                ..
            } => {
                assert_eq!(app_id, "my-app");
                assert_eq!(watermark_column, "updated_at");
            }
            _ => panic!("expected SourceWatermark txn"),
        }
    }

    #[test]
    fn merge_processing_time_parses() {
        let dest = parse(
            r#"
name: public.users
delta.merge_keys: [id]
delta.txn_mode: processing_time
delta.batch_version: "{{ run_id }}"
"#,
        );
        match dest.mode {
            DeltaMode::Merge {
                txn:
                    Some(MergeTxnConfig::ProcessingTime {
                        app_id,
                        batch_version,
                    }),
                ..
            } => {
                // app_id defaulted to ldrs-merge-{{ name }} template (rendered later)
                assert_eq!(app_id, "ldrs-merge-{{ name }}");
                assert_eq!(batch_version.as_deref(), Some("{{ run_id }}"));
            }
            _ => panic!("expected ProcessingTime txn"),
        }
    }

    #[test]
    fn merge_allow_null_keys_parses() {
        let dest = parse(
            r#"
name: public.users
delta.merge_keys: [id]
delta.allow_null_keys: true
"#,
        );
        match dest.mode {
            DeltaMode::Merge {
                allow_null_keys, ..
            } => assert!(allow_null_keys),
            _ => panic!("expected Merge mode"),
        }
    }

    #[test]
    fn unknown_txn_mode_errors() {
        let value: Value = serde_yaml::from_str(
            r#"
name: public.users
delta.merge_keys: [id]
delta.txn_mode: bogus
"#,
        )
        .unwrap();
        assert!(DeltaDestination::try_from(&value).is_err());
    }

    #[test]
    fn empty_merge_keys_errors() {
        let value: Value = serde_yaml::from_str(
            r#"
name: public.users
delta.merge_keys: []
"#,
        )
        .unwrap();
        assert!(DeltaDestination::try_from(&value).is_err());
    }
}
