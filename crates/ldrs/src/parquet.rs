use anyhow::Context;
use ldrs_arrow::ColumnSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(
    description = "Parquet destination. Writes a Parquet file to LDRS_DEST + filename via object_store."
)]
pub struct ParquetDestination {
    pub name: String,
    #[serde(default)]
    pub target: Option<String>,
    pub filename: String,
    #[schemars(schema_with = "crate::cli_schema::columns_schema")]
    pub columns: Vec<ColumnSpec>,
    #[serde(default)]
    pub bloom_filters: Vec<Vec<String>>,
    #[serde(default, alias = "pq.max_rows")]
    pub max_rows: Option<usize>,
    #[serde(default, alias = "pq.max_bytes")]
    pub max_bytes: Option<usize>,
}

impl TryFrom<&Value> for ParquetDestination {
    type Error = anyhow::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let name = value
            .get("name")
            .and_then(|v| String::deserialize(v).ok())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Missing name for kind pq (see `ldrs schema pq` for required fields)"
                )
            })?;
        let target = value.get("target").and_then(|v| String::deserialize(v).ok());
        let filename = value
            .get("pq.filename")
            .or(value.get("filename"))
            .and_then(|f| String::deserialize(f).ok())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Missing filename for kind pq (see `ldrs schema pq` for required fields)"
                )
            })?;
        let columns = value
            .get("columns")
            .map(|c| Vec::<ColumnSpec>::deserialize(c))
            .transpose()
            .context("failed to parse columns for kind pq")?
            .unwrap_or_default();
        let bloom_filters = value
            .get("bloom_filters")
            .and_then(|b| Vec::<Vec<String>>::deserialize(b).ok())
            .unwrap_or_default();
        let max_rows = value
            .get("pq.max_rows")
            .or(value.get("max_rows"))
            .map(usize::deserialize)
            .transpose()
            .context("failed to parse max_rows for kind pq")?;
        let max_bytes = value
            .get("pq.max_bytes")
            .or(value.get("max_bytes"))
            .map(usize::deserialize)
            .transpose()
            .context("failed to parse max_bytes for kind pq")?;
        Ok(ParquetDestination {
            name,
            target,
            filename,
            columns,
            bloom_filters,
            max_rows,
            max_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_from_propagates_unknown_column_field_error() {
        let yaml = r#"
name: foo
filename: foo.parquet
columns:
  - type: varchar
    name: id
    lenght: 32
"#;
        let value: Value = serde_yaml::from_str(yaml).unwrap();
        let err = ParquetDestination::try_from(&value).unwrap_err();
        let msg = format!("{:#}", err);
        assert!(
            msg.contains("lenght"),
            "expected error to mention the bad field, got: {msg}"
        );
        assert!(
            msg.contains("columns"),
            "expected error to mention the columns context, got: {msg}"
        );
    }

    #[test]
    fn try_from_parses_rotation_limits() {
        // exercises both the `pq.`-prefixed alias and the bare key
        let yaml = r#"
name: foo
pq.filename: "out/{{ name }}_{{ pad index 5 }}.parquet"
pq.max_rows: 1000
max_bytes: 2000
"#;
        let value: Value = serde_yaml::from_str(yaml).unwrap();
        let dest = ParquetDestination::try_from(&value).unwrap();
        assert_eq!(dest.max_rows, Some(1000));
        assert_eq!(dest.max_bytes, Some(2000));
    }

    #[test]
    fn try_from_rotation_limits_default_none() {
        let yaml = r#"
name: foo
filename: out.parquet
"#;
        let value: Value = serde_yaml::from_str(yaml).unwrap();
        let dest = ParquetDestination::try_from(&value).unwrap();
        assert_eq!(dest.max_rows, None);
        assert_eq!(dest.max_bytes, None);
    }
}
