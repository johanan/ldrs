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
    pub filename: String,
    pub columns: Vec<ColumnSpec>,
    pub bloom_filters: Vec<Vec<String>>,
}

impl TryFrom<&Value> for ParquetDestination {
    type Error = anyhow::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        let name = value
            .get("name")
            .and_then(|v| String::deserialize(v).ok())
            .ok_or_else(|| {
                anyhow::anyhow!("Missing name for kind pq (see `ldrs schema pq` for required fields)")
            })?;
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
        Ok(ParquetDestination {
            name,
            filename,
            columns,
            bloom_filters,
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
}
