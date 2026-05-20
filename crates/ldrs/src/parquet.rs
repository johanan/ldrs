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
            .and_then(|c| Vec::<ColumnSpec>::deserialize(c).ok())
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
