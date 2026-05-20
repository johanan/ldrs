use crate::{
    delta::{self, DeltaDestination},
    file_source::FileSource,
    ldrs_snowflake::snowflake_source::{from_serde_yaml as from_sf_serde_yaml, SFSource},
    parquet::ParquetDestination,
    postgres::postgres_destination::{from_serde_yaml, PgDestination},
};

use anyhow::Context;
use ldrs_arrow::ColumnSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct LdrsConfig {
    #[serde(default)]
    pub src: Option<String>,
    #[schemars(with = "Option<serde_json::Value>")]
    pub src_defaults: Option<Value>,
    #[serde(default)]
    pub dest: Option<String>,
    #[schemars(with = "Option<serde_json::Value>")]
    pub dest_defaults: Option<Value>,
    #[schemars(with = "Vec<serde_json::Value>")]
    pub tables: Vec<serde_yaml::Value>,
}

#[derive(Debug, PartialEq)]
pub enum LdrsSource {
    File(FileSource),
    SF(SFSource),
}

impl LdrsSource {
    pub fn name(&self) -> &str {
        match self {
            LdrsSource::File(fs) => &fs.name,
            LdrsSource::SF(sf) => sf.get_name(),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(
    description = "Arrow IPC streaming destination. Writes the stream to stdout. Pipe to any Arrow IPC reader."
)]
pub struct ArrowDestination {
    pub name: String,
    #[serde(default)]
    pub columns: Vec<ColumnSpec>,
}

#[derive(Debug, PartialEq)]
pub enum LdrsDestination {
    Pg(PgDestination),
    Pq(ParquetDestination),
    Delta(DeltaDestination),
    Arrow(ArrowDestination),
}

#[derive(Debug, PartialEq)]
pub struct LdrsParsedConfig {
    pub src: LdrsSource,
    pub dest: LdrsDestination,
}

pub fn merge_with_defaults(defaults: &Option<Value>, specific: Value) -> Value {
    match (defaults, specific) {
        (Some(Value::Mapping(def_map)), Value::Mapping(mut spec_map)) => {
            for (k, v) in def_map {
                spec_map.entry(k.clone()).or_insert(v.clone());
            }
            Value::Mapping(spec_map)
        }
        (_, specific) => specific,
    }
}

/// Parses the src block of the config. The value here should already have defaults merged in.
pub fn parse_src(value: Value, src_default: &Option<String>) -> Result<LdrsSource, anyhow::Error> {
    let src = value
        .get("src")
        .map(|v| String::deserialize(v))
        .transpose()
        .map_err(|_| anyhow::Error::msg("src is not correctly set in the table block"))?
        .or(src_default.clone())
        .ok_or_else(|| anyhow::Error::msg("src is not set"))?;

    let src_prefix = src.split('.').next().unwrap_or(&src);

    let mut src_value = value;
    if let Value::Mapping(ref mut src_map) = src_value {
        src_map.insert(Value::String("src".to_string()), Value::String(src.clone()));
    }
    match src_prefix {
        "file" => {
            let parsed: FileSource = serde_yaml::from_value(src_value)
                .with_context(|| format!("failed to parse file source: {}", src))?;
            Ok(LdrsSource::File(parsed))
        }
        "sf" => {
            let parsed = serde_yaml::from_value(src_value.clone())
                .with_context(|| format!("failed to parse snowflake source: {}", src))
                .or_else(|_| from_sf_serde_yaml(&src_value, Some(&src)))?;
            Ok(LdrsSource::SF(parsed))
        }
        _ => Err(anyhow::Error::msg(
            "unsupported src type (see `ldrs schema` for valid kinds)",
        )),
    }
}

/// Parses the dest block of the config. The value here should already have defaults merged in.
pub fn parse_dest(
    value: Value,
    dest_default: &Option<String>,
) -> Result<LdrsDestination, anyhow::Error> {
    let dest = value
        .get("dest")
        .map(|v| String::deserialize(v))
        .transpose()
        .map_err(|_| anyhow::Error::msg("dest is not correctly set in the table block"))?
        .or(dest_default.clone())
        .ok_or_else(|| anyhow::Error::msg("dest is not set"))?;

    let dest_prefix = dest.split('.').next().unwrap_or(&dest);
    let mut value = value;
    if let Value::Mapping(ref mut dest_map) = value {
        dest_map.insert(
            Value::String("dest".to_string()),
            Value::String(dest.clone()),
        );
    }
    match dest_prefix {
        "pg" => {
            let parsed = serde_yaml::from_value::<PgDestination>(value.clone())
                .with_context(|| format!("failed to parse pg destination: {}", dest))
                .or_else(|_| from_serde_yaml(&value, Some(&dest)))?;
            Ok(LdrsDestination::Pg(parsed))
        }
        "pq" => {
            let parsed = ParquetDestination::try_from(&value)?;
            Ok(LdrsDestination::Pq(parsed))
        }
        "delta" => {
            let parsed: DeltaDestination = serde_yaml::from_value(value.clone())
                .with_context(|| format!("failed to parse delta destination: {}", dest))?;
            delta::validate(&value, &parsed)?;
            Ok(LdrsDestination::Delta(parsed))
        }
        "arrow" => {
            let parsed = serde_yaml::from_value::<ArrowDestination>(value.clone())
                .with_context(|| format!("failed to parse arrow destination: {}", dest))?;
            Ok(LdrsDestination::Arrow(parsed))
        }
        _ => Err(anyhow::Error::msg(
            "unsupported dest type (see `ldrs schema` for valid kinds)",
        )),
    }
}
