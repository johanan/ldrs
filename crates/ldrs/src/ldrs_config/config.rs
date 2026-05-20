use crate::{
    delta::{self, DeltaCommon, DeltaDestination, DeltaMerge},
    file_source::FileSource,
    ldrs_config::field_validation::{extract_props, find_unknown_keys, UnknownKey},
    ldrs_snowflake::snowflake_source::{
        from_serde_yaml as from_sf_serde_yaml, SFQuery, SFSource, SFTable,
    },
    parquet::ParquetDestination,
    postgres::postgres_destination::{
        from_serde_yaml, PgCommon, PgDeleteInsert, PgDestination, PgMerge,
    },
};

use std::collections::HashSet;

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
    pub unknown_keys: Vec<UnknownKey>,
}

/// Block-level keys that the parser injects or expects but that are not declared
/// on any kind's struct (the `src` and `dest` tags are pulled out before parsing
/// the inner variant).
pub const STRUCTURAL_KEYS: &[&str] = &["src", "dest"];

/// Field names allowed by the parsed source variant.
pub fn source_known_fields(src: &LdrsSource) -> Vec<String> {
    match src {
        LdrsSource::File(_) => extract_props::<FileSource>(),
        LdrsSource::SF(SFSource::Query(_)) => extract_props::<SFQuery>(),
        LdrsSource::SF(SFSource::Table(_)) => extract_props::<SFTable>(),
    }
}

/// Field names allowed by the parsed destination variant.
pub fn destination_known_fields(dest: &LdrsDestination) -> Vec<String> {
    match dest {
        LdrsDestination::Pq(_) => extract_props::<ParquetDestination>(),
        LdrsDestination::Pg(PgDestination::DropReplace(_)) => extract_props::<PgCommon>(),
        LdrsDestination::Pg(PgDestination::TruncateInsert(_)) => extract_props::<PgCommon>(),
        LdrsDestination::Pg(PgDestination::Merge(_)) => extract_props::<PgMerge>(),
        LdrsDestination::Pg(PgDestination::DeleteInsert(_)) => extract_props::<PgDeleteInsert>(),
        LdrsDestination::Delta(DeltaDestination::Overwrite(_)) => extract_props::<DeltaCommon>(),
        LdrsDestination::Delta(DeltaDestination::Merge(_)) => extract_props::<DeltaMerge>(),
        LdrsDestination::Arrow(_) => extract_props::<ArrowDestination>(),
    }
}

/// Walks a raw block against the union of fields declared by the parsed src and
/// dest variants, plus the block-level structural keys. Returns findings; does
/// not emit. Caller decides when/how to surface them.
pub fn find_unknown_block_keys(
    value: &Value,
    src: &LdrsSource,
    dest: &LdrsDestination,
) -> Vec<UnknownKey> {
    let src_fields = source_known_fields(src);
    let dest_fields = destination_known_fields(dest);
    let allowed: HashSet<&str> = src_fields
        .iter()
        .map(String::as_str)
        .chain(dest_fields.iter().map(String::as_str))
        .chain(STRUCTURAL_KEYS.iter().copied())
        .collect();
    find_unknown_keys(value, &allowed)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_known_fields_sf_query() {
        let src = LdrsSource::SF(SFSource::Query(SFQuery {
            sql: String::new(),
            name: String::new(),
            param_keys: None,
        }));
        let mut got = source_known_fields(&src);
        got.sort();
        assert_eq!(got, vec!["name", "param_keys", "sql"]);
    }

    #[test]
    fn source_known_fields_file() {
        let src = LdrsSource::File(FileSource {
            name: String::new(),
            filename: None,
        });
        let mut got = source_known_fields(&src);
        got.sort();
        assert_eq!(got, vec!["filename", "name"]);
    }

    #[test]
    fn destination_known_fields_delta_merge_includes_flattened() {
        let dest = LdrsDestination::Delta(DeltaDestination::Merge(DeltaMerge {
            common: DeltaCommon {
                name: String::new(),
                columns: Vec::new(),
                max_rows: None,
                max_bytes: None,
            },
            merge_keys: Vec::new(),
            allow_null_keys: false,
            txn_mode: None,
            watermark_column: None,
            batch_version: None,
            app_id: String::new(),
        }));
        let mut got = destination_known_fields(&dest);
        got.sort();
        assert_eq!(
            got,
            vec![
                "allow_null_keys",
                "app_id",
                "batch_version",
                "columns",
                "max_bytes",
                "max_rows",
                "merge_keys",
                "name",
                "txn_mode",
                "watermark_column",
            ]
        );
    }
}
