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
    // dest / dest_defaults are the deprecated flat (v1) single-destination form; still parsed,
    // but `#[schemars(skip)]` keeps them out of `ldrs schema yaml` so discovery points at v2.
    #[serde(default)]
    #[schemars(skip)]
    pub dest: Option<String>,
    #[schemars(skip)]
    pub dest_defaults: Option<Value>,
    /// Config format version. `2` selects the nested `destinations:` form (one source → many
    /// destinations). Authoritative when set; otherwise the form is inferred from whether a
    /// `destinations:` list is present. Version 1 (flat, single `dest:`) is deprecated.
    #[serde(default)]
    pub version: Option<u32>,
    /// Fan-out destination list: each entry is a destination block with its own `dest:` kind
    /// tag (run `ldrs schema <kind>` for each block's fields). The shared default for any table
    /// that doesn't declare its own `destinations:`. Whole-array overwrite, never field- or
    /// array-merged.
    #[serde(default)]
    #[schemars(with = "Option<Vec<serde_json::Value>>")]
    pub destinations: Option<Vec<Value>>,
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
    #[schemars(schema_with = "crate::cli_schema::columns_schema")]
    pub columns: Vec<ColumnSpec>,
}

#[derive(Debug, PartialEq)]
pub enum LdrsDestination {
    Pg(PgDestination),
    Pq(ParquetDestination),
    Delta(DeltaDestination),
    Arrow(ArrowDestination),
}

impl LdrsDestination {
    /// The destination's output column specs.
    pub fn columns(&self) -> &[ColumnSpec] {
        match self {
            LdrsDestination::Pg(PgDestination::DropReplace(c)) => &c.columns,
            LdrsDestination::Pg(PgDestination::TruncateInsert(c)) => &c.columns,
            LdrsDestination::Pg(PgDestination::Merge(m)) => &m.columns,
            LdrsDestination::Pg(PgDestination::DeleteInsert(d)) => &d.columns,
            LdrsDestination::Pq(p) => &p.columns,
            LdrsDestination::Delta(DeltaDestination::Overwrite(c)) => &c.columns,
            LdrsDestination::Delta(DeltaDestination::Merge(m)) => &m.common.columns,
            LdrsDestination::Arrow(a) => &a.columns,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct LdrsParsedConfig {
    pub src: LdrsSource,
    pub dests: Vec<LdrsDestination>,
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

/// Parse one table block into its source and destination(s): the flat (v1) form or the
/// nested (v2) `destinations:` form (dispatched by [`is_nested`]).
pub fn parse_table(
    table: Value,
    config: &LdrsConfig,
    src_default: &Option<String>,
    dest_default: &Option<String>,
) -> Result<LdrsParsedConfig, anyhow::Error> {
    if is_nested(&table, config)? {
        parse_table_nested(table, config, src_default)
    } else {
        parse_table_flat(table, config, src_default, dest_default)
    }
}

/// Decide flat vs. nested. `version:` is authoritative and errors on a content mismatch;
/// absent, the presence of a `destinations:` list (table-level or top-level) decides.
fn is_nested(table: &Value, config: &LdrsConfig) -> Result<bool, anyhow::Error> {
    let has_destinations = table.get("destinations").is_some() || config.destinations.is_some();
    match (config.version, has_destinations) {
        (Some(1), true) => {
            anyhow::bail!("version: 1 is the flat form, but a `destinations:` list is present")
        }
        (Some(2), false) => {
            anyhow::bail!("version: 2 is the nested form, but no `destinations:` list was found")
        }
        (Some(1), false) => Ok(false),
        (Some(2), true) => Ok(true),
        (Some(v), _) => anyhow::bail!("unsupported config version: {} (use 1 or 2)", v),
        (None, has) => Ok(has),
    }
}

/// Flat (v1) form: the table block is both the source and the single destination.
fn parse_table_flat(
    table: Value,
    config: &LdrsConfig,
    src_default: &Option<String>,
    dest_default: &Option<String>,
) -> Result<LdrsParsedConfig, anyhow::Error> {
    let raw_block = table.clone();
    let src = parse_src(
        merge_with_defaults(&config.src_defaults, table.clone()),
        src_default,
    )?;
    let dest = parse_dest(
        merge_with_defaults(&config.dest_defaults, table),
        dest_default,
    )?;
    let unknown_keys = find_unknown_block_keys(&raw_block, &src, &dest);
    Ok(LdrsParsedConfig {
        src,
        dests: vec![dest],
        unknown_keys,
    })
}

/// Nested (v2) form: the table block is the source; each `destinations:` entry is its own
/// destination, inheriting the table's `name` and `columns`.
fn parse_table_nested(
    table: Value,
    config: &LdrsConfig,
    src_default: &Option<String>,
) -> Result<LdrsParsedConfig, anyhow::Error> {
    let src = parse_src(
        merge_with_defaults(&config.src_defaults, table.clone()),
        src_default,
    )?;

    // `name` (fixed) and `columns` (unless the block overrides) inherited into each block.
    let mut inherited = serde_yaml::Mapping::new();
    inherited.insert(
        Value::String("name".to_string()),
        Value::String(src.name().to_string()),
    );
    if let Some(cols) = table.get("columns") {
        inherited.insert(Value::String("columns".to_string()), cols.clone());
    }
    let inherited = Some(Value::Mapping(inherited));

    let dest_blocks = resolve_dest_blocks(&table, config)?;

    let (dests, per_block_unknowns): (Vec<LdrsDestination>, Vec<Vec<UnknownKey>>) = dest_blocks
        .into_iter()
        .map(|raw_item| {
            if raw_item.get("name").is_some() {
                anyhow::bail!("a destination must not set `name`; it is inherited from the table");
            }
            let block = merge_with_defaults(&inherited, raw_item.clone());
            // `&None`: each entry must carry its own `dest:` tag.
            let dest = parse_dest(block, &None)?;
            // unknown keys against this dest kind's fields + its own `dest:` tag (not `src`).
            let dest_fields = destination_known_fields(&dest);
            let allowed: HashSet<&str> = dest_fields
                .iter()
                .map(String::as_str)
                .chain(["dest"])
                .collect();
            Ok::<_, anyhow::Error>((dest, find_unknown_keys(&raw_item, &allowed)))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .unzip();

    // source-block unknown keys against src fields + `src`/`destinations`/`columns` (not `dest`).
    let src_fields = source_known_fields(&src);
    let src_allowed: HashSet<&str> = src_fields
        .iter()
        .map(String::as_str)
        .chain(["src", "destinations", "columns"])
        .collect();

    let unknown_keys = per_block_unknowns
        .into_iter()
        .flatten()
        .chain(find_unknown_keys(&table, &src_allowed))
        .collect();

    Ok(LdrsParsedConfig {
        src,
        dests,
        unknown_keys,
    })
}

/// A nested table's `destinations:`, or the top-level default if the table has none.
fn resolve_dest_blocks(table: &Value, config: &LdrsConfig) -> Result<Vec<Value>, anyhow::Error> {
    match table.get("destinations") {
        Some(Value::Sequence(seq)) => Ok(seq.clone()),
        Some(_) => anyhow::bail!("`destinations` must be a list"),
        None => config
            .destinations
            .clone()
            .ok_or_else(|| anyhow::Error::msg("nested table has no destinations")),
    }
}

/// Run-level validation: at most one Arrow (stdout) destination across the whole run
/// concatenated IPC streams on one stdout would corrupt the output.
pub fn validate_configs(tasks: &[LdrsParsedConfig]) -> Result<(), anyhow::Error> {
    let arrow_count = tasks
        .iter()
        .flat_map(|t| t.dests.iter())
        .filter(|d| matches!(d, LdrsDestination::Arrow(_)))
        .count();
    if arrow_count > 1 {
        anyhow::bail!(
            "at most one Arrow (stdout) destination is allowed per run; multiple IPC streams on stdout would corrupt the output"
        );
    }
    Ok(())
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

    #[test]
    fn nested_inherits_name_and_columns_into_each_dest() {
        let yaml = r#"
src: file
destinations:
  - dest: delta.overwrite
  - dest: pq
    filename: "out/{{ name }}.parquet"
tables:
  - name: users
    columns:
      - { type: uuid, name: id }
      - { type: text, name: email }
"#;
        let configs = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap();
        assert_eq!(configs.len(), 1);
        let dests = &configs[0].dests;
        assert_eq!(dests.len(), 2);
        match &dests[0] {
            LdrsDestination::Delta(DeltaDestination::Overwrite(c)) => {
                assert_eq!(c.name, "users");
                assert_eq!(c.columns.len(), 2);
            }
            other => panic!("expected delta overwrite, got {:?}", other),
        }
        match &dests[1] {
            LdrsDestination::Pq(p) => {
                assert_eq!(p.name, "users");
                assert_eq!(p.columns.len(), 2);
            }
            other => panic!("expected pq, got {:?}", other),
        }
    }

    #[test]
    fn nested_single_destination_yields_one_dest() {
        let yaml = r#"
src: file
destinations:
  - dest: pg.drop_replace
tables:
  - name: public.users
    columns:
      - { type: uuid, name: id }
"#;
        let configs = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].dests.len(), 1);
        match &configs[0].dests[0] {
            LdrsDestination::Pg(pg) => assert_eq!(pg.get_name(), "public.users"),
            other => panic!("expected pg, got {:?}", other),
        }
    }

    #[test]
    fn nested_dest_setting_name_is_an_error() {
        let yaml = r#"
src: file
destinations:
  - dest: pg.drop_replace
    name: warehouse.users
tables:
  - name: users
    columns:
      - { type: uuid, name: id }
"#;
        let err = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap_err();
        assert!(
            err.to_string().contains("inherited"),
            "expected a name-is-inherited error, got: {}",
            err
        );
    }

    #[test]
    fn nested_dest_columns_override_is_whole_replace() {
        let yaml = r#"
src: file
destinations:
  - dest: delta.overwrite
    columns:
      - { type: uuid, name: id }
  - dest: pq
    filename: "out/{{ name }}.parquet"
tables:
  - name: users
    columns:
      - { type: uuid, name: id }
      - { type: text, name: email }
      - { type: integer, name: age }
"#;
        let configs = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap();
        let dests = &configs[0].dests;
        // delta declared its own 1-column set: whole replace, NOT merged with the table's 3
        match &dests[0] {
            LdrsDestination::Delta(DeltaDestination::Overwrite(c)) => {
                assert_eq!(c.columns.len(), 1)
            }
            other => panic!("expected delta overwrite, got {:?}", other),
        }
        // pq declared none, so it inherits the table's full 3-column set
        match &dests[1] {
            LdrsDestination::Pq(p) => assert_eq!(p.columns.len(), 3),
            other => panic!("expected pq, got {:?}", other),
        }
    }

    #[test]
    fn nested_dest_columns_empty_opts_out_of_inheritance() {
        let yaml = r#"
src: file
destinations:
  - dest: pq
    filename: "raw/{{ name }}.parquet"
    columns: []
tables:
  - name: users
    columns:
      - { type: uuid, name: id }
      - { type: text, name: email }
"#;
        let configs = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap();
        // explicit empty list is "present", so inheritance does not fill it
        match &configs[0].dests[0] {
            LdrsDestination::Pq(p) => assert!(p.columns.is_empty()),
            other => panic!("expected pq, got {:?}", other),
        }
    }

    #[test]
    fn nested_per_block_validation_flags_wrong_kind_key() {
        let yaml = r#"
src: file
destinations:
  - dest: pq
    filename: "out/{{ name }}.parquet"
    merge_keys: [id]
tables:
  - name: users
    columns:
      - { type: uuid, name: id }
"#;
        let configs = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap();
        // `merge_keys` is not a pq field; the per-block check validates against pq's kind only
        let flagged: Vec<&str> = configs[0]
            .unknown_keys
            .iter()
            .map(|u| u.key.as_str())
            .collect();
        assert!(
            flagged.contains(&"merge_keys"),
            "expected merge_keys flagged, got: {:?}",
            flagged
        );
    }

    #[test]
    fn nested_source_block_allows_columns_and_destinations() {
        let yaml = r#"
src: file
tables:
  - name: users
    columns:
      - { type: uuid, name: id }
    destinations:
      - dest: pq
        filename: "out/{{ name }}.parquet"
"#;
        let configs = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap();
        // table-level `columns` and `destinations` are not source fields, but both are allowed
        assert!(
            configs[0].unknown_keys.is_empty(),
            "unexpected unknown keys: {:?}",
            configs[0].unknown_keys
        );
    }

    #[test]
    fn nested_dest_without_dest_tag_is_an_error() {
        let yaml = r#"
src: file
destinations:
  - filename: "out/{{ name }}.parquet"
tables:
  - name: users
    columns:
      - { type: uuid, name: id }
"#;
        let err = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap_err();
        assert!(err.to_string().contains("dest"), "got: {}", err);
    }

    #[test]
    fn version_2_without_destinations_is_an_error() {
        let yaml = r#"
version: 2
src: file
dest: pq
tables:
  - name: users
    filename: "out.parquet"
"#;
        let err = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap_err();
        assert!(err.to_string().contains("version: 2"), "got: {}", err);
    }

    #[test]
    fn version_1_with_destinations_is_an_error() {
        let yaml = r#"
version: 1
src: file
destinations:
  - dest: pq
    filename: "out/{{ name }}.parquet"
tables:
  - name: users
"#;
        let err = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap_err();
        assert!(err.to_string().contains("version: 1"), "got: {}", err);
    }

    #[test]
    fn version_2_with_destinations_parses_nested() {
        let yaml = r#"
version: 2
src: file
destinations:
  - dest: pq
    filename: "out/{{ name }}.parquet"
tables:
  - name: users
    columns:
      - { type: uuid, name: id }
"#;
        let configs = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap();
        assert_eq!(configs[0].dests.len(), 1);
        assert!(matches!(configs[0].dests[0], LdrsDestination::Pq(_)));
    }

    #[test]
    fn unsupported_version_is_an_error() {
        let yaml = r#"
version: 3
src: file
dest: pq
tables:
  - name: users
    filename: "out.parquet"
"#;
        let err = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap_err();
        assert!(
            err.to_string().contains("unsupported config version"),
            "got: {}",
            err
        );
    }

    #[test]
    fn nested_table_destinations_replace_top_level() {
        let yaml = r#"
src: file
destinations:
  - dest: delta.overwrite
  - dest: pg.drop_replace
tables:
  - name: users
    columns:
      - { type: uuid, name: id }
  - name: audit_log
    columns:
      - { type: bigint, name: event_id }
    destinations:
      - dest: pq
        filename: "audit/{{ name }}.parquet"
"#;
        let configs = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap();
        assert_eq!(configs.len(), 2);
        // `users` inherits the top-level pair
        assert_eq!(configs[0].dests.len(), 2);
        // `audit_log` replaces it entirely with its own single pq dest (no merge with top-level)
        assert_eq!(configs[1].dests.len(), 1);
        assert!(matches!(configs[1].dests[0], LdrsDestination::Pq(_)));
    }

    #[test]
    fn nested_dest_block_does_not_allow_src_key() {
        let yaml = r#"
src: file
destinations:
  - dest: pq
    filename: "out/{{ name }}.parquet"
    src: file
tables:
  - name: users
    columns:
      - { type: uuid, name: id }
"#;
        let configs = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap();
        // `src` is meaningless on a destination block
        let flagged: Vec<&str> = configs[0]
            .unknown_keys
            .iter()
            .map(|u| u.key.as_str())
            .collect();
        assert!(
            flagged.contains(&"src"),
            "expected src flagged, got: {:?}",
            flagged
        );
    }

    #[test]
    fn validate_configs_rejects_arrow_across_tasks() {
        let yaml = r#"
src: file
dest: arrow
tables:
  - name: users
  - name: orders
"#;
        // two flat tasks, each an arrow dest → two stdout streams across the run
        let configs = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap();
        assert!(validate_configs(&configs).is_err());
    }

    #[test]
    fn validate_configs_rejects_two_arrow_in_one_task() {
        let yaml = r#"
src: file
destinations:
  - dest: arrow
  - dest: arrow
tables:
  - name: users
"#;
        let configs = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap();
        assert!(validate_configs(&configs).is_err());
    }

    #[test]
    fn validate_configs_allows_single_arrow() {
        let yaml = r#"
src: file
destinations:
  - dest: arrow
  - dest: pq
    filename: "out/{{ name }}.parquet"
tables:
  - name: users
"#;
        let configs = crate::ldrs_config::parse_yaml_config(yaml, &[]).unwrap();
        assert!(validate_configs(&configs).is_ok());
    }
}
