use crate::{
    ldrs_postgres::postgres_destination::{from_serde_yaml, PgDestination},
    ldrs_snowflake::snowflake_source::{from_serde_yaml as from_sf_serde_yaml, SFSource},
    ldrs_storage::FileSource,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LdrsConfig {
    #[serde(default)]
    pub src: Option<String>,
    pub src_defaults: Option<Value>,
    #[serde(default)]
    pub dest: Option<String>,
    pub dest_defaults: Option<Value>,
    pub tables: Vec<serde_yaml::Value>,
}

#[derive(Debug)]
pub enum LdrsSource {
    File(FileSource),
    SF(SFSource),
}

#[derive(Debug)]
pub enum LdrsDestination {
    Pg(PgDestination),
}

#[derive(Debug)]
pub struct LdrsParsedConfig {
    pub src: LdrsSource,
    pub src_prefix: String,
    pub dest: LdrsDestination,
    pub dest_prefix: String,
}

pub fn get_parsed_config(
    src_default: &Option<String>,
    dest_default: &Option<String>,
    value: Value,
) -> Result<LdrsParsedConfig, anyhow::Error> {
    // first see if we have a src in the yaml value, that overrides
    let src = value
        .get("src")
        .map(|v| String::deserialize(v))
        .transpose()
        .map_err(|_| anyhow::Error::msg("src is not correctly set in the table block"))?
        .or(src_default.clone())
        .ok_or_else(|| anyhow::Error::msg("src is not set"))?;

    let dest = value
        .get("dest")
        .map(|v| String::deserialize(v))
        .transpose()
        .map_err(|_| anyhow::Error::msg("dest is not correctly set in the table block"))?
        .or(dest_default.clone())
        .ok_or_else(|| anyhow::Error::msg("dest is not set"))?;

    // get the prefix or the whole value
    // and then insert it into the value and then parse it
    let src_prefix = src.split('.').next().unwrap_or(&src);
    let mut src_value = value.clone();
    if let Value::Mapping(ref mut src_map) = src_value {
        src_map.insert(Value::String("src".to_string()), Value::String(src.clone()));
    }
    let ldrs_src = match src_prefix {
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
        _ => Err(anyhow::Error::msg("unsupported src type")),
    }?;

    let dest_prefix = dest.split('.').next().unwrap_or(&dest);
    let mut dest_value = value.clone();
    if let Value::Mapping(ref mut dest_map) = dest_value {
        dest_map.insert(
            Value::String("dest".to_string()),
            Value::String(dest.clone()),
        );
    }
    let ldrs_dest = match dest_prefix {
        "pg" => {
            let parsed = serde_yaml::from_value::<PgDestination>(dest_value.clone())
                .with_context(|| format!("failed to parse pg destination: {}", dest))
                .or_else(|_| from_serde_yaml(&dest_value, Some(&dest)))?;
            Ok(LdrsDestination::Pg(parsed))
        }
        _ => Err(anyhow::Error::msg("unsupported dest type")),
    }?;

    Ok(LdrsParsedConfig {
        src: ldrs_src,
        src_prefix: src_prefix.to_string(),
        dest: ldrs_dest,
        dest_prefix: dest_prefix.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_parse() {
        let config_yaml = r#"
src: file
dest: pg

tables:
    - name: public.users
      merge_keys: [id]
    - name: public.users
      dest: pg.drop_replace
      filename: public.users
      post_sql:
        - create index if not exists on {{ name }} (id);
    - name: public.strings
      dest: pg.drop_replace
      post_sql:
        - create index if not exists on {{ name }} (ok);
"#;
        let config: LdrsConfig = serde_yaml::from_str(config_yaml).unwrap();
        // parse each table
        for table in config.tables {
            let parsed = get_parsed_config(&config.src, &config.dest, table);
            println!("{:?}", parsed);
        }
    }
}
