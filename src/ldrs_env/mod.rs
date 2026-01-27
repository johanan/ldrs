use serde_json::Value;
use tracing::{debug, warn};

use crate::types::ColumnType;

pub fn get_all_ldrs_env_vars() -> Vec<(String, String)> {
    std::env::vars()
        .filter(|(key, _)| key.starts_with("LDRS_"))
        .collect()
}

const SIMPLE_TYPES: &[&str] = &[
    "_UUID",
    "_INT",
    "_BIGINT",
    "_SMALLINT",
    "_BOOL",
    "_TIMESTAMP",
    "_TIMESTAMPTZ",
    "_TEXT",
    "_VARCHAR",
    "_REAL",
    "_DOUBLE",
    "_DATE",
    "_JSONB",
    "_BYTEA",
];

/// Collects query parameters from environment variables.
/// This function should be used with `get_all_ldrs_env_vars()` to find all LDRS_PARAM_* variables.
/// The funciton will find all env vars that start with "LDRS_PARAM_" and return a vector of tuples containing the parameter name, value, and column type.
/// The parameter name is the key without the prefix "LDRS_PARAM_" so `LDRS_PARAM_P1` will be returned as `P1`.
/// In addition this function will also parse the column type from the suffix of the parameter name.
/// So `LDRS_PARAM_P1_INT` will be returned as `P1` with a column type of `ColumnType::Int`.
pub fn collect_params(params: &[(String, String)]) -> Vec<(String, String, Option<ColumnType>)> {
    let mut ldrs_params = params
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix("LDRS_PARAM_").map(|suffix| {
                for simple_type in SIMPLE_TYPES {
                    if let Some(key) = suffix.strip_suffix(simple_type) {
                        let simple_type_rest = &simple_type[1..];
                        // at least output if something failed
                        let column_type = ColumnType::try_from(simple_type_rest)
                            .map_err(|e| warn!("Failed to parse column type: {}", e))
                            .ok();
                        return (key.to_string(), value.to_string(), column_type);
                    }
                }
                (suffix.to_string(), value.to_string(), None)
            })
        })
        .collect::<Vec<_>>();
    // ensure that the params are sorted by key
    ldrs_params.sort_by(|a, b| a.0.cmp(&b.0));
    ldrs_params
}

/// Finds all the parameters for a given key tied to this execution.
/// This function should be used with `collect_params()` to find all LDRS_PARAM_* variables and clean them up.
/// For example, the key `public.users` will return all parameters that start with `LDRS_PARAM_PUBLIC_USERS_`.
/// This allows you to target specific parameters for a given query.
pub fn get_params_for_key(
    key: &str,
    params: &[(String, String, Option<ColumnType>)],
) -> Vec<(String, Option<ColumnType>)> {
    let prefix = format!("{}_", key);
    params
        .iter()
        .filter(|(key, _, _)| key.starts_with(&prefix))
        .map(|(_, value, param_type)| (value.to_string(), param_type.clone()))
        .collect::<Vec<_>>()
}

pub fn get_params_default(
    params: &[(String, String, Option<ColumnType>)],
) -> Vec<(String, Option<ColumnType>)> {
    params
        .iter()
        .filter(|(key, _, _)| !key.contains('_'))
        .map(|(_, value, param_type)| (value.to_string(), param_type.clone()))
        .collect::<Vec<_>>()
}

/// Finds all the parameters for a given key tied to this execution.
/// This function should be used with `collect_params()` to find all LDRS_PARAM_* variables and clean them up.
/// If there is a key and no parameters are found, it will return the default parameters. Which are params without an underscore.
/// Also default parameters are returned if no key is provided.
pub fn get_params_for_stmt_with_default(
    key: Option<&str>,
    params: &[(String, String, Option<ColumnType>)],
) -> Vec<(String, Option<ColumnType>)> {
    debug!("Getting params for key: {:?}", key);
    let key_params = key.map(|k| get_params_for_key(k, params));
    debug!("Key params: {:?}", key_params);
    match key_params {
        Some(p) if p.is_empty() => get_params_default(params),
        Some(p) => p,
        None => get_params_default(params),
    }
}

pub fn get_env_values_by_keys<'a>(
    keys: &'a [String],
    env_vars: &'a [(String, String, Option<ColumnType>)],
) -> Vec<(String, Option<ColumnType>)> {
    env_vars
        .iter()
        .filter(|(k, _, _)| keys.iter().any(|key| k.eq_ignore_ascii_case(key)))
        .map(|(_, v, t)| (v.clone(), t.clone()))
        .collect::<Vec<_>>()
}

#[derive(Debug)]
pub struct LdrsExecutionContext<'a> {
    pub context: Value,
    pub handlebars: &'a handlebars::Handlebars<'a>,
}

impl<'a> LdrsExecutionContext<'a> {
    pub fn try_new(
        name: &str,
        handlebars: &'a handlebars::Handlebars<'a>,
    ) -> Result<Self, anyhow::Error> {
        let fqtn_tup = name.split_once('.');
        let (schema, table) = match fqtn_tup {
            Some((schema, table)) => Ok((schema, table)),
            None => Err(anyhow::Error::msg("Invalid table name")),
        }?;

        // create random load_table name
        let load_table_name = format!(
            "{}_{}",
            table,
            uuid::Uuid::new_v4().to_string().replace('-', "")
        );
        let load_table = format!("{}.{}", schema, load_table_name);

        let context = serde_json::json!({
            "name": name,
            "schema": schema,
            "table": table,
            "load_table": load_table,
            "load_table_name": load_table_name
        });
        Ok(Self {
            context,
            handlebars,
        })
    }
}
