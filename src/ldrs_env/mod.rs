use handlebars::handlebars_helper;
use serde_json::{json, Value};
use tracing::{debug, warn};

use crate::types::ColumnType;

pub fn get_all_ldrs_env_vars() -> Vec<(String, String)> {
    std::env::vars()
        .filter(|(key, _)| key.starts_with("LDRS_"))
        .collect()
}

/// Collects environment variables by prefix.
/// This function should be used with `get_all_ldrs_env_vars()` to find all LDRS_* variables.
/// This will return a subset of the environment variables that start with the given prefix
/// with the prefix removed.
///
/// ```rust
/// use ldrs::ldrs_env::collect_vars_by_prefix;
/// use ldrs::ldrs_env::get_all_ldrs_env_vars;
///
/// let env_vars = get_all_ldrs_env_vars();
/// let vars = collect_vars_by_prefix(&env_vars, "MY_PREFIX");
/// ```
pub fn collect_vars_by_prefix<'a>(
    env_vars: &'a [(String, String)],
    prefix: &str,
) -> Vec<(String, String)> {
    let full_prefix = format!("LDRS_{}_", prefix);
    env_vars
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix(&full_prefix)
                .map(|k| (k.to_string(), value.to_string()))
        })
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

handlebars_helper!(now_timestamp: | | chrono::prelude::Utc::now().timestamp() );

pub fn setup_handlebars(handle_bars: &mut handlebars::Handlebars) -> () {
    handle_bars.register_helper("now_timestamp", Box::new(now_timestamp));
    handle_bars.set_strict_mode(true);
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
        template_vars: &[(String, String)],
    ) -> Result<Self, anyhow::Error> {
        let (schema, table) = match name.split_once('.') {
            Some((schema, table)) => (Some(schema), table),
            None => (None, name),
        };

        let shouty_name = name.to_uppercase().replace('.', "_");
        let name_prefix = format!("{}_", shouty_name);

        let mut map = serde_json::Map::new();

        // named is LDRS_TEMPL_SHOUTY_NAME_
        // default is LDRS_TEMPL_
        let (named, default): (Vec<_>, Vec<_>) = template_vars
            .iter()
            .partition(|(k, _)| k.starts_with(&name_prefix));
        for (key, value) in &default {
            map.insert(key.to_lowercase(), Value::String(value.clone()));
        }

        for (key, value) in &named {
            let scoped_key = key.strip_prefix(&name_prefix).unwrap().to_lowercase();
            map.insert(scoped_key, Value::String(value.clone()));
        }
        // create random load_table name
        let load_table_name = format!(
            "{}_{}",
            table,
            uuid::Uuid::new_v4().to_string().replace('-', "")
        );
        let now = chrono::prelude::Utc::now();
        map.insert("name".into(), name.into());
        if let Some(s) = schema {
            let load_table = format!("{}.{}", s, load_table_name);
            map.insert("schema".into(), s.into());
            map.insert("load_table".into(), load_table.into());
        }
        map.insert("table".into(), table.into());
        map.insert("load_table_name".into(), load_table_name.into());
        map.insert(
            "rfc3339".into(),
            json!(now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)),
        );
        map.insert("YYYY".into(), json!(now.format("%Y").to_string()));
        map.insert("MM".into(), json!(now.format("%m").to_string()));
        map.insert("DD".into(), json!(now.format("%d").to_string()));
        map.insert("HH".into(), json!(now.format("%H").to_string()));
        map.insert("mm".into(), json!(now.format("%M").to_string()));
        map.insert("ss".into(), json!(now.format("%S").to_string()));
        map.insert("ms".into(), json!(now.format("%3f").to_string()));
        debug!("Handlebars Context: {:?}", map);

        let context = Value::Object(map);
        Ok(Self {
            context,
            handlebars,
        })
    }

    pub fn render_template(&self, template: &str) -> Result<String, anyhow::Error> {
        self.handlebars
            .render_template(template, &self.context)
            .map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use tracing::info;

    use super::*;

    #[test_log::test]
    fn test_handlebars_time() {
        let mut hb = handlebars::Handlebars::new();
        setup_handlebars(&mut hb);

        let rendered = hb
            .render_template("{{now_timestamp}}", &serde_json::Value::Null)
            .unwrap();
        let ts: i64 = rendered.parse().unwrap();

        let now = chrono::prelude::Utc::now().timestamp();
        assert!(
            (now - ts).abs() < 5,
            "timestamp should be within 5 seconds of now"
        );
        info!("Timestamp: {}", ts);

        // create context to test time formatting
        let context = LdrsExecutionContext::try_new("test.table", &hb, &[]).unwrap();
        let now = chrono::prelude::Utc::now();
        let year_render = context.render_template("{{YYYY}}").unwrap();
        assert_eq!(year_render, now.format("%Y").to_string());
        info!("year_render: {}", year_render);

        let rfc = context.render_template("{{rfc3339}}").unwrap();
        assert_eq!(rfc, now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true));
        info!("rfc: {}", rfc);
    }

    #[test_log::test]
    fn test_handlebars_vars() {
        let mut hb = handlebars::Handlebars::new();
        setup_handlebars(&mut hb);
        let env = vec![
            ("LDRS_TEMPL_TEST".to_string(), "TEMPL TEST".to_string()),
            ("LDRS_TEMPL_OVERRIDE".to_string(), "DEFAULT".to_string()),
            ("LDRS_TEMPL_TEST_TABLE_FOO".to_string(), "BAR".to_string()),
            (
                "LDRS_TEMPL_TEST_TABLE_OVERRIDE".to_string(),
                "OVERRIDEN".to_string(),
            ),
        ];
        let template_vars = collect_vars_by_prefix(&env, "TEMPL");

        let context = LdrsExecutionContext::try_new("test.table", &hb, &template_vars).unwrap();
        assert_eq!(
            context.render_template("{{ test }}").unwrap(),
            "TEMPL TEST".to_string()
        );
        assert_eq!(
            context.render_template("{{ override }}").unwrap(),
            "OVERRIDEN".to_string()
        );
        assert_eq!(
            context.render_template("{{ foo }}").unwrap(),
            "BAR".to_string()
        );
    }

    #[test_log::test]
    fn test_handlebars_schema() {
        let mut hb = handlebars::Handlebars::new();
        setup_handlebars(&mut hb);
        let context = LdrsExecutionContext::try_new("test.table", &hb, &[]).unwrap();
        assert_eq!(
            context.render_template("{{ schema }}.{{ table }}").unwrap(),
            "test.table".to_string()
        );

        let mut hb = handlebars::Handlebars::new();
        setup_handlebars(&mut hb);
        let context = LdrsExecutionContext::try_new("table", &hb, &[]).unwrap();
        assert_eq!(
            context.render_template("{{ table }}").unwrap(),
            "table".to_string()
        );
        assert_eq!(context.render_template("{{ schema }}").is_err(), true);
    }
}
