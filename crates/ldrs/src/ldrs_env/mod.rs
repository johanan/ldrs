use handlebars::handlebars_helper;
use heck::ToShoutySnakeCase;
use ldrs_arrow::ColumnType;
use serde_json::{json, Value};
use tracing::{debug, warn};

/// Screaming-snake-case an identity for env-var lookups and template scoping (the one shared rule).
pub fn shouty(s: &str) -> String {
    s.to_shouty_snake_case()
}

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
    keys.iter()
        .filter_map(|key| {
            env_vars
                .iter()
                .find(|(k, _, _)| k.eq_ignore_ascii_case(key))
                .map(|(_, v, t)| (v.clone(), t.clone()))
        })
        .collect::<Vec<_>>()
}

handlebars_helper!(now_timestamp: | | chrono::prelude::Utc::now().timestamp() );
// Zero-pad an integer to a fixed width: `{{ pad index 5 }}` with index 3 -> "00003".
handlebars_helper!(pad: |value: i64, width: usize| format!("{value:0width$}"));
// `{{ shoutySnakeCase name }}` -> SCREAMING_SNAKE_CASE (owned here, not the handlebars feature).
handlebars_helper!(shouty_snake_case: |s: str| s.to_shouty_snake_case());
// Decompose a `schema.table` identity on the first dot; permissive (empty/whole, never error).
handlebars_helper!(schema_of: |s: str| s.split_once('.').map(|(a, _)| a).unwrap_or(""));
handlebars_helper!(table_of: |s: str| s.split_once('.').map(|(_, b)| b).unwrap_or(s));

/// The domain helpers paired with their names
fn helper_defs() -> Vec<(
    &'static str,
    Box<dyn handlebars::HelperDef + Send + Sync + 'static>,
)> {
    vec![
        ("now_timestamp", Box::new(now_timestamp)),
        ("pad", Box::new(pad)),
        ("shoutySnakeCase", Box::new(shouty_snake_case)),
        ("schema_of", Box::new(schema_of)),
        ("table_of", Box::new(table_of)),
    ]
}

pub fn setup_handlebars(handle_bars: &mut handlebars::Handlebars) -> () {
    for (name, def) in helper_defs() {
        handle_bars.register_helper(name, def);
    }
    handle_bars.set_strict_mode(true);
}

/// The registered helper names, for surfacing in a render error alongside the bound variables.
pub fn helper_names() -> Vec<&'static str> {
    helper_defs().into_iter().map(|(name, _)| name).collect()
}

/// Attach the bound variable names and available helpers to a render failure
pub fn explain_render_error(context: &Value, e: handlebars::RenderError) -> anyhow::Error {
    let vars = context
        .as_object()
        .map(|m| m.keys().cloned().collect::<Vec<_>>().join(", "))
        .unwrap_or_default();
    anyhow::Error::from(e).context(format!(
        "template render failed — bound variables: [{vars}]; helpers: [{}]",
        helper_names().join(", ")
    ))
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
        let name_prefix = format!("{}_", shouty(name));

        let mut map = serde_json::Map::new();

        // named is LDRS_TEMPL_<SHOUTY_NAME>_, default is LDRS_TEMPL_
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
        let now = chrono::prelude::Utc::now();
        map.insert("name".into(), name.into());
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
            .map_err(|e| explain_render_error(&self.context, e))
    }

    /// Derive a per-destination context: clone the base, insert (overwriting) the given vars,
    /// share the same handlebars registry.
    pub fn with_vars(&self, vars: &[(&str, &str)]) -> Self {
        let mut context = self.context.clone();
        if let Value::Object(map) = &mut context {
            for (k, v) in vars {
                map.insert(k.to_string(), Value::String(v.to_string()));
            }
        }
        Self {
            context,
            handlebars: self.handlebars,
        }
    }
}

#[cfg(test)]
mod tests {
    use tracing::info;

    use super::*;

    #[test]
    fn env_values_bind_in_key_order_not_lexical() {
        // collect_params leaves env vars lexically sorted; when param_keys is given, binding must
        // follow the param_keys order, not that lexical order (the P10-between-P1-P2 footgun).
        let env_vars: Vec<(String, String, Option<ColumnType>)> = vec![
            ("P1".to_string(), "one".to_string(), None),
            ("P2".to_string(), "two".to_string(), None),
        ];
        let keys = vec!["P2".to_string(), "P1".to_string()];
        let bound = get_env_values_by_keys(&keys, &env_vars);
        assert_eq!(
            bound,
            vec![("two".to_string(), None), ("one".to_string(), None)],
            "params must bind in param_keys order (P2, P1), not lexical env order"
        );
    }

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
    fn test_handlebars_pad() {
        let mut hb = handlebars::Handlebars::new();
        setup_handlebars(&mut hb);
        let rendered = hb
            .render_template("{{ pad index 5 }}", &json!({ "index": 3 }))
            .unwrap();
        assert_eq!(rendered, "00003");
        // a value wider than the pad width is not truncated
        let wide = hb
            .render_template("{{ pad index 3 }}", &json!({ "index": 123456 }))
            .unwrap();
        assert_eq!(wide, "123456");
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
    fn test_shouty() {
        // the shared Rust rule
        assert_eq!(shouty("public.users"), "PUBLIC_USERS");
        assert_eq!(shouty("acme.users"), "ACME_USERS");
        assert_eq!(shouty("users"), "USERS");
        assert_eq!(shouty("myTable"), "MY_TABLE");

        // the self-registered helper (no longer the handlebars `string_helpers` feature)
        let mut hb = handlebars::Handlebars::new();
        setup_handlebars(&mut hb);
        let rendered = hb
            .render_template(
                "{{ shoutySnakeCase name }}",
                &json!({ "name": "public.users" }),
            )
            .unwrap();
        assert_eq!(rendered, "PUBLIC_USERS");
    }

    #[test_log::test]
    fn test_schema_table_helpers() {
        // schema/table are no longer decomposed into the context; they come from the
        // `schema_of`/`table_of` helpers applied to `name` (or `target`), on demand.
        let mut hb = handlebars::Handlebars::new();
        setup_handlebars(&mut hb);
        let context = LdrsExecutionContext::try_new("test.table", &hb, &[]).unwrap();
        assert_eq!(
            context.render_template("{{ schema_of name }}").unwrap(),
            "test"
        );
        assert_eq!(
            context.render_template("{{ table_of name }}").unwrap(),
            "table"
        );
        assert_eq!(
            context
                .render_template("{{ schema_of name }}.{{ table_of name }}")
                .unwrap(),
            "test.table"
        );

        // no dot: schema_of is empty (permissive, never an error), table_of is the whole name
        let context = LdrsExecutionContext::try_new("table", &hb, &[]).unwrap();
        assert_eq!(context.render_template("{{ schema_of name }}").unwrap(), "");
        assert_eq!(
            context.render_template("{{ table_of name }}").unwrap(),
            "table"
        );
    }

    #[test_log::test]
    fn render_error_lists_variables_and_helpers() {
        // Strict mode makes an unknown variable an error; the message should name what is in scope.
        let mut hb = handlebars::Handlebars::new();
        setup_handlebars(&mut hb);
        let context = LdrsExecutionContext::try_new("public.users", &hb, &[]).unwrap();
        let err = context.render_template("{{ nonexistent }}").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("bound variables"), "got: {msg}");
        assert!(
            msg.contains("name"),
            "should list the bound `name` var, got: {msg}"
        );
        assert!(msg.contains("schema_of"), "should list helpers, got: {msg}");
    }
}
