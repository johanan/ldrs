pub mod snowflake_source;

use anyhow::Context;
use ldrs_arrow::ColumnType;
use ldrs_core::spawn::Spawned;
use std::{ffi::OsString, path::PathBuf, process::Command};
use tracing::debug;
use url::Url;

use crate::ldrs_env::child_env;

#[derive(Clone)]
pub struct SnowflakeConnection {
    pub conn_url: Url,
    pub raw_conn_url: String,
    pub binary_path: PathBuf,
    pub pem_key: Option<String>,
    pub pem_file: Option<String>,
    pub inherited_sf_env: Vec<(String, String)>,
}

impl SnowflakeConnection {
    pub fn create_connection(
        conn_url: &str,
        pem_key: Option<String>,
        pem_file: Option<String>,
        inherited_sf_env: Vec<(String, String)>,
    ) -> Result<SnowflakeConnection, anyhow::Error> {
        let parsed_url = Url::parse(conn_url).with_context(|| "Failed to parse connection URL")?;
        let binary_path =
            which::which("ldrs-sf").with_context(|| "Failed to find ldrs-sf binary in PATH")?;

        if parsed_url.scheme() != "snowflake" {
            return Err(anyhow::anyhow!(
                "Invalid scheme in connection URL: expected 'snowflake'"
            ));
        }

        return Ok(SnowflakeConnection {
            conn_url: parsed_url,
            raw_conn_url: conn_url.to_string(),
            binary_path,
            pem_key,
            pem_file,
            inherited_sf_env,
        });
    }

    /// Execute an ordered list of SQL statements via `ldrs-sf exec` in a single spawn, returning the
    /// captured stdout JSON (one result set per statement). Statements are passed pre-separated as
    /// repeated `--sql` flags; ldrs-sf runs them in order and stops at the first driver error.
    pub fn exec(
        &self,
        statements: &[String],
        ambient: Vec<(String, OsString)>,
    ) -> Result<String, anyhow::Error> {
        if statements.is_empty() {
            return Ok(String::new());
        }
        debug!("Running ldrs-sf exec: {} statement(s)", statements.len());

        let args = std::iter::once("exec".to_string())
            .chain(
                statements
                    .iter()
                    .flat_map(|sql| ["--sql".to_string(), sql.clone()]),
            )
            .collect();
        run_capture(self.spawn(args, &[], ambient))
    }

    /// First create the auth LDRS_SF_*, then bind the params as LDRS_SF_PARAM_P*.
    pub fn env(&self, params: &[(String, Option<ColumnType>)]) -> Vec<(String, OsString)> {
        self.inherited_sf_env
            .iter()
            .cloned()
            .chain(sf_auth_env(
                &self.raw_conn_url,
                self.pem_key.as_deref(),
                self.pem_file.as_deref(),
            ))
            .chain(sf_param_env(params))
            .map(|(k, v)| (k, v.into()))
            .collect()
    }

    pub fn spawn(
        &self,
        args: Vec<String>,
        params: &[(String, Option<ColumnType>)],
        ambient: Vec<(String, OsString)>,
    ) -> Spawned {
        Spawned {
            binary: self.binary_path.clone(),
            args,
            stdin: None,
            env: child_env(ambient, self.env(params)),
        }
    }
}

/// Build the ordered `LDRS_SF_PARAM_P<n>` env pairs for ldrs-sf, one per bound parameter. Names
/// are zero-padded to the count's width so ldrs-sf's lexical sort of the names matches this
/// positional order past nine params (`P10` would otherwise sort between `P1` and `P2`).
fn sf_param_env(params: &[(String, Option<ColumnType>)]) -> Vec<(String, String)> {
    let width = params.len().to_string().len();
    params
        .iter()
        .enumerate()
        .map(|(i, (value, _))| {
            (
                format!("LDRS_SF_PARAM_P{:0width$}", i + 1, width = width),
                value.clone(),
            )
        })
        .collect()
}

/// The inherited `LDRS_SF_*`, minus params.
pub fn resolve_inherited_sf_env(vars: &[(String, String)]) -> Vec<(String, String)> {
    vars.iter()
        .filter(|(key, _)| key.starts_with("LDRS_SF_") && !key.starts_with("LDRS_SF_PARAM_"))
        .cloned()
        .collect()
}

/// Resolve the Snowflake credentials co-located with a resolved connection `base`. Strict co-location
pub fn resolve_conn_creds(
    vars: &[(String, String)],
    base: &str,
) -> (Option<String>, Option<String>) {
    let at = |attr: &str| -> Option<String> {
        let key = format!("{base}_{attr}");
        vars.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(&key))
            .map(|(_, v)| v.clone())
    };
    (at("PEM_KEY"), at("PEM_FILE"))
}

/// The connection auth env: the source URL and, if resolved, the PEM credential.
fn sf_auth_env(
    source: &str,
    pem_key: Option<&str>,
    pem_file: Option<&str>,
) -> Vec<(String, String)> {
    let mut env = vec![("LDRS_SF_SOURCE".to_string(), source.to_string())];
    if let Some(pk) = pem_key {
        env.push(("LDRS_SF_PEM_KEY".to_string(), pk.to_string()));
    }
    if let Some(pf) = pem_file {
        env.push(("LDRS_SF_PEM_FILE".to_string(), pf.to_string()));
    }
    env
}

/// Run a spec to completion and return its stdout. `stdin` is ignored
fn run_capture(spec: Spawned) -> Result<String, anyhow::Error> {
    let mut cmd = Command::new(&spec.binary);
    cmd.args(&spec.args).env_clear().envs(spec.env);
    let output = cmd
        .output()
        .with_context(|| "Failed to execute ldrs-sf command")?;
    match output.status.success() {
        true => Ok(String::from_utf8_lossy(&output.stdout).into_owned()),
        false => Err(anyhow::anyhow!(
            "Command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )),
    }
}

/// Build the ldrs-sf spawn spec for a query. The caller spawns it via `spawn_arrow_source`.
/// `bind_params` are the parameter values and types, in bind order.
pub fn sf_spawned(
    conn: &SnowflakeConnection,
    sql: &str,
    bind_params: Vec<(String, Option<ColumnType>)>,
    ambient: Vec<(String, OsString)>,
) -> Spawned {
    let args = vec!["query".to_string(), "--sql".to_string(), sql.to_string()];
    conn.spawn(args, &bind_params, ambient)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sf_param_env_zero_pads_names() {
        let params: Vec<(String, Option<ColumnType>)> =
            (1..=10).map(|i| (format!("v{i}"), None)).collect();
        let env = sf_param_env(&params);
        assert_eq!(env[0].0, "LDRS_SF_PARAM_P01", "first param, zero-padded");
        assert_eq!(env[9].0, "LDRS_SF_PARAM_P10", "tenth param");
        // ldrs-sf sorts these names lexically; zero-padding must make that match positional order.
        let names: Vec<String> = env.iter().map(|(k, _)| k.clone()).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(
            names, sorted,
            "padded names must sort lexically into positional order"
        );
    }

    #[test]
    fn conn_creds_are_strictly_co_located() {
        let vars = vec![
            ("LDRS_SRC_SF".to_string(), "snowflake://acct".to_string()),
            ("LDRS_SRC_SF_PEM_KEY".to_string(), "KEYMATERIAL".to_string()),
        ];
        let (pk, pf) = resolve_conn_creds(&vars, "LDRS_SRC_SF");
        assert_eq!(pk.as_deref(), Some("KEYMATERIAL"));
        assert_eq!(pf, None);
        // a different base (e.g. a per-name URL) does NOT inherit the kind-scoped key
        let (pk2, _) = resolve_conn_creds(&vars, "LDRS_SRC_SALES");
        assert_eq!(
            pk2, None,
            "credential must be co-located with the resolved connection base"
        );
    }

    #[test]
    fn managed_env_is_auth_keys_then_params() {
        let params = vec![("v1".to_string(), None), ("v2".to_string(), None)];
        let env = sf_auth_env("snowflake://acct", Some("KEY"), None)
            .into_iter()
            .chain(sf_param_env(&params))
            .collect::<Vec<_>>();
        assert_eq!(
            env,
            vec![
                ("LDRS_SF_SOURCE".to_string(), "snowflake://acct".to_string()),
                ("LDRS_SF_PEM_KEY".to_string(), "KEY".to_string()),
                ("LDRS_SF_PARAM_P1".to_string(), "v1".to_string()),
                ("LDRS_SF_PARAM_P2".to_string(), "v2".to_string()),
            ],
            "managed env is auth keys (no PEM_FILE when None) then ordered params"
        );
    }

    fn connection(inherited: Vec<(String, String)>) -> SnowflakeConnection {
        SnowflakeConnection {
            conn_url: Url::parse("snowflake://acct").unwrap(),
            raw_conn_url: "snowflake://acct".to_string(),
            binary_path: PathBuf::from("ldrs-sf"),
            pem_key: Some("RESOLVED".to_string()),
            pem_file: None,
            inherited_sf_env: inherited,
        }
    }

    /// Least specific first, so a resolved per-target credential displaces an inherited global and
    /// never the other way round. `Command::envs` applies in order, so the last write wins.
    #[test]
    fn resolved_values_overlay_the_inherited_ones() {
        let conn = connection(vec![
            ("LDRS_SF_PEM_KEY".to_string(), "LEGACY".to_string()),
            ("LDRS_SF_PROXY".to_string(), "corp".to_string()),
        ]);
        let env = conn.env(&[("v1".to_string(), None)]);
        let names: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();

        assert_eq!(
            names,
            vec![
                "LDRS_SF_PEM_KEY",
                "LDRS_SF_PROXY",
                "LDRS_SF_SOURCE",
                "LDRS_SF_PEM_KEY",
                "LDRS_SF_PARAM_P1",
            ],
            "inherited, then auth, then params"
        );
        let last_pem = env
            .iter()
            .rev()
            .find(|(k, _)| k == "LDRS_SF_PEM_KEY")
            .unwrap();
        assert_eq!(last_pem.1, OsString::from("RESOLVED"));
        assert!(env.contains(&("LDRS_SF_PROXY".to_string(), OsString::from("corp"))));
    }

    /// Params are never inherited: ldrs-sf finds binds by scanning the prefix, so a stale one would
    /// be read as an extra bind rather than overridden.
    #[test]
    fn inherited_env_excludes_params() {
        let vars = vec![
            ("LDRS_SF_PROXY".to_string(), "corp".to_string()),
            ("LDRS_SF_PARAM_P01".to_string(), "stale".to_string()),
            ("LDRS_SRC_SF".to_string(), "snowflake://acct".to_string()),
        ];
        let inherited = resolve_inherited_sf_env(&vars);
        assert_eq!(
            inherited,
            vec![("LDRS_SF_PROXY".to_string(), "corp".to_string())]
        );
    }
}
