pub mod snowflake_source;

use anyhow::Context;
use ldrs_arrow::ColumnType;
use ldrs_core::spawn::Spawned;
use std::{path::PathBuf, process::Command};
use tracing::{debug, warn};
use url::Url;

#[derive(Clone)]
pub struct SnowflakeConnection {
    pub conn_url: Url,
    pub raw_conn_url: String,
    pub binary_path: PathBuf,
    pub pem_key: Option<String>,
    pub pem_file: Option<String>,
}

impl SnowflakeConnection {
    pub fn create_connection(
        conn_url: &str,
        pem_key: Option<String>,
        pem_file: Option<String>,
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
        });
    }

    /// Execute an ordered list of SQL statements via `ldrs-sf exec` in a single spawn, returning the
    /// captured stdout JSON (one result set per statement). Statements are passed pre-separated as
    /// repeated `--sql` flags; ldrs-sf runs them in order and stops at the first driver error.
    pub fn exec(&self, statements: &[String]) -> Result<String, anyhow::Error> {
        if statements.is_empty() {
            return Ok(String::new());
        }

        let mut cmd = Command::new(&self.binary_path);
        cmd.arg("exec");
        for sql in statements {
            cmd.arg("--sql").arg(sql);
        }

        debug!("Running ldrs-sf exec: {} statement(s)", statements.len());

        let parent: Vec<(String, String)> = std::env::vars().collect();
        let managed = sf_auth_env(
            &self.raw_conn_url,
            self.pem_key.as_deref(),
            self.pem_file.as_deref(),
        )
        .into_iter()
        .chain(sf_param_env(&[]))
        .collect();
        let env = build_child_env(parent, "LDRS_SF_", managed);
        cmd.env_clear();
        cmd.envs(env);
        let output = cmd
            .output()
            .with_context(|| "Failed to execute ldrs-sf command")?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            Ok(stdout.to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(anyhow::anyhow!("Command failed: {}", stderr))
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

/// Build the complete child env: inherit the parent, drop its `LDRS_*` config vars (leaving
/// `keep_prefix` warned), then overlay the
/// managed set so ldrs's resolved values win over any inherited collision.
fn build_child_env(
    parent: Vec<(String, String)>,
    keep_prefix: &str,
    managed: Vec<(String, String)>,
) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = parent
        .into_iter()
        .filter(|(k, _)| {
            if k.starts_with(keep_prefix) {
                warn!("inherited user-set {k} reaches the child; execution not guaranteed for env ldrs does not manage");
                true
            } else {
                !k.starts_with("LDRS_")
            }
        })
        .collect();
    env.extend(managed);
    env
}

/// Build the ldrs-sf spawn spec: binary, `query --sql` args, and the resolved child env. The
/// caller spawns it via `spawn_arrow_source`. `bind_params` are the parameter values and types, in
/// bind order.
pub fn sf_spawned(
    conn: &SnowflakeConnection,
    sql: &str,
    bind_params: Vec<(String, Option<ColumnType>)>,
) -> Spawned {
    let managed: Vec<(String, String)> = sf_auth_env(
        &conn.raw_conn_url,
        conn.pem_key.as_deref(),
        conn.pem_file.as_deref(),
    )
    .into_iter()
    .chain(sf_param_env(&bind_params))
    .collect();
    let env = build_child_env(std::env::vars().collect(), "LDRS_SF_", managed);
    Spawned {
        binary: conn.binary_path.clone(),
        args: vec!["query".to_string(), "--sql".to_string(), sql.to_string()],
        stdin: None,
        env,
    }
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

    #[test]
    fn build_child_env_strips_config_keeps_sf_and_overlays_managed() {
        let parent = vec![
            ("LDRS_SRC_SF".to_string(), "url".to_string()),
            ("LDRS_SF_PEM_KEY".to_string(), "legacy".to_string()),
            ("PATH".to_string(), "/usr/bin".to_string()),
        ];
        let managed = vec![("LDRS_SF_SOURCE".to_string(), "resolved".to_string())];
        let env = build_child_env(parent, "LDRS_SF_", managed);

        // config var dropped; the kept-prefix and non-LDRS vars retained; managed overlaid
        assert!(!env.iter().any(|(k, _)| k == "LDRS_SRC_SF"));
        assert!(env.contains(&("LDRS_SF_PEM_KEY".to_string(), "legacy".to_string())));
        assert!(env.contains(&("PATH".to_string(), "/usr/bin".to_string())));
        assert!(env.contains(&("LDRS_SF_SOURCE".to_string(), "resolved".to_string())));
    }
}
