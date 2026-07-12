pub mod snowflake_source;

use anyhow::Context;
use arrow::ipc::reader::StreamReader;
use arrow_array::RecordBatch;
use ldrs_arrow::ColumnType;
use std::{
    future::Future,
    path::PathBuf,
    process::{Command, Stdio},
};
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, trace, warn};
use url::Url;

#[derive(Clone)]
pub struct SnowflakeConnection {
    pub conn_url: Url,
    pub raw_conn_url: String,
    pub binary_path: PathBuf,
    pub pem_key: Option<String>,
    pub pem_file: Option<String>,
}

pub struct SnowflakeStreamResult<S> {
    pub batch_stream: ReceiverStream<Result<RecordBatch, anyhow::Error>>,
    pub schema_stream: S,
    pub command_handle: tokio::task::JoinHandle<Result<(), anyhow::Error>>,
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
        apply_child_env(&mut cmd, &parent, managed);
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

/// Wire the managed env onto a spawned ldrs-sf Command. The child inherits the parent env
/// (fork-like); this strips the parent's `LDRS_*` config vars but leaves `LDRS_SF_*` in place, so
/// user-set `LDRS_SF_*` reach the child by inheritance (each is warned). `managed` is then set on
/// top, so ldrs's resolved values win over any inherited `LDRS_SF_*` they collide with.
fn apply_child_env(cmd: &mut Command, parent: &[(String, String)], managed: Vec<(String, String)>) {
    for (k, _) in parent {
        match k {
            _ if k.starts_with("LDRS_SF_") => {
                warn!("inherited user-set {k} reaches ldrs-sf; execution not guaranteed for env ldrs does not manage");
            }
            _ if k.starts_with("LDRS_") => {
                cmd.env_remove(k);
            }
            _ => {}
        }
    }
    cmd.envs(managed);
}

/// Execute a SQL query and stream the results as Arrow RecordBatches.
/// This uses the ldrs-sf binary to execute the query and stream the results.
/// `bind_params` are the parameter values and types, in the order they bind.
pub async fn sf_arrow_stream(
    conn: &SnowflakeConnection,
    sql: &str,
    bind_params: Vec<(String, Option<ColumnType>)>,
) -> Result<
    SnowflakeStreamResult<impl Future<Output = Option<arrow_schema::SchemaRef>> + Send>,
    anyhow::Error,
> {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let (schema_tx, schema_rx) = tokio::sync::oneshot::channel();
    let sql = sql.to_string();
    let conn_url = conn.raw_conn_url.clone();
    let binary_path = conn.binary_path.clone();
    let pem_key = conn.pem_key.clone();
    let pem_file = conn.pem_file.clone();

    let command_handle = task::spawn_blocking(move || {
        let mut cmd = std::process::Command::new(&binary_path);
        let args = vec!["query", "--sql", &sql];
        let cmd = cmd.args(args);
        debug!("Running command: {:?}", cmd);
        let parent: Vec<(String, String)> = std::env::vars().collect();
        let managed = sf_auth_env(&conn_url, pem_key.as_deref(), pem_file.as_deref())
            .into_iter()
            .chain(sf_param_env(&bind_params))
            .collect();
        apply_child_env(cmd, &parent, managed);

        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd
            .spawn()
            .with_context(|| format!("Failed to spawn ldrs-sf"))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stdout"))?;

        let mut stderr = child
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to capture stderr"))?;

        let buf_reader = std::io::BufReader::new(stdout);
        let stream_reader = StreamReader::try_new(buf_reader, None).ok();
        let stream_opened = stream_reader.is_some();

        if let Some(mut stream_reader) = stream_reader {
            let schema = stream_reader.schema();
            if schema_tx.send(schema).is_err() {
                return Err(anyhow::anyhow!("Failed to send schema: receiver dropped"));
            }

            // Stream batches
            while let Some(batch_result) = stream_reader.next() {
                trace!("Processing batch");
                if tx
                    .blocking_send(batch_result.map_err(anyhow::Error::from))
                    .is_err()
                {
                    return Err(anyhow::anyhow!("Failed to send batch"));
                }
            }
        }
        let status = child.wait()?;

        match (status.success(), stream_opened) {
            (true, true) => Ok(()),
            (true, false) => Err(anyhow::anyhow!(
                "Command succeeded but failed to open Arrow stream"
            )),
            (false, _) => {
                let mut stderr_output = String::new();
                std::io::Read::read_to_string(&mut stderr, &mut stderr_output)?;
                Err(anyhow::anyhow!(
                    "ldrs-sf command failed with status: {}. Stderr: {}",
                    status,
                    stderr_output
                ))
            }
        }
    });

    let batch_stream = ReceiverStream::new(rx);
    let schema_future = async move {
        match schema_rx.await {
            Ok(schema) => {
                if schema.fields().is_empty() {
                    None
                } else {
                    debug!("Schema: {:?}", schema);
                    Some(schema)
                }
            }
            Err(_) => None,
        }
    };

    Ok(SnowflakeStreamResult {
        batch_stream,
        schema_stream: schema_future,
        command_handle,
    })
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
    fn apply_child_env_strips_config_keeps_sf_and_sets_managed() {
        let parent = vec![
            ("LDRS_SRC_SF".to_string(), "url".to_string()),
            ("LDRS_SF_PEM_KEY".to_string(), "legacy".to_string()),
            ("PATH".to_string(), "/usr/bin".to_string()),
        ];
        let managed = vec![("LDRS_SF_SOURCE".to_string(), "resolved".to_string())];
        let mut cmd = Command::new("true");
        apply_child_env(&mut cmd, &parent, managed);

        // get_envs() reports only our explicit modifications: Some(v) = set, None = remove.
        let mods: Vec<(String, Option<String>)> = cmd
            .get_envs()
            .map(|(k, v)| {
                (
                    k.to_string_lossy().into_owned(),
                    v.map(|v| v.to_string_lossy().into_owned()),
                )
            })
            .collect();

        // the config var is queued for removal; the managed key is set
        assert!(mods.contains(&("LDRS_SRC_SF".to_string(), None)));
        assert!(mods.contains(&("LDRS_SF_SOURCE".to_string(), Some("resolved".to_string()))));
        // a user LDRS_SF_ key is left inherited (no modification), and non-LDRS is untouched
        assert!(!mods.iter().any(|(k, _)| k == "LDRS_SF_PEM_KEY"));
        assert!(!mods.iter().any(|(k, _)| k == "PATH"));
    }
}
