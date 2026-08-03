pub mod duckdb_source;

use anyhow::Context;
use ldrs_core::spawn::Spawned;
use object_store::ObjectStoreScheme;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::path::PathBuf;
use tokio_postgres::config::{Host, SslMode};
use url::Url;

use crate::ldrs_config::get_env_value;
use crate::ldrs_env::{child_env, shouty, starts_with_ignore_ascii_case};
use duckdb_source::DuckDbSource;

/// Suppresses the CLI's own result rendering, which shares stdout with the Arrow stream.
const RENDER_OFF: &str = ".output /dev/null";
const LOAD_ARROW: &str = "LOAD nanoarrow;";
/// The alias a Postgres source query reads through. Connection values come from the environment
const PG_ATTACH: &str = "ATTACH '' AS pg (TYPE postgres, READ_ONLY);";

fn resolve_scoped<'a>(
    vars: &'a [(String, String)],
    base: &str,
    ident: &str,
) -> Option<&'a (String, String)> {
    let raw = format!("{base}_{ident}");
    let snake = format!("{base}_{}", shouty(ident));
    get_env_value(vars, &[raw.as_str(), snake.as_str(), base])
}

pub fn resolve_binary(vars: &[(String, String)], ident: &str) -> Result<PathBuf, anyhow::Error> {
    match resolve_scoped(vars, "LDRS_DUCKDB_BIN", ident) {
        Some((_, path)) => Ok(PathBuf::from(path)),
        None => which::which("duckdb").with_context(|| "Failed to find duckdb binary in PATH"),
    }
}

/// The database file to attach. `None` runs in memory.
pub fn resolve_db(vars: &[(String, String)], ident: &str) -> Option<String> {
    resolve_scoped(vars, "LDRS_DUCKDB_DB", ident).map(|(_, path)| path.clone())
}

fn var(name: &str, value: impl Into<OsString>) -> (String, OsString) {
    (name.to_string(), value.into())
}

/// libpq takes a comma-separated list for both host and port, so a multi-host URL needs no special case.
fn joined(name: &str, parts: Vec<String>) -> Option<(String, OsString)> {
    match parts.is_empty() {
        true => None,
        false => Some(var(name, parts.join(","))),
    }
}

/// Connection values for a `postgres://` source URL, as the variables libpq reads. Empty for any
/// other scheme. Addressing and credential both travel in the environment.
pub fn pg_env(url: &str) -> Result<Vec<(String, OsString)>, anyhow::Error> {
    if !is_pg_url(url) {
        return Ok(vec![]);
    }

    let cfg = url
        .parse::<tokio_postgres::Config>()
        .with_context(|| "Could not parse Postgres source URL")?;

    let hosts = cfg
        .get_hosts()
        .iter()
        .map(|host| match host {
            Host::Tcp(name) => name.clone(),
            Host::Unix(path) => path.display().to_string(),
        })
        .collect();
    let ports = cfg.get_ports().iter().map(u16::to_string).collect();

    // Sent unconditionally: `prefer` is the default on both sides, so an unset mode is unchanged,
    let ssl_mode = match cfg.get_ssl_mode() {
        SslMode::Disable => "disable",
        SslMode::Require => "require",
        _ => "prefer",
    };

    let entries = vec![
        joined("PGHOST", hosts),
        joined("PGPORT", ports),
        cfg.get_user().map(|user| var("PGUSER", user)),
        cfg.get_dbname().map(|db| var("PGDATABASE", db)),
        cfg.get_password()
            .map(|pw| var("PGPASSWORD", OsString::from_vec(pw.to_vec()))),
        Some(var("PGSSLMODE", ssl_mode)),
        cfg.get_options().map(|opts| var("PGOPTIONS", opts)),
        cfg.get_application_name().map(|app| var("PGAPPNAME", app)),
        cfg.get_connect_timeout()
            .map(|t| var("PGCONNECT_TIMEOUT", t.as_secs().to_string())),
        // libpq reads this one in milliseconds; the URL form is parsed as seconds.
        cfg.get_tcp_user_timeout()
            .map(|t| var("PGTCPUSERTIMEOUT", t.as_millis().to_string())),
        cfg.get_keepalives_interval()
            .map(|i| var("PGKEEPALIVESINTERVAL", i.as_secs().to_string())),
        cfg.get_keepalives_retries()
            .map(|n| var("PGKEEPALIVESCOUNT", n.to_string())),
    ];

    Ok(entries.into_iter().flatten().collect())
}

fn copy_to_stdout(sql: &str) -> String {
    format!("COPY ({sql}) TO '/dev/stdout' (FORMAT arrows);")
}

/// Whether the read location is Postgres.
fn is_pg_url(url: &str) -> bool {
    starts_with_ignore_ascii_case(url, "postgres://")
        || starts_with_ignore_ascii_case(url, "postgresql://")
}

/// The attach for a `postgres://` read location, or `None` for any other scheme.
fn pg_attach_statement(url: &str) -> Option<String> {
    is_pg_url(url).then(|| PG_ATTACH.to_string())
}

/// A secret option carrying an addressing value from the environment
fn ambient_option(ambient: &[(String, OsString)], name: &str, keys: &[&str]) -> Option<String> {
    get_env_value(ambient, keys)
        .and_then(|(_, value)| value.to_str())
        .map(|value| format!("{name} '{value}'"))
}

/// `CREATE SECRET` for the read location's scheme, or `None` for local and plain HTTP. Credentials
/// are never emitted: `credential_chain` walks the same provider chain object_store uses. Only
/// addressing keys are forwarded, read from the variables object_store reads them from.
fn secret_statement(url: &str, ambient: &[(String, OsString)]) -> Option<String> {
    let parsed = Url::parse(url).ok()?;
    let (scheme, _) = ObjectStoreScheme::parse(&parsed).ok()?;
    let mut options = vec!["PROVIDER credential_chain".to_string()];

    let secret_type = match scheme {
        ObjectStoreScheme::MicrosoftAzure => {
            let short_form = matches!(parsed.scheme(), "az" | "azure");
            options.extend(
                short_form
                    .then(|| {
                        ambient_option(ambient, "ACCOUNT_NAME", &["AZURE_STORAGE_ACCOUNT_NAME"])
                    })
                    .flatten(),
            );
            "azure"
        }
        ObjectStoreScheme::AmazonS3 => {
            options.extend(ambient_option(
                ambient,
                "ENDPOINT",
                &["AWS_ENDPOINT_URL", "AWS_ENDPOINT"],
            ));
            options.extend(ambient_option(ambient, "REGION", &["AWS_REGION"]));
            "s3"
        }
        ObjectStoreScheme::GoogleCloudStorage => "gcs",
        _ => return None,
    };

    Some(format!(
        "CREATE SECRET ldrs_src (TYPE {secret_type}, {}, SCOPE '{url}');",
        options.join(", ")
    ))
}

/// ldrs owns the session: deterministic timezone and the statement the read location needs.
fn managed_statements(
    sql: &str,
    pre_sql: Option<&str>,
    src_url: Option<&str>,
    ambient: &[(String, OsString)],
) -> Vec<String> {
    let mut statements = vec![RENDER_OFF.to_string(), "SET TimeZone='UTC';".to_string()];
    statements.extend(pre_sql.map(|s| s.trim_end().to_string()));
    statements.push(LOAD_ARROW.to_string());
    statements.extend(src_url.and_then(pg_attach_statement));
    statements.extend(src_url.and_then(|url| secret_statement(url, ambient)));
    statements.push(copy_to_stdout(sql));
    statements
}

/// The caller's session is used as configured; ldrs adds only what it needs to read the output.
fn raw_statements(sql: &str, pre_sql: Option<&str>) -> Vec<String> {
    let mut statements = vec![RENDER_OFF.to_string()];
    statements.extend(pre_sql.map(|s| s.trim_end().to_string()));
    statements.push(LOAD_ARROW.to_string());
    statements.push(copy_to_stdout(sql));
    statements
}

/// Build the duckdb spawn spec. `sql` and `pre_sql` are already rendered; `ambient` is the
/// environment the child inherits from.
pub fn duckdb_spawned(
    binary: PathBuf,
    db: Option<String>,
    src: &DuckDbSource,
    sql: &str,
    pre_sql: Option<&str>,
    src_url: Option<&str>,
    ambient: Vec<(String, OsString)>,
    ldrs_env: &[(String, String)],
) -> Result<Spawned, anyhow::Error> {
    let pg = src_url.map(pg_env).transpose()?.unwrap_or_default();
    let managed = ldrs_env
        .iter()
        .filter(|(key, _)| key.starts_with("LDRS_PARAM_"))
        .map(|(key, value)| (key.clone(), value.clone().into()))
        .chain(pg)
        .collect();

    let (mut args, statements) = match src {
        DuckDbSource::Query(_) => (
            vec!["-bail".to_string(), "-no-init".to_string()],
            managed_statements(sql, pre_sql, src_url, &ambient),
        ),
        DuckDbSource::Raw(_) => (vec!["-bail".to_string()], raw_statements(sql, pre_sql)),
    };
    if let Some(path) = db {
        args.push("-readonly".to_string());
        args.push(path);
    }
    Ok(Spawned {
        binary,
        args,
        stdin: Some(statements.join("\n")),
        env: child_env(ambient, managed),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb_source::DuckDbBlock;

    fn block() -> DuckDbBlock {
        DuckDbBlock {
            name: "events".to_string(),
            sql: "SELECT 1".to_string(),
            pre_sql: None,
        }
    }

    const PG_URL: &str = "postgres://app_ro:s3cret@db.internal:5432/app";

    fn env_pairs(pairs: &[(&str, &str)]) -> Vec<(String, OsString)> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), OsString::from(*value)))
            .collect()
    }

    #[test]
    fn managed_order_puts_pre_sql_before_load_and_secret() {
        let statements = managed_statements("SELECT 1", Some("INSTALL azure;\n"), None, &[]);
        assert_eq!(
            statements,
            vec![
                ".output /dev/null",
                "SET TimeZone='UTC';",
                "INSTALL azure;",
                "LOAD nanoarrow;",
                "COPY (SELECT 1) TO '/dev/stdout' (FORMAT arrows);",
            ]
        );
    }

    #[test]
    fn raw_omits_timezone_and_secret() {
        let statements = raw_statements("SELECT 1", None);
        assert_eq!(
            statements,
            vec![
                ".output /dev/null",
                "LOAD nanoarrow;",
                "COPY (SELECT 1) TO '/dev/stdout' (FORMAT arrows);",
            ]
        );
    }

    #[test]
    fn local_and_http_need_no_secret() {
        assert_eq!(secret_statement("file:///data/", &[]), None);
        assert_eq!(secret_statement("https://example.com/data/", &[]), None);
        assert_eq!(secret_statement("not a url", &[]), None);
    }

    #[test]
    fn cloud_secret_carries_scope_and_no_material() {
        let secret = secret_statement("s3://bucket/prefix/", &[]).unwrap();
        assert!(secret.starts_with("CREATE SECRET ldrs_src (TYPE s3, PROVIDER credential_chain"));
        assert!(secret.contains("SCOPE 's3://bucket/prefix/'"));
        assert!(secret.ends_with(';'));

        assert!(secret_statement("gs://bucket/prefix/", &[])
            .unwrap()
            .contains("TYPE gcs"));
    }

    #[test]
    fn azure_account_name_only_for_the_short_form() {
        let ambient = env_pairs(&[("AZURE_STORAGE_ACCOUNT_NAME", "mylake")]);
        let short = secret_statement("az://lake/events/", &ambient).unwrap();
        assert!(short.contains("ACCOUNT_NAME 'mylake'"), "got: {short}");

        // the account is already in the host here, so adding it could contradict the URL
        let long = secret_statement("abfss://lake@mylake.dfs.core.windows.net/", &ambient).unwrap();
        assert!(!long.contains("ACCOUNT_NAME"), "got: {long}");

        let missing = secret_statement("az://lake/events/", &[]).unwrap();
        assert!(!missing.contains("ACCOUNT_NAME"), "got: {missing}");
    }

    #[test]
    fn s3_forwards_endpoint_and_region() {
        let ambient = env_pairs(&[
            ("AWS_ENDPOINT_URL", "http://minio:9000"),
            ("AWS_REGION", "us-east-1"),
        ]);
        let secret = secret_statement("s3://bucket/", &ambient).unwrap();
        assert!(
            secret.contains("ENDPOINT 'http://minio:9000'"),
            "got: {secret}"
        );
        assert!(secret.contains("REGION 'us-east-1'"), "got: {secret}");
    }

    #[test]
    fn managed_adds_no_init_and_an_attached_db_is_read_only() {
        let spawned = duckdb_spawned(
            PathBuf::from("duckdb"),
            Some("/data/warehouse.db".to_string()),
            &DuckDbSource::Query(block()),
            "SELECT 1",
            None,
            None,
            vec![],
            &[],
        )
        .unwrap();
        assert_eq!(
            spawned.args,
            vec!["-bail", "-no-init", "-readonly", "/data/warehouse.db"]
        );

        let spawned = duckdb_spawned(
            PathBuf::from("duckdb"),
            None,
            &DuckDbSource::Raw(block()),
            "SELECT 1",
            None,
            None,
            vec![],
            &[],
        )
        .unwrap();
        assert_eq!(spawned.args, vec!["-bail"]);
    }

    #[test]
    fn child_env_takes_params_from_config_and_the_rest_from_ambient() {
        let ambient = env_pairs(&[
            ("PATH", "/usr/bin"),
            ("AZURE_STORAGE_ACCOUNT_NAME", "lake"),
            // an inherited param must not reach the child on its own
            ("LDRS_PARAM_STALE", "inherited"),
        ]);
        let ldrs_env = vec![
            ("LDRS_SRC_EVENTS".to_string(), "az://lake/".to_string()),
            ("LDRS_PARAM_DAY".to_string(), "2026-07-27".to_string()),
        ];
        let env = duckdb_spawned(
            PathBuf::from("duckdb"),
            None,
            &DuckDbSource::Query(block()),
            "SELECT 1",
            None,
            None,
            ambient,
            &ldrs_env,
        )
        .unwrap()
        .env;

        assert!(env.contains(&("PATH".to_string(), OsString::from("/usr/bin"))));
        assert!(env.contains(&(
            "AZURE_STORAGE_ACCOUNT_NAME".to_string(),
            OsString::from("lake")
        )));
        assert!(env.contains(&("LDRS_PARAM_DAY".to_string(), OsString::from("2026-07-27"))));
        assert!(!env.iter().any(|(key, _)| key == "LDRS_SRC_EVENTS"));
        assert!(!env.iter().any(|(key, _)| key == "LDRS_PARAM_STALE"));
    }

    #[test]
    fn postgres_url_attaches_instead_of_creating_a_secret() {
        let statements =
            managed_statements("SELECT * FROM pg.public.users", None, Some(PG_URL), &[]);
        assert_eq!(
            statements,
            vec![
                ".output /dev/null",
                "SET TimeZone='UTC';",
                "LOAD nanoarrow;",
                "ATTACH '' AS pg (TYPE postgres, READ_ONLY);",
                "COPY (SELECT * FROM pg.public.users) TO '/dev/stdout' (FORMAT arrows);",
            ]
        );
        assert_eq!(
            secret_statement(PG_URL, &[]),
            None,
            "no secret for postgres"
        );
        assert_eq!(
            pg_attach_statement("s3://bucket/"),
            None,
            "no attach for s3"
        );
    }

    #[test]
    fn pg_env_carries_connection_values_and_no_sql() {
        let env = pg_env(PG_URL).unwrap();
        let value = |key: &str| {
            env.iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.to_str().unwrap())
        };

        assert_eq!(value("PGHOST"), Some("db.internal"));
        assert_eq!(value("PGPORT"), Some("5432"));
        assert_eq!(value("PGUSER"), Some("app_ro"));
        assert_eq!(value("PGDATABASE"), Some("app"));
        assert_eq!(value("PGPASSWORD"), Some("s3cret"));
        // sent whether or not the URL asked, so an explicit `require` is never dropped
        assert_eq!(value("PGSSLMODE"), Some("prefer"));
        assert_eq!(value("PGOPTIONS"), None);
    }

    #[test]
    fn pg_env_forwards_sslmode_options_and_multiple_hosts() {
        // in a URI each host carries its own port; the separate comma-lists are the env form
        let env = pg_env(
            "postgres://u@one.internal:5432,two.internal:5433/db\
             ?sslmode=require&options=-c%20search_path%3Dapp&connect_timeout=7",
        )
        .unwrap();
        let value = |key: &str| {
            env.iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.to_str().unwrap())
        };

        // libpq reads both as comma-separated lists, so failover needs no special case
        assert_eq!(value("PGHOST"), Some("one.internal,two.internal"));
        assert_eq!(value("PGPORT"), Some("5432,5433"));
        assert_eq!(value("PGSSLMODE"), Some("require"));
        assert_eq!(value("PGOPTIONS"), Some("-c search_path=app"));
        assert_eq!(value("PGCONNECT_TIMEOUT"), Some("7"));
    }

    #[test]
    fn pg_env_is_empty_for_every_other_scheme() {
        assert!(pg_env("s3://bucket/prefix/").unwrap().is_empty());
        assert!(pg_env("file:///data/").unwrap().is_empty());
        assert!(pg_env("not a url").unwrap().is_empty());
    }

    #[test]
    fn a_password_that_is_not_utf8_still_reaches_the_child() {
        // percent-encoded 0xFF: valid in a URL, never valid UTF-8
        let env = pg_env("postgres://u:%FF@host/db").unwrap();
        let password = env
            .iter()
            .find(|(key, _)| key == "PGPASSWORD")
            .map(|(_, value)| value.clone())
            .unwrap();
        assert_eq!(password.into_vec(), vec![0xFF]);
    }

    #[test]
    fn scoped_keys_prefer_the_most_specific() {
        let vars = vec![
            ("LDRS_DUCKDB_DB".to_string(), "/shared.db".to_string()),
            (
                "LDRS_DUCKDB_DB_EVENTS".to_string(),
                "/events.db".to_string(),
            ),
        ];
        assert_eq!(resolve_db(&vars, "events").as_deref(), Some("/events.db"));
        assert_eq!(resolve_db(&vars, "other").as_deref(), Some("/shared.db"));
        assert_eq!(resolve_db(&[], "events"), None);
    }
}
