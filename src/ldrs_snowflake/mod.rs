use anyhow::Context;
use clap::Subcommand;
use std::process::Command;
use tracing::info;
use url::Url;

#[derive(Subcommand)]
pub enum SnowflakeCommands {
    /// Execute a SQL statement (non-data retrieval)
    Exec { sql: String },
}

#[derive(Clone)]
pub struct SnowflakeConnection {
    pub conn_url: Url,
    pub raw_conn_url: String,
}

pub fn create_connection(conn_url: &str) -> Result<SnowflakeConnection, anyhow::Error> {
    let parsed_url = Url::parse(conn_url).with_context(|| "Failed to parse connection URL")?;

    if parsed_url.scheme() != "snowflake" {
        return Err(anyhow::anyhow!(
            "Invalid scheme in connection URL: expected 'snowflake'"
        ));
    }

    return Ok(SnowflakeConnection {
        conn_url: parsed_url,
        raw_conn_url: conn_url.to_string(),
    });
}

impl SnowflakeConnection {
    /// Execute a SQL statement (typically DDL) and return success/failure
    pub fn exec(&self, sql: &str) -> Result<String, anyhow::Error> {
        let mut cmd = Command::new("mvr");
        let args = vec!["exec", "--sql", sql];

        info!("Running command: mvr {:?}", args);

        let output = cmd
            .args(args)
            .env("MVR_DEST", "stdout://")
            .env("MVR_SOURCE", &self.raw_conn_url)
            .output()
            .with_context(|| "Failed to execute mvr command")?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            Ok(stdout.to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(anyhow::anyhow!("Command failed: {}", stderr))
        }
    }
}
