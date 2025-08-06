use anyhow::Context;
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use std::process::Command;
use tracing::info;
use url::Url;

use crate::types::{lua_args::LuaArgs, TableSchema};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnowflakeStrategy {
    Sql(Vec<String>),
    Ingest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnowflakeResult {
    pub pre_sql: Vec<String>,
    pub schema: Option<TableSchema>,
    pub strategy: SnowflakeStrategy,
    pub post_sql: Vec<String>,
}

#[derive(Subcommand)]
pub enum SnowflakeCommands {
    /// Execute a SQL statement (non-data retrieval)
    Exec { sql: String },
    Ingest {
        #[arg(long, short)]
        file_url: String,

        #[arg(long, short)]
        pattern: String,

        #[clap(flatten)]
        lua_args: LuaArgs,
    },
}

#[derive(Clone)]
pub struct SnowflakeConnection {
    pub conn_url: Url,
    pub raw_conn_url: String,
}

impl SnowflakeConnection {
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

    /// Execute multiple SQL statements in a transaction
    pub fn exec_transaction(&self, sql_statements: &[String]) -> Result<String, anyhow::Error> {
        if sql_statements.is_empty() {
            return Ok("No statements to execute".to_string());
        }

        // Combine all statements into a single transaction
        let transaction_sql = format!("BEGIN;\n{};\nCOMMIT;", sql_statements.join(";\n"));

        self.exec(&transaction_sql)
    }

    pub fn exec_each_statement(
        &self,
        sql_statements: &[String],
    ) -> Result<Vec<String>, anyhow::Error> {
        let mut results = Vec::new();
        for sql in sql_statements {
            let result = self.exec(sql)?;
            results.push(result);
        }
        Ok(results)
    }
}
