pub mod snowflake_source;

use anyhow::Context;
use arrow::ipc::reader::StreamReader;
use arrow_array::RecordBatch;
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    process::{Command, Stdio},
};
use tokio::task;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info};
use url::Url;

use crate::types::{lua_args::LuaArgs, ColumnType};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnowflakeStrategy {
    Sql(Vec<String>),
    Ingest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnowflakeResult {
    pub pre_sql: Vec<String>,
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

pub struct SnowflakeStreamResult<S> {
    pub batch_stream: ReceiverStream<Result<RecordBatch, anyhow::Error>>,
    pub schema_stream: S,
    pub command_handle: tokio::task::JoinHandle<Result<(), anyhow::Error>>,
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
        let mut cmd = Command::new("ldrs-sf");
        let args = vec!["exec", "--sql", sql];

        info!("Running command: ldrs-sf {:?}", args);

        let output = cmd
            .args(args)
            .env("LDRS_SF_SOURCE", &self.raw_conn_url)
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

/// Execute a SQL query and stream the results as Arrow RecordBatches.
/// This uses the ldrs-sf binary to execute the query and stream the results.
/// `bind_params` should already be sorted by key and this is the value and type of the parameter.
pub async fn sf_arrow_stream(
    conn: &str,
    sql: &str,
    bind_params: Vec<(String, Option<ColumnType>)>,
) -> Result<
    SnowflakeStreamResult<impl Future<Output = Option<arrow_schema::SchemaRef>> + Send>,
    anyhow::Error,
> {
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    let (schema_tx, schema_rx) = tokio::sync::oneshot::channel();
    let (status_tx, status_rx) = tokio::sync::oneshot::channel();
    let sql = sql.to_string();
    let conn_url = conn.to_string();

    task::spawn_blocking(move || {
        let mut cmd = std::process::Command::new("ldrs-sf");
        let args = vec!["query", "--sql", &sql];
        let cmd = cmd.args(args);
        info!("Running command: {:?}", cmd);
        let cmd = cmd.env("LDRS_SF_SOURCE", conn_url);
        // bind the parameters in order for ldrs-sf
        let sf_named_params = bind_params.into_iter().enumerate().map(|(i, (value, _))| {
            let name = format!("LDRS_SF_PARAM_P{}", i + 1);
            (name, value)
        });
        let cmd = cmd.envs(sf_named_params);

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

        // No SyncIoBridge needed - stdout is already synchronous
        let buf_reader = std::io::BufReader::new(stdout);
        let mut stream_reader = StreamReader::try_new(buf_reader, None)
            .with_context(|| "Failed to create StreamReader")?;

        let schema = stream_reader.schema();
        if schema_tx.send(schema).is_err() {
            return Err(anyhow::anyhow!("Failed to send schema: receiver dropped"));
        }

        // Stream batches
        while let Some(batch_result) = stream_reader.next() {
            debug!("Processing batch");
            if tx
                .blocking_send(batch_result.map_err(anyhow::Error::from))
                .is_err()
            {
                return Err(anyhow::anyhow!("Failed to send batch"));
            }
        }
        let status = child.wait()?;

        if !status.success() {
            // Read stderr synchronously if command failed
            let mut stderr_output = String::new();
            std::io::Read::read_to_string(&mut stderr, &mut stderr_output)?;

            let _ = status_tx.send(Err(anyhow::anyhow!("ldrs-sf stderr: {}", stderr_output)));
        } else {
            let _ = status_tx.send(Ok(()));
        }

        Ok::<(), anyhow::Error>(())
    });

    let command_handle = tokio::spawn(async move {
        match status_rx.await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("Command monitoring task failed")),
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
