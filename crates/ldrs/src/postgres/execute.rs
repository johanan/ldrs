use arrow_array::RecordBatch;
use deadpool_postgres::Pool;
use ldrs_arrow::{ColumnSpec, ColumnType};
use ldrs_postgres::{
    execute_create_table, execute_create_temp_table, execute_merge, execute_prepared_stmt,
    execute_sql, PgCopySink,
};
use tokio_postgres::Client;

use crate::{
    ldrs_env::{get_params_for_stmt_with_default, LdrsExecutionContext},
    postgres::postgres_destination::{PgDestCommand, PgMergeConfig, PgPlan, PgPreparedStmt},
};

/// Render a command's templates against the context, returning a command with literal strings.
fn render_command(
    cmd: &PgDestCommand,
    context: &LdrsExecutionContext<'_>,
) -> Result<PgDestCommand, anyhow::Error> {
    Ok(match cmd {
        PgDestCommand::Sql(sql) => PgDestCommand::Sql(context.render_template(sql)?),
        PgDestCommand::Prepared(stmt) => PgDestCommand::Prepared(PgPreparedStmt {
            stmt: context.render_template(&stmt.stmt)?,
            key: stmt
                .key
                .as_ref()
                .map(|k| context.render_template(k))
                .transpose()?
                .map(|k| k.to_uppercase()),
            types: stmt.types.clone(),
        }),
        PgDestCommand::CreateTable(t) => PgDestCommand::CreateTable(context.render_template(t)?),
        PgDestCommand::CreateTempTable(t) => {
            PgDestCommand::CreateTempTable(context.render_template(t)?)
        }
        PgDestCommand::Merge(m) => PgDestCommand::Merge(PgMergeConfig {
            target: context.render_template(&m.target)?,
            source: context.render_template(&m.source)?,
            keys: m.keys.clone(),
        }),
        PgDestCommand::Load(t) => PgDestCommand::Load(context.render_template(t)?),
    })
}

/// Execute an already-rendered command on the connection. No context: the strings are literal;
/// `env_params` is still needed to bind a `Prepared` statement's params at execution time.
async fn execute_command(
    client: &Client,
    cmd: &PgDestCommand,
    all_params: &[(String, String, Option<ColumnType>)],
    final_cols: &[ColumnSpec],
) -> Result<(), anyhow::Error> {
    match cmd {
        PgDestCommand::Sql(sql) => execute_sql(client, sql).await,
        PgDestCommand::Prepared(stmt) => {
            let resolved_params = get_params_for_stmt_with_default(stmt.key.as_deref(), all_params);
            execute_prepared_stmt(client, &stmt.stmt, &resolved_params, stmt.types.as_deref()).await
        }
        PgDestCommand::CreateTable(table) => execute_create_table(client, table, final_cols).await,
        PgDestCommand::CreateTempTable(table) => {
            execute_create_temp_table(client, table, final_cols).await
        }
        PgDestCommand::Merge(m) => {
            execute_merge(client, &m.source, &m.target, &m.keys, final_cols).await
        }
        PgDestCommand::Load(_) => unreachable!("Load is the COPY, not an executed command"),
    }
}

pub struct PgSink {
    copy: PgCopySink,
    /// Post-load commands, rendered at open (identity is fixed then) and executed at commit — so
    /// commit needs neither the context nor the handlebars registry.
    after_commands: Vec<PgDestCommand>,
    final_cols: Vec<ColumnSpec>,
    /// The resolved destination identity (`schema.table`), for the outcome.
    target: String,
}

impl PgSink {
    /// Check out a connection, `BEGIN`, set the role, run the pre-load commands, and
    /// open the binary COPY.
    pub async fn open(
        pool: Pool,
        role: Option<String>,
        plan: PgPlan,
        final_cols: Vec<ColumnSpec>,
        target: String,
        context: &LdrsExecutionContext<'_>,
        env_params: &[(String, String, Option<ColumnType>)],
    ) -> Result<Self, anyhow::Error> {
        let PgPlan {
            before,
            load_table,
            after,
        } = plan;
        let load_table = context.render_template(&load_table)?;
        // Render the post-load commands now, while the context is in hand; identity is fixed.
        let after_commands = after
            .iter()
            .map(|cmd| render_command(cmd, context))
            .collect::<Result<Vec<_>, _>>()?;

        let conn = pool.get().await?;
        let prepared = async {
            conn.batch_execute("BEGIN").await?;
            if let Some(role) = &role {
                anyhow::ensure!(
                    !role.contains('"'),
                    "Role name cannot contain double quotes: {}",
                    role
                );
                conn.execute(&format!(r#"SET LOCAL ROLE "{}""#, role), &[])
                    .await?;
            }
            for cmd in &before {
                execute_command(&conn, &render_command(cmd, context)?, env_params, &final_cols)
                    .await?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(e) = prepared {
            let _ = conn.batch_execute("ROLLBACK").await;
            return Err(e);
        }

        let copy = PgCopySink::open(conn, &load_table, final_cols.clone()).await?;
        Ok(PgSink {
            copy,
            after_commands,
            final_cols,
            target,
        })
    }

    /// The resolved destination identity (the landing `schema.table`).
    pub fn target(&self) -> &str {
        &self.target
    }

    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), anyhow::Error> {
        self.copy.write_batch(batch).await
    }

    /// Finish the COPY, run the post-load commands, and `COMMIT`. A post-load failure
    /// rolls back instead.
    pub async fn commit(
        self,
        env_params: &[(String, String, Option<ColumnType>)],
    ) -> Result<(), anyhow::Error> {
        let PgSink {
            copy,
            after_commands,
            final_cols,
            ..
        } = self;
        let conn = copy.finish().await?;
        let finalized = async {
            for cmd in &after_commands {
                execute_command(&conn, cmd, env_params, &final_cols).await?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(e) = finalized {
            let _ = conn.batch_execute("ROLLBACK").await;
            return Err(e);
        }
        conn.batch_execute("COMMIT").await?;
        Ok(())
    }

    /// Abandon the COPY and roll back the transaction.
    pub async fn rollback(self) {
        let conn = self.copy.cancel();
        let _ = conn.batch_execute("ROLLBACK").await;
    }
}
