use arrow_array::RecordBatch;
use ldrs_arrow::{ArrowColumnTransformStrategy, ColumnSpec, ColumnType};
use ldrs_postgres::{
    build_pg_pool, execute_create_table, execute_create_temp_table, execute_merge,
    execute_prepared_stmt, execute_sql, PgCopySink,
};
use tokio_postgres::Client;

use crate::{
    ldrs_env::{get_params_for_stmt_with_default, LdrsExecutionContext},
    postgres::postgres_destination::{PgDestCommand, PgPlan},
};

/// Run one rendered command on the connection.
async fn run_command(
    client: &Client,
    cmd: &PgDestCommand,
    all_params: &[(String, String, Option<ColumnType>)],
    final_cols: &[ColumnSpec],
    context: &LdrsExecutionContext<'_>,
) -> Result<(), anyhow::Error> {
    match cmd {
        PgDestCommand::Sql(sql) => execute_sql(client, &context.render_template(sql)?).await,
        PgDestCommand::Prepared(stmt) => {
            let rendered_sql = context.render_template(&stmt.stmt)?;
            let rendered_key = stmt
                .key
                .as_ref()
                .map(|k| context.render_template(k))
                .transpose()?
                .map(|k| k.to_uppercase());
            let resolved_params =
                get_params_for_stmt_with_default(rendered_key.as_deref(), all_params);
            execute_prepared_stmt(
                client,
                &rendered_sql,
                &resolved_params,
                stmt.types.as_deref(),
            )
            .await
        }
        PgDestCommand::CreateTable(table) => {
            execute_create_table(client, &context.render_template(table)?, final_cols).await
        }
        PgDestCommand::CreateTempTable(table) => {
            execute_create_temp_table(client, &context.render_template(table)?, final_cols).await
        }
        PgDestCommand::Merge(merge_cfg) => {
            let source = context.render_template(&merge_cfg.source)?;
            let target = context.render_template(&merge_cfg.target)?;
            execute_merge(client, &source, &target, &merge_cfg.keys, final_cols).await
        }
        PgDestCommand::Load(_) => unreachable!("Load is the COPY, not a run_command command"),
    }
}

pub struct PgSink {
    copy: PgCopySink,
    after_commands: Vec<PgDestCommand>,
    final_cols: Vec<ColumnSpec>,
}

impl PgSink {
    /// Check out a connection, `BEGIN`, set the role, run the pre-load commands, and
    /// open the binary COPY.
    pub async fn open(
        pg_url: &str,
        role: Option<String>,
        plan: PgPlan,
        final_cols: Vec<ColumnSpec>,
        strategies: Vec<Option<ArrowColumnTransformStrategy>>,
        context: &LdrsExecutionContext<'_>,
        env_params: &[(String, String, Option<ColumnType>)],
    ) -> Result<Self, anyhow::Error> {
        let PgPlan {
            before,
            load_table,
            after,
        } = plan;
        let load_table = context.render_template(&load_table)?;

        let pool = build_pg_pool(pg_url)?;
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
                run_command(&conn, cmd, env_params, &final_cols, context).await?;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(e) = prepared {
            let _ = conn.batch_execute("ROLLBACK").await;
            return Err(e);
        }

        let copy = PgCopySink::open(conn, &load_table, final_cols.clone(), strategies).await?;
        Ok(PgSink {
            copy,
            after_commands: after,
            final_cols,
        })
    }

    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), anyhow::Error> {
        self.copy.write_batch(batch).await
    }

    /// Finish the COPY, run the post-load commands, and `COMMIT`. A post-load failure
    /// rolls back instead.
    pub async fn commit(
        self,
        context: &LdrsExecutionContext<'_>,
        env_params: &[(String, String, Option<ColumnType>)],
    ) -> Result<(), anyhow::Error> {
        let PgSink {
            copy,
            after_commands,
            final_cols,
        } = self;
        let conn = copy.finish().await?;
        let finalized = async {
            for cmd in &after_commands {
                run_command(&conn, cmd, env_params, &final_cols, context).await?;
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
