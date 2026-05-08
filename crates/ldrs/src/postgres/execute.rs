use std::pin::pin;

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use ldrs_arrow::{ArrowColumnTransformStrategy, ColumnSpec, ColumnType};
use ldrs_postgres::{
    create_connection, execute_binary_copy, execute_create_table, execute_create_temp_table,
    execute_merge, execute_prepared_stmt, execute_sql,
};

use crate::{
    ldrs_env::{get_params_for_stmt_with_default, LdrsExecutionContext},
    postgres::postgres_destination::{validate_execution_plan, PgDestCommand},
};

pub async fn load_to_postgres<S>(
    pg_url: &str,
    commands: &[PgDestCommand],
    final_cols: &[ColumnSpec],
    arrow_transforms: &[Option<ArrowColumnTransformStrategy>],
    all_params: &[(String, String, Option<ColumnType>)],
    role: Option<String>,
    context: &LdrsExecutionContext<'_>,
    stream: S,
) -> Result<(), anyhow::Error>
where
    S: futures::TryStream<Ok = RecordBatch, Error = anyhow::Error> + Send,
{
    // ensure we have exactly one load action
    let _ = validate_execution_plan(commands)?;
    let mut client = create_connection(pg_url).await?;
    let tx = client.transaction().await?;
    if let Some(role) = role {
        if role.contains('"') {
            anyhow::bail!("Role name cannot contain double quotes: {}", role);
        }
        tx.execute(&format!(r#"SET ROLE "{}""#, role), &[]).await?;
    }

    let stream = stream.into_stream();
    let mut stream = pin!(stream);

    for cmd in commands {
        match cmd {
            PgDestCommand::Sql(sql) => {
                let rendered_sql = context.render_template(sql)?;
                execute_sql(&tx, &rendered_sql).await?;
            }
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
                execute_prepared_stmt(&tx, &rendered_sql, &resolved_params, stmt.types.as_deref())
                    .await?;
            }
            PgDestCommand::CreateTable(table) => {
                let rendered_table = context.render_template(table)?;
                execute_create_table(&tx, &rendered_table, final_cols).await?;
            }
            PgDestCommand::CreateTempTable(table) => {
                let rendered_table = context.render_template(table)?;
                execute_create_temp_table(&tx, &rendered_table, final_cols).await?;
            }
            PgDestCommand::Merge(merge_cfg) => {
                let source = context.render_template(&merge_cfg.source)?;
                let target = context.render_template(&merge_cfg.target)?;
                execute_merge(&tx, &source, &target, &merge_cfg.keys, final_cols).await?;
            }
            PgDestCommand::Load(load_table) => {
                let rendered_table = context.render_template(load_table)?;
                execute_binary_copy(
                    &tx,
                    &rendered_table,
                    final_cols,
                    arrow_transforms,
                    stream.as_mut(),
                )
                .await?;
            }
        }
    }

    tx.commit().await?;

    Ok(())
}
