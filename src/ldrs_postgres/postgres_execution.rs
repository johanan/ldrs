use std::{iter::zip, pin::pin};

use anyhow::Context;
use arrow_array::RecordBatch;
use handlebars::Handlebars;
use postgres_types::ToSql;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, Transaction};
use tracing::{debug, warn};

use crate::{
    arrow_access::{
        extracted_values::{ColumnConverter, ExtractedValue},
        TypedColumnAccessor,
    },
    ldrs_postgres::{
        client::create_connection, map_col_schema_to_pg_type,
        schema_change::map_columnschema_to_pg_ddl,
    },
    types::{ColumnSchema, ColumnType},
};
use futures::{StreamExt, TryStreamExt};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgMergeConfig {
    pub target: String,
    pub source: Option<String>,
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PgAction {
    CreateTable(String),
    CreateTempTable(String),
    Merge(PgMergeConfig),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgPreparedStmt {
    pub stmt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub types: Option<Vec<ColumnType>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PgDestCommand {
    #[serde(rename = "action")]
    Action(PgAction),
    #[serde(rename = "load")]
    Load(String),
    #[serde(rename = "stmt")]
    Prepared(PgPreparedStmt),
    Sql(String),
}

#[derive(Debug)]
pub struct PgDestExecutionContext<'a> {
    context: Value,
    handlebars: handlebars::Handlebars<'a>,
}

impl<'a> PgDestExecutionContext<'a> {
    pub fn try_new(
        name: &'a str,
        handlebars: handlebars::Handlebars<'a>,
    ) -> Result<Self, anyhow::Error> {
        let fqtn_tup = name.split_once('.');
        let (schema, table) = match fqtn_tup {
            Some((schema, table)) => Ok((schema, table)),
            None => Err(anyhow::Error::msg("Invalid table name")),
        }?;

        // create random load_table name
        let load_table_name = format!(
            "{}_{}",
            table,
            uuid::Uuid::new_v4().to_string().replace('-', "")
        );
        let load_table = format!("{}.{}", schema, load_table_name);

        let context = serde_json::json!({
            "name": name,
            "schema": schema,
            "table": table,
            "load_table": load_table,
            "load_table_name": load_table_name
        });
        Ok(Self {
            context,
            handlebars,
        })
    }
}

const SIMPLE_TYPES: &[&str] = &[
    "_UUID",
    "_INT",
    "_BIGINT",
    "_SMALLINT",
    "_BOOL",
    "_TIMESTAMP",
    "_TIMESTAMPTZ",
    "_TEXT",
    "_VARCHAR",
    "_REAL",
    "_DOUBLE",
    "_DATE",
    "_JSONB",
    "_BYTEA",
];

pub fn collect_params(params: &[(String, String)]) -> Vec<(String, String, Option<ColumnType>)> {
    let mut ldrs_params = params
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix("LDRS_PARAM_").map(|suffix| {
                for simple_type in SIMPLE_TYPES {
                    if let Some(key) = suffix.strip_suffix(simple_type) {
                        let simple_type_rest = &simple_type[1..];
                        // at least output if something failed
                        let column_type = ColumnType::try_from(simple_type_rest)
                            .map_err(|e| warn!("Failed to parse column type: {}", e))
                            .ok();
                        return (key.to_string(), value.to_string(), column_type);
                    }
                }
                (suffix.to_string(), value.to_string(), None)
            })
        })
        .collect::<Vec<_>>();
    // ensure that the params are sorted by key
    ldrs_params.sort_by(|a, b| a.0.cmp(&b.0));
    ldrs_params
}

fn get_params_for_stmt<'a>(
    key: &Option<String>,
    params: &'a [(String, String, Option<ColumnType>)],
) -> Vec<(&'a String, Option<&'a ColumnType>)> {
    // match the key if it exists or match anything without a key
    let key_params: Vec<(&'a String, Option<&'a ColumnType>)> = match key {
        Some(k) => {
            let prefix = format!("{}_", k);
            params
                .iter()
                .filter(|(key, _, _)| key.starts_with(&prefix))
                .map(|(_, value, param_type)| (value, param_type.as_ref()))
                .collect::<Vec<_>>()
        }
        None => params
            .iter()
            .filter(|(key, _, _)| !key.contains('_'))
            .map(|(_, value, param_type)| (value, param_type.as_ref()))
            .collect::<Vec<_>>(),
    };

    // if there are no params and a key fallback to the defaults
    match key_params.is_empty() && key.is_some() {
        true => params
            .iter()
            .filter(|(key, _, _)| !key.contains('_'))
            .map(|(_, value, param_type)| (value, param_type.as_ref()))
            .collect::<Vec<_>>(),
        false => key_params,
    }
}

fn validate_execution_plan(commands: &[PgDestCommand]) -> Result<(), anyhow::Error> {
    let load_actions = commands.iter().filter_map(|cmd| match cmd {
        PgDestCommand::Load(action) => Some(action),
        _ => None,
    });

    if load_actions.count() != 1 {
        Err(anyhow::Error::msg(
            "Execution plan must contain exactly one Load",
        ))
    } else {
        Ok(())
    }
}

pub async fn load_to_postgres<S>(
    pg_url: &str,
    name: &str,
    commands: &[PgDestCommand],
    final_cols: &[ColumnSchema<'_>],
    transforms: &[Option<ColumnType>],
    all_params: &[(String, String, Option<ColumnType>)],
    role: Option<String>,
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
    let handlebars = Handlebars::new();
    let context = PgDestExecutionContext::try_new(name, handlebars)?;

    let stream = stream.into_stream();
    let mut stream = pin!(stream);

    for cmd in commands {
        match cmd {
            PgDestCommand::Sql(sql) => {
                execute_sql(&tx, sql, &context).await?;
            }
            PgDestCommand::Prepared(stmt) => {
                execute_prepared_stmt(&tx, stmt, all_params, &context).await?;
            }
            PgDestCommand::Action(action) => {
                execute_action(&tx, action, final_cols, &context).await?;
            }
            PgDestCommand::Load(load_table) => {
                // set it all up to write to
                let handled_table = context
                    .handlebars
                    .render_template(load_table, &context.context)
                    .unwrap();
                let pg_types = final_cols
                    .iter()
                    .map(map_col_schema_to_pg_type)
                    .collect::<Vec<_>>();
                let binary_ddl = format!("COPY {} FROM STDIN WITH (FORMAT BINARY)", handled_table);
                let sink = tx
                    .copy_in(&binary_ddl)
                    .await
                    .with_context(|| "Could not create binary copy")?;
                let writer = BinaryCopyInWriter::new(sink, &pg_types);
                let mut pinned_writer = pin!(writer);

                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result.unwrap();
                    let num_rows = batch.num_rows();
                    let columns = batch.columns();

                    let accessors: Vec<TypedColumnAccessor> =
                        columns.iter().map(TypedColumnAccessor::new).collect();

                    let converters = accessors
                        .iter()
                        .zip(final_cols.iter().zip(transforms.iter()))
                        .map(|(accessor, (col_schema, transform))| {
                            ColumnConverter::new(accessor, col_schema, transform.as_ref())
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();

                    let mut batch_buffer = Vec::<Vec<ExtractedValue>>::with_capacity(num_rows);
                    for row_idx in 0..num_rows {
                        let row_values: Vec<ExtractedValue> = converters
                            .iter()
                            .map(|converter| converter.extract_value(row_idx))
                            .collect();

                        batch_buffer.push(row_values);
                    }

                    for row in batch_buffer.iter() {
                        pinned_writer
                            .as_mut()
                            .write_raw(row)
                            .await
                            .with_context(|| "Failed to write row to PostgreSQL")
                            .unwrap();
                    }
                }

                pinned_writer
                    .finish()
                    .await
                    .with_context(|| "Could not finish copy")
                    .unwrap();
            }
        }
    }

    tx.commit().await?;

    Ok(())
}

pub async fn execute_sql<'a>(
    tx: &Transaction<'a>,
    sql: &str,
    context: &PgDestExecutionContext<'a>,
) -> Result<(), anyhow::Error> {
    let rendered_sql = context.handlebars.render_template(&sql, &context.context)?;
    debug!("Executing SQL: {}", rendered_sql);
    tx.batch_execute(&rendered_sql).await?;
    Ok(())
}

pub async fn execute_prepared_stmt<'a>(
    tx: &Transaction<'a>,
    stmt: &PgPreparedStmt,
    params: &[(String, String, Option<ColumnType>)],
    context: &PgDestExecutionContext<'a>,
) -> Result<(), anyhow::Error> {
    let rendered_stmt = context
        .handlebars
        .render_template(&stmt.stmt, &context.context)?;
    debug!("Executing prepared statement: {}", rendered_stmt);

    let matched_params = get_params_for_stmt(&stmt.key, params);
    // if stmt has types use them, otherwise use from the matched_params
    let param_types = match &stmt.types {
        Some(types) => {
            if types.len() != matched_params.len() {
                return Err(anyhow::anyhow!("Mismatched parameter types length. Please make sure the number of types matches the number of parameters."));
            }
            // zip together with the string value and column type
            zip(
                matched_params.iter().map(|(k, _)| *k),
                types.iter().map(Some),
            )
            .collect::<Vec<_>>()
        }
        None => matched_params,
    };

    let param_values = param_types
        .iter()
        .map(param_tosql)
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    let param_refs = param_values.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    tx.execute(&rendered_stmt, &param_refs).await?;
    Ok(())
}

pub async fn execute_action<'a>(
    tx: &Transaction<'a>,
    action: &PgAction,
    columns: &[ColumnSchema<'a>],
    context: &PgDestExecutionContext<'a>,
) -> Result<(), anyhow::Error> {
    match action {
        PgAction::CreateTable(table) => {
            let handled_table = context
                .handlebars
                .render_template(&table, &context.context)?;
            let col_ddl = columns
                .iter()
                .map(|col| map_columnschema_to_pg_ddl(col))
                .collect::<Vec<String>>();
            let mut ddl = format!("CREATE TABLE if not exists {} (", handled_table);
            let fields_ddl = col_ddl.join(", ");
            ddl.push_str(&fields_ddl);
            ddl.push_str(");");
            tx.batch_execute(&ddl).await?;
            Ok(())
        }
        PgAction::CreateTempTable(table) => {
            let handled_table = context
                .handlebars
                .render_template(&table, &context.context)?;
            let col_ddl = columns
                .iter()
                .map(|col| map_columnschema_to_pg_ddl(col))
                .collect::<Vec<String>>();
            let mut ddl = format!("CREATE TEMP TABLE {} (", handled_table);
            let fields_ddl = col_ddl.join(", ");
            ddl.push_str(&fields_ddl);
            ddl.push_str(");");
            tx.batch_execute(&ddl).await?;
            Ok(())
        }
        PgAction::Merge(merge) => {
            let handled_target = context
                .handlebars
                .render_template(&merge.target, &context.context)?;
            let handled_source = context.handlebars.render_template(
                merge
                    .source
                    .as_ref()
                    .map(|s| s.as_str())
                    .unwrap_or_else(|| "{{ load_table }}"),
                &context.context,
            )?;
            let on = merge
                .keys
                .iter()
                .map(|key| format!("target.{} = source.{}", key, key))
                .collect::<Vec<_>>()
                .join(" and ");
            let update = columns
                .iter()
                .map(|col| format!("{} = source.{}", col.name(), col.name()))
                .collect::<Vec<_>>()
                .join(", ");
            let insert = columns
                .iter()
                .map(|col| col.name())
                .collect::<Vec<_>>()
                .join(", ");
            let insert_values = columns
                .iter()
                .map(|col| format!("source.{}", col.name()))
                .collect::<Vec<_>>()
                .join(", ");
            let merge_sql = format!(
                r#"
                MERGE INTO {} target
                USING {} as source
                ON {}
                WHEN MATCHED THEN UPDATE SET {}
                WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});
            "#,
                handled_target, handled_source, on, update, insert, insert_values
            );
            debug!("Executing merge SQL: {}", merge_sql);
            tx.batch_execute(&merge_sql).await?;
            Ok(())
        }
    }
}

fn param_tosql<'a>(
    param: &'a (&'a String, Option<&ColumnType>),
) -> Result<Box<dyn ToSql + Sync + 'a>, anyhow::Error> {
    match param {
        (value, Some(ct)) => match ct {
            ColumnType::SmallInt => value
                .parse::<i16>()
                .map(|v| Box::new(v) as Box<dyn ToSql + Sync>)
                .map_err(|e| anyhow::anyhow!("Failed to parse SmallInt: {}", e)),
            ColumnType::BigInt => value
                .parse::<i64>()
                .map(|v| Box::new(v) as Box<dyn ToSql + Sync>)
                .map_err(|e| anyhow::anyhow!("Failed to parse BigInt: {}", e)),
            ColumnType::Integer => value
                .parse::<i32>()
                .map(|v| Box::new(v) as Box<dyn ToSql + Sync>)
                .map_err(|e| anyhow::anyhow!("Failed to parse Int: {}", e)),
            ColumnType::Double(_) => value
                .parse::<f64>()
                .map(|v| Box::new(v) as Box<dyn ToSql + Sync>)
                .map_err(|e| anyhow::anyhow!("Failed to parse Float: {}", e)),
            ColumnType::Boolean => value
                .parse::<bool>()
                .map(|v| Box::new(v) as Box<dyn ToSql + Sync>)
                .map_err(|e| anyhow::anyhow!("Failed to parse Boolean: {}", e)),
            ColumnType::TimestampTz(_) => value
                .parse::<chrono::DateTime<chrono::Utc>>()
                .map(|v| Box::new(v) as Box<dyn ToSql + Sync>)
                .map_err(|e| anyhow::anyhow!("Failed to parse TimestampTz: {}", e)),
            ColumnType::Timestamp(_) => value
                .parse::<chrono::NaiveDateTime>()
                .map(|v| Box::new(v) as Box<dyn ToSql + Sync>)
                .map_err(|e| anyhow::anyhow!("Failed to parse Timestamp: {}", e)),
            ColumnType::Uuid => value
                .parse::<uuid::Uuid>()
                .map(|v| Box::new(v) as Box<dyn ToSql + Sync>)
                .map_err(|e| anyhow::anyhow!("Failed to parse Uuid: {}", e)),
            _ => Ok(Box::new(value)),
        },
        (value, None) => Ok(Box::new(value)),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_collect_params() {
        let simple_env = vec![
            ("LDRS_PARAM_P1".to_string(), "value1".to_string()),
            ("LDRS_PARAM_P2".to_string(), "value2".to_string()),
        ];
        let params = collect_params(&simple_env);
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], ("P1".to_string(), "value1".to_string(), None));

        let with_type = vec![
            ("LDRS_PARAM_P1_UUID".to_string(), "value1".to_string()),
            (
                "LDRS_PARAM_P2_TIMESTAMP".to_string(),
                "2023-01-01T00:00:00Z".to_string(),
            ),
        ];
        let params = collect_params(&with_type);
        assert_eq!(params.len(), 2);
        assert_eq!(
            params[0],
            (
                "P1".to_string(),
                "value1".to_string(),
                Some(ColumnType::Uuid)
            )
        );
        assert_eq!(
            params[1],
            (
                "P2".to_string(),
                "2023-01-01T00:00:00Z".to_string(),
                Some(ColumnType::Timestamp(crate::types::TimeUnit::Micros))
            )
        );
    }
}
