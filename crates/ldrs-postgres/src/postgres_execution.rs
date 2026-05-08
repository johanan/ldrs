use std::{iter::zip, pin::pin, sync::Arc};

use anyhow::Context;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use ldrs_arrow::{
    transform_batch, ArrowColumnTransformStrategy, ColumnSpec, ColumnType, TypedColumnAccessor,
};
use postgres_types::ToSql;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, Transaction};

use crate::{
    extracted_values::{ColumnConverter, ExtractedValue},
    map_colspec_to_pg_type,
};
use futures::{StreamExt, TryStreamExt};

pub fn map_colspec_to_pg_ddl(pq: &ColumnSpec) -> String {
    match pq {
        ColumnSpec::SmallInt { name } => format!("{} smallint", name),
        ColumnSpec::BigInt { name } => format!("{} bigint", name),
        ColumnSpec::Boolean { name } => format!("{} boolean", name),
        ColumnSpec::Double { name } => format!("{} double precision", name),
        ColumnSpec::Integer { name } => format!("{} integer", name),
        ColumnSpec::Jsonb { name } => format!("{} jsonb", name),
        ColumnSpec::Numeric {
            name,
            precision,
            scale,
        } => format!("{} numeric({}, {})", name, precision, scale),
        ColumnSpec::Timestamp { name, .. } => format!("{} timestamp", name),
        ColumnSpec::TimestampTz { name, .. } => format!("{} timestamptz", name),
        ColumnSpec::Date { name } => format!("{} date", name),
        ColumnSpec::Real { name } => format!("{} real", name),
        ColumnSpec::Text { name } => format!("{} text", name),
        ColumnSpec::Uuid { name } => format!("{} uuid", name),
        ColumnSpec::Varchar { name, length } => format!("{} varchar({})", name, length),
        ColumnSpec::Custom { name, ddl_type } => format!("{} {}", name, ddl_type),
        ColumnSpec::Bytea { name } => format!("{} bytea", name),
        ColumnSpec::FixedSizeBinary { name, .. } => format!("{} bytea", name),
    }
}

pub async fn execute_sql<'a>(tx: &Transaction<'a>, sql: &str) -> Result<(), anyhow::Error> {
    tx.batch_execute(sql).await?;
    Ok(())
}

pub async fn execute_prepared_stmt<'a>(
    tx: &Transaction<'a>,
    sql: &str,
    params: &[(String, Option<ColumnType>)],
    stmt_types: Option<&[ColumnType]>,
) -> Result<(), anyhow::Error> {
    // if stmt has types use them, otherwise use from the matched_params
    let param_types = match stmt_types {
        Some(types) => {
            if types.len() != params.len() {
                return Err(anyhow::anyhow!("Mismatched parameter types length. Please make sure the number of types matches the number of parameters."));
            }
            // zip together with the string value and column type
            zip(params.iter().map(|(k, _)| k), types.iter().map(Some)).collect::<Vec<_>>()
        }
        None => params
            .iter()
            .map(|(k, ct)| (k, ct.as_ref()))
            .collect::<Vec<_>>(),
    };

    let param_values = param_types
        .iter()
        .map(param_tosql)
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    let param_refs = param_values.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    tx.execute(sql, &param_refs)
        .await
        .with_context(|| "Failed to execute PostgreSQL prepared statement")?;
    Ok(())
}

pub async fn execute_create_table<'a>(
    tx: &Transaction<'a>,
    table: &str,
    columns: &[ColumnSpec],
) -> Result<(), anyhow::Error> {
    let col_ddl = columns
        .iter()
        .map(map_colspec_to_pg_ddl)
        .collect::<Vec<String>>();
    let mut ddl = format!("CREATE TABLE if not exists {} (", table);
    let fields_ddl = col_ddl.join(", ");
    ddl.push_str(&fields_ddl);
    ddl.push_str(");");
    tx.batch_execute(&ddl).await?;
    Ok(())
}

pub async fn execute_create_temp_table<'a>(
    tx: &Transaction<'a>,
    table: &str,
    columns: &[ColumnSpec],
) -> Result<(), anyhow::Error> {
    let col_ddl = columns
        .iter()
        .map(map_colspec_to_pg_ddl)
        .collect::<Vec<String>>();
    let mut ddl = format!("CREATE TEMP TABLE {} (", table);
    let fields_ddl = col_ddl.join(", ");
    ddl.push_str(&fields_ddl);
    ddl.push_str(");");
    tx.batch_execute(&ddl).await?;
    Ok(())
}

pub async fn execute_merge<'a>(
    tx: &Transaction<'a>,
    source: &str,
    target: &str,
    keys: &[String],
    columns: &[ColumnSpec],
) -> Result<(), anyhow::Error> {
    let on = keys
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
        target, source, on, update, insert, insert_values
    );
    tx.batch_execute(&merge_sql).await?;
    Ok(())
}

pub async fn execute_binary_copy<'a>(
    tx: &Transaction<'a>,
    load_table: &str,
    final_cols: &[ColumnSpec],
    arrow_transforms: &[Option<ArrowColumnTransformStrategy>],
    stream: impl futures::TryStream<Ok = RecordBatch, Error = anyhow::Error> + Send,
) -> Result<(), anyhow::Error> {
    let stream = stream.into_stream();
    let mut stream = pin!(stream);

    let pg_types = final_cols
        .iter()
        .map(map_colspec_to_pg_type)
        .collect::<Vec<_>>();
    let binary_ddl = format!("COPY {} FROM STDIN WITH (FORMAT BINARY)", load_table);
    let sink = tx
        .copy_in(&binary_ddl)
        .await
        .with_context(|| "Could not create binary copy")?;
    let writer = BinaryCopyInWriter::new(sink, &pg_types);
    let mut pinned_writer = pin!(writer);

    // calculate if we need to do record batch transforms
    let transform_plan = if arrow_transforms.iter().any(|s| s.is_some()) {
        let target_schema = Arc::new(Schema::new(
            final_cols
                .iter()
                .map(|col| col.to_arrow_field())
                .collect::<Vec<_>>(),
        ));
        Some((arrow_transforms, target_schema))
    } else {
        None
    };

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;

        let batch = match &transform_plan {
            Some((transforms, target_schema)) => {
                transform_batch(&batch, transforms, target_schema.clone())?
            }
            None => batch,
        };

        let num_rows = batch.num_rows();
        let columns = batch.columns();

        let accessors: Vec<TypedColumnAccessor> =
            columns.iter().map(TypedColumnAccessor::new).collect();

        let converters = accessors
            .iter()
            .zip(final_cols.iter())
            .map(|(accessor, col_schema)| ColumnConverter::new(accessor, col_schema))
            .collect::<Result<Vec<_>, _>>()?;

        let mut batch_buffer = Vec::<ExtractedValue>::with_capacity(batch.columns().len());
        for row_idx in 0..num_rows {
            batch_buffer.clear();
            for converter in converters.iter() {
                batch_buffer.push(converter.extract_value(row_idx));
            }

            pinned_writer
                .as_mut()
                .write_raw(&batch_buffer)
                .await
                .with_context(|| "Failed to write row to PostgreSQL")?;
        }
    }

    pinned_writer
        .finish()
        .await
        .with_context(|| "Could not finish copy")?;
    Ok(())
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
            ColumnType::Double => value
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
    fn test_map_parquet_to_ddl() {
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::SmallInt { name: "id".into() }),
            "id smallint"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::BigInt { name: "id".into() }),
            "id bigint"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Boolean {
                name: "is_active".into()
            }),
            "is_active boolean"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Double {
                name: "price".into()
            }),
            "price double precision"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Integer { name: "age".into() }),
            "age integer"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Jsonb {
                name: "data".into()
            }),
            "data jsonb"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Numeric {
                name: "amount".into(),
                precision: 10,
                scale: 2,
            }),
            "amount numeric(10, 2)"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Timestamp {
                name: "created_at".into(),
                time_unit: ldrs_arrow::TimeUnit::Millis,
            }),
            "created_at timestamp"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::TimestampTz {
                name: "updated_at".into(),
                time_unit: ldrs_arrow::TimeUnit::Micros,
            }),
            "updated_at timestamptz"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Date { name: "dob".into() }),
            "dob date"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Real {
                name: "score".into()
            }),
            "score real"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Text {
                name: "description".into()
            }),
            "description text"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Uuid { name: "id".into() }),
            "id uuid"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Varchar {
                name: "name".into(),
                length: 100
            }),
            "name varchar(100)"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Custom {
                name: "custom".into(),
                ddl_type: "custom_type".into(),
            }),
            "custom custom_type"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Bytea {
                name: "data".into()
            }),
            "data bytea"
        );
    }
}
