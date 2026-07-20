use anyhow::Context;
use ldrs_arrow::{ColumnSpec, ColumnType};
use postgres_types::ToSql;
use tokio_postgres::Client;

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

pub async fn execute_sql(client: &Client, sql: &str) -> Result<(), anyhow::Error> {
    client.batch_execute(sql).await?;
    Ok(())
}

pub async fn execute_prepared_stmt(
    client: &Client,
    sql: &str,
    params: &[(String, String)],
    cols: &[ColumnSpec],
) -> Result<(), anyhow::Error> {
    // Type each bind from the target columns by name. The table's columns are the resolved
    // schema, so a bound key is present by definition; an unmatched name has no type and binds
    // as text (its string value passes through unchanged).
    let typed: Vec<(&String, Option<ColumnType>)> = params
        .iter()
        .map(|(name, value)| {
            let ty = cols
                .iter()
                .find(|c| c.name().eq_ignore_ascii_case(name))
                .map(ColumnType::from);
            (value, ty)
        })
        .collect();
    let param_types = typed
        .iter()
        .map(|(value, ty)| (*value, ty.as_ref()))
        .collect::<Vec<_>>();

    let param_values = param_types
        .iter()
        .map(param_tosql)
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    let param_refs = param_values.iter().map(|v| v.as_ref()).collect::<Vec<_>>();
    client
        .execute(sql, &param_refs)
        .await
        .with_context(|| "Failed to execute PostgreSQL prepared statement")?;
    Ok(())
}

pub async fn execute_create_table(
    client: &Client,
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
    client.batch_execute(&ddl).await?;
    Ok(())
}

pub async fn execute_create_temp_table(
    client: &Client,
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
    client.batch_execute(&ddl).await?;
    Ok(())
}

pub async fn execute_merge(
    client: &Client,
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
    client.batch_execute(&merge_sql).await?;
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
