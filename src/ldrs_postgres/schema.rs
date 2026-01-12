use tokio_postgres::{Client, Row};

use crate::types::{ColumnSchema, TimeUnit};

#[derive(Debug)]
pub struct PostgresColumnRaw {
    column_name: String,
    pg_type: String,
    type_modifier: i32,
    not_null: bool,
    att_num: i16,
}

impl TryFrom<i32> for TimeUnit {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        // https://www.postgresql.org/docs/17/datatype-datetime.html
        match value {
            6 => Ok(TimeUnit::Micros),
            3 => Ok(TimeUnit::Millis),
            // -1 is the default so microseconds
            -1 => Ok(TimeUnit::Micros),
            _ => Err(anyhow::anyhow!("Unsupported time unit size: {}", value)),
        }
    }
}

impl TryFrom<&Row> for PostgresColumnRaw {
    type Error = anyhow::Error;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        let column_name: &str = row.get(0);
        let pg_type: &str = row.get(1);
        let type_modifier: i32 = row.get(2);
        let not_null: bool = row.get(3);
        let att_num: i16 = row.get(4);

        Ok(PostgresColumnRaw {
            column_name: column_name.to_string(),
            pg_type: pg_type.to_string(),
            type_modifier,
            not_null,
            att_num,
        })
    }
}

impl<'a> TryFrom<&'a PostgresColumnRaw> for ColumnSchema<'a> {
    type Error = anyhow::Error;

    fn try_from(raw: &'a PostgresColumnRaw) -> Result<Self, Self::Error> {
        let col_name = raw.column_name.as_str();
        let col = match raw.pg_type.as_str() {
            "bool" => Ok(ColumnSchema::Boolean(col_name)),
            "int2" => Ok(ColumnSchema::SmallInt(col_name)),
            "int4" => Ok(ColumnSchema::Integer(col_name)),
            "int8" => Ok(ColumnSchema::BigInt(col_name)),
            "float4" => Ok(ColumnSchema::Real(col_name)),
            "float8" => Ok(ColumnSchema::Double(col_name, None)),
            "numeric" => {
                let precision = ((raw.type_modifier - 4) >> 16) & 65535;
                let scale = (raw.type_modifier - 4) & 65535;
                Ok(ColumnSchema::Numeric(col_name, precision, scale))
            }
            "bpchar" => {
                let length = raw.type_modifier - 4;
                // TBD determine if we need a char type
                Ok(ColumnSchema::Varchar(col_name, length))
            }
            "text" => Ok(ColumnSchema::Text(col_name)),
            "varchar" => {
                let length = raw.type_modifier - 4;
                Ok(ColumnSchema::Varchar(col_name, length))
            }
            "json" | "jsonb" | "_text" => Ok(ColumnSchema::Jsonb(col_name)),
            "timestamp" => {
                let time_unit = TimeUnit::try_from(raw.type_modifier)?;
                Ok(ColumnSchema::Timestamp(col_name, time_unit))
            }
            "timestamptz" => {
                let time_unit = TimeUnit::try_from(raw.type_modifier)?;
                Ok(ColumnSchema::TimestampTz(col_name, time_unit))
            }
            "uuid" => Ok(ColumnSchema::Uuid(col_name)),
            _ => Err(anyhow::anyhow!("Unsupported type: {}", raw.pg_type)),
        };
        col
    }
}

pub async fn query_column(
    conn: &Client,
    table_name: String,
) -> Result<Vec<PostgresColumnRaw>, anyhow::Error> {
    let query = r#"
        SELECT
            a.attname AS column_name,
            t.typname AS pg_type,  -- Raw PG type name like 'varchar', 'timestamptz'
            a.atttypmod AS type_modifier,  -- Type modifier (length info)
            a.attnotnull AS not_null,
            a.attnum AS att_num
           FROM pg_attribute a
        JOIN pg_type t ON a.atttypid = t.oid
        WHERE a.attrelid = $1::text::regclass
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum;
    "#;

    let column_result = conn.query(query, &[&table_name]).await?;
    column_result
        .into_iter()
        .map(|row| PostgresColumnRaw::try_from(&row))
        .collect::<Result<Vec<PostgresColumnRaw>, anyhow::Error>>()
}

pub async fn query_column_safe(conn: &Client, table_name: String) -> Vec<PostgresColumnRaw> {
    let cols = query_column(conn, table_name).await;
    match cols {
        Ok(cols) => cols,
        Err(_) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use crate::ldrs_postgres::client::create_connection;

    use super::*;

    #[tokio::test]
    async fn test_query_column() {
        let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
        let conn = create_connection(pg_url).await.unwrap();
        let table_name = "test_schema.strings";
        let columns = query_column(&conn, table_name.to_string()).await.unwrap();
        let col_schema = columns
            .iter()
            .map(|col| ColumnSchema::try_from(col))
            .collect::<Result<Vec<ColumnSchema>, anyhow::Error>>()
            .unwrap();
        assert!(col_schema.len() == 6);
        assert!(col_schema[0] == ColumnSchema::Varchar("char_value", 10));
        assert!(col_schema[1] == ColumnSchema::Varchar("varchar_value", 10));
        assert!(col_schema[2] == ColumnSchema::Text("text_value"));
        assert!(col_schema[3] == ColumnSchema::Jsonb("json_value"));
        assert!(col_schema[4] == ColumnSchema::Jsonb("jsonb_value"));
        assert!(col_schema[5] == ColumnSchema::Jsonb("array_value"));

        // query for numbers table
        let table_name = "test_schema.numbers";
        let columns = query_column(&conn, table_name.to_string()).await.unwrap();
        let col_schema = columns
            .iter()
            .map(|col| ColumnSchema::try_from(col))
            .collect::<Result<Vec<ColumnSchema>, anyhow::Error>>()
            .unwrap();
        assert!(col_schema.len() == 6);
        assert!(col_schema[0] == ColumnSchema::SmallInt("smallint_value"));
        assert!(col_schema[1] == ColumnSchema::Integer("integer_value"));
        assert!(col_schema[2] == ColumnSchema::BigInt("bigint_value"));
        assert!(col_schema[3] == ColumnSchema::Numeric("decimal_value", 38, 15));
        assert!(col_schema[4] == ColumnSchema::Double("double_value", None));
        assert!(col_schema[5] == ColumnSchema::Real("float_value"));

        // query for the users table
        let table_name = "test_schema.users";
        let columns = query_column(&conn, table_name.to_string()).await.unwrap();
        let col_schema = columns
            .iter()
            .map(|col| ColumnSchema::try_from(col))
            .collect::<Result<Vec<ColumnSchema>, anyhow::Error>>()
            .unwrap();
        assert!(col_schema.len() == 6);
        assert!(col_schema[0] == ColumnSchema::Varchar("name", 100));
        assert!(col_schema[1] == ColumnSchema::Timestamp("created", TimeUnit::Micros));
        assert!(col_schema[2] == ColumnSchema::TimestampTz("createdz", TimeUnit::Micros));
        assert!(col_schema[3] == ColumnSchema::Uuid("unique_id"));
        assert!(col_schema[4] == ColumnSchema::Uuid("nullable_id"));
        assert!(col_schema[5] == ColumnSchema::Boolean("active"));
    }
}
