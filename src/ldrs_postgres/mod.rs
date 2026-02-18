pub mod client;
pub mod postgres_destination;
pub mod postgres_execution;
pub mod schema;
pub mod schema_change;

use crate::arrow_access::extracted_values::ExtractedValue;
use crate::types::ColumnSchema;
use crate::types::ColumnType;
use postgres_types::{to_sql_checked, ToSql, Type};

pub fn map_col_schema_to_pg_type(pq: &ColumnSchema) -> postgres_types::Type {
    match pq {
        ColumnSchema::SmallInt(_) => postgres_types::Type::INT2,
        ColumnSchema::BigInt(_) => postgres_types::Type::INT8,
        ColumnSchema::Boolean(_) => postgres_types::Type::BOOL,
        ColumnSchema::Double(_, _) => postgres_types::Type::FLOAT8,
        ColumnSchema::Integer(_) => postgres_types::Type::INT4,
        ColumnSchema::Jsonb(_) => postgres_types::Type::JSONB,
        ColumnSchema::Numeric(_, _, _) => postgres_types::Type::NUMERIC,
        ColumnSchema::Timestamp(_, _) => postgres_types::Type::TIMESTAMP,
        ColumnSchema::TimestampTz(_, _) => postgres_types::Type::TIMESTAMPTZ,
        ColumnSchema::Date(_) => postgres_types::Type::DATE,
        ColumnSchema::Real(_) => postgres_types::Type::FLOAT4,
        ColumnSchema::Text(_) => postgres_types::Type::TEXT,
        ColumnSchema::Uuid(_) => postgres_types::Type::UUID,
        ColumnSchema::Varchar(_, _) => postgres_types::Type::VARCHAR,
        ColumnSchema::Custom(_, _) => postgres_types::Type::VARCHAR,
        ColumnSchema::FixedSizeBinary(_, _) => postgres_types::Type::BYTEA,
        ColumnSchema::Bytea(_) => postgres_types::Type::BYTEA,
    }
}

impl<'a> ToSql for ExtractedValue<'a> {
    #[inline]
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            ExtractedValue::Boolean(v) => v.to_sql(ty, out),
            ExtractedValue::Int64(v) => v.to_sql(ty, out),
            ExtractedValue::Int32(v) => v.to_sql(ty, out),
            ExtractedValue::Double(v) => v.to_sql(ty, out),
            ExtractedValue::Uuid(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampSeconds(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampMillis(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampMicros(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampNanos(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampTzSeconds(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampTzMillis(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampTzMicros(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampTzNanos(v) => v.to_sql(ty, out),
            ExtractedValue::Real(v) => v.to_sql(ty, out),
            ExtractedValue::Decimal(v) => v.to_sql(ty, out),
            ExtractedValue::Utf8(v) => v.to_sql(ty, out),
            ExtractedValue::Jsonb(v) => v.to_sql(ty, out),
        }
    }

    to_sql_checked!();

    fn accepts(_ty: &Type) -> bool {
        true
    }
}
