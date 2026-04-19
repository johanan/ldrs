pub mod client;
pub mod pg_numeric;
pub mod postgres_destination;
pub mod postgres_execution;
pub mod schema;
pub mod schema_change;

use crate::arrow_access::extracted_values::ExtractedValue;
use crate::types::ColumnSpec;
use crate::types::ColumnType;
use bytes::BufMut;
use postgres_types::{to_sql_checked, ToSql, Type};

pub fn map_colspec_to_pg_type(pq: &ColumnSpec) -> postgres_types::Type {
    match pq {
        ColumnSpec::SmallInt { .. } => postgres_types::Type::INT2,
        ColumnSpec::BigInt { .. } => postgres_types::Type::INT8,
        ColumnSpec::Boolean { .. } => postgres_types::Type::BOOL,
        ColumnSpec::Double { .. } => postgres_types::Type::FLOAT8,
        ColumnSpec::Integer { .. } => postgres_types::Type::INT4,
        ColumnSpec::Jsonb { .. } => postgres_types::Type::JSONB,
        ColumnSpec::Numeric { .. } => postgres_types::Type::NUMERIC,
        ColumnSpec::Timestamp { .. } => postgres_types::Type::TIMESTAMP,
        ColumnSpec::TimestampTz { .. } => postgres_types::Type::TIMESTAMPTZ,
        ColumnSpec::Date { .. } => postgres_types::Type::DATE,
        ColumnSpec::Real { .. } => postgres_types::Type::FLOAT4,
        ColumnSpec::Text { .. } => postgres_types::Type::TEXT,
        ColumnSpec::Uuid { .. } => postgres_types::Type::UUID,
        ColumnSpec::Varchar { .. } => postgres_types::Type::VARCHAR,
        ColumnSpec::Custom { .. } => postgres_types::Type::VARCHAR,
        ColumnSpec::FixedSizeBinary { .. } => postgres_types::Type::BYTEA,
        ColumnSpec::Bytea { .. } => postgres_types::Type::BYTEA,
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
            ExtractedValue::Jsonb(v) => match v {
                None => Ok(postgres_types::IsNull::Yes),
                Some(s) => {
                    out.put_u8(0x01);
                    out.put_slice(s.as_bytes());
                    Ok(postgres_types::IsNull::No)
                }
            },
        }
    }

    to_sql_checked!();

    fn accepts(_ty: &Type) -> bool {
        true
    }
}
