mod arrow_bridge;
pub mod client;
pub mod extracted_values;
pub mod pg_numeric;
pub mod postgres_destination;
pub mod postgres_execution;
pub mod schema;
pub mod schema_change;

use crate::types::ColumnSpec;
use crate::types::ColumnType;

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
