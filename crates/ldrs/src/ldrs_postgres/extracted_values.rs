use bytes::BufMut;
use chrono::{DateTime, NaiveDateTime, Utc};
use postgres_types::{to_sql_checked, ToSql, Type};

use crate::{
    arrow_access::TypedColumnAccessor,
    ldrs_postgres::{arrow_bridge::ToPgNumeric, pg_numeric::PgFixedNumeric},
    types::ColumnSpec,
};

#[derive(Debug)]
pub enum ExtractedValue<'a> {
    Boolean(Option<bool>),
    Int64(Option<i64>),
    Int32(Option<i32>),
    Double(Option<f64>),
    Real(Option<f32>),
    Decimal(Option<PgFixedNumeric>),
    Uuid(Option<uuid::Uuid>),
    Utf8(Option<&'a str>),
    TimestampSeconds(Option<NaiveDateTime>),
    TimestampMillis(Option<NaiveDateTime>),
    TimestampMicros(Option<NaiveDateTime>),
    TimestampNanos(Option<NaiveDateTime>),
    TimestampTzSeconds(Option<DateTime<Utc>>),
    TimestampTzMillis(Option<DateTime<Utc>>),
    TimestampTzMicros(Option<DateTime<Utc>>),
    TimestampTzNanos(Option<DateTime<Utc>>),
    Jsonb(Option<&'a str>),
}

#[derive(Debug)]
enum ExtractionStrategy {
    Boolean,
    BigInt,
    Integer,
    Double,
    Real,
    Text,
    Numeric { scale: i32 },
    Uuid,
    Jsonb,
    TimestampSeconds,
    TimestampMillis,
    TimestampMicros,
    TimestampNanos,
    TimestampTzSeconds,
    TimestampTzMillis,
    TimestampTzMicros,
    TimestampTzNanos,
}

#[derive(Debug)]
pub struct ColumnConverter<'a> {
    accessor: &'a TypedColumnAccessor<'a>,
    strategy: ExtractionStrategy,
}

impl<'a> ColumnConverter<'a> {
    pub fn new(
        accessor: &'a TypedColumnAccessor<'a>,
        col_spec: &ColumnSpec,
    ) -> Result<Self, anyhow::Error> {
        let strategy = match col_spec {
            ColumnSpec::Boolean { .. } => ExtractionStrategy::Boolean,
            ColumnSpec::BigInt { .. } => ExtractionStrategy::BigInt,
            ColumnSpec::SmallInt { .. } => ExtractionStrategy::Integer,
            ColumnSpec::Integer { .. } => ExtractionStrategy::Integer,
            ColumnSpec::Double { .. } => ExtractionStrategy::Double,
            ColumnSpec::Real { .. } => ExtractionStrategy::Real,
            ColumnSpec::Text { .. } | ColumnSpec::Varchar { .. } => ExtractionStrategy::Text,
            ColumnSpec::Numeric { scale, .. } => ExtractionStrategy::Numeric { scale: *scale },
            ColumnSpec::Timestamp {
                time_unit: crate::types::TimeUnit::Second,
                ..
            } => ExtractionStrategy::TimestampSeconds,
            ColumnSpec::Timestamp {
                time_unit: crate::types::TimeUnit::Millis,
                ..
            } => ExtractionStrategy::TimestampMillis,
            ColumnSpec::Timestamp {
                time_unit: crate::types::TimeUnit::Micros,
                ..
            } => ExtractionStrategy::TimestampMicros,
            ColumnSpec::Timestamp {
                time_unit: crate::types::TimeUnit::Nanos,
                ..
            } => ExtractionStrategy::TimestampNanos,
            ColumnSpec::TimestampTz {
                time_unit: crate::types::TimeUnit::Second,
                ..
            } => ExtractionStrategy::TimestampTzSeconds,
            ColumnSpec::TimestampTz {
                time_unit: crate::types::TimeUnit::Millis,
                ..
            } => ExtractionStrategy::TimestampTzMillis,
            ColumnSpec::TimestampTz {
                time_unit: crate::types::TimeUnit::Micros,
                ..
            } => ExtractionStrategy::TimestampTzMicros,
            ColumnSpec::TimestampTz {
                time_unit: crate::types::TimeUnit::Nanos,
                ..
            } => ExtractionStrategy::TimestampTzNanos,
            ColumnSpec::Uuid { .. } => ExtractionStrategy::Uuid,
            ColumnSpec::Jsonb { .. } => ExtractionStrategy::Jsonb,
            _ => return Err(anyhow::anyhow!("Unsupported column spec: {:?}", col_spec)),
        };

        Ok(ColumnConverter { accessor, strategy })
    }

    #[inline]
    pub fn extract_value(&self, row_idx: usize) -> ExtractedValue<'_> {
        match &self.strategy {
            ExtractionStrategy::Boolean => unsafe {
                ExtractedValue::Boolean(self.accessor.Boolean(row_idx))
            },
            ExtractionStrategy::BigInt => unsafe {
                ExtractedValue::Int64(self.accessor.Int64(row_idx))
            },
            ExtractionStrategy::Integer => unsafe {
                ExtractedValue::Int32(self.accessor.Int32(row_idx))
            },
            ExtractionStrategy::Double => unsafe {
                ExtractedValue::Double(self.accessor.Float64(row_idx))
            },
            ExtractionStrategy::Real => unsafe {
                ExtractedValue::Real(self.accessor.Float32(row_idx))
            },
            ExtractionStrategy::Text => unsafe {
                ExtractedValue::Utf8(self.accessor.Utf8(row_idx))
            },
            ExtractionStrategy::Numeric { scale } => {
                ExtractedValue::Decimal(self.accessor.as_pg_numeric(row_idx, *scale))
            }
            ExtractionStrategy::Uuid => unsafe {
                ExtractedValue::Uuid(self.accessor.as_uuid(row_idx))
            },
            ExtractionStrategy::Jsonb => unsafe {
                ExtractedValue::Jsonb(self.accessor.Utf8(row_idx))
            },
            ExtractionStrategy::TimestampMillis => {
                ExtractedValue::TimestampMillis(self.accessor.as_chrono_naive(row_idx))
            }
            ExtractionStrategy::TimestampMicros => {
                ExtractedValue::TimestampMicros(self.accessor.as_chrono_naive(row_idx))
            }
            ExtractionStrategy::TimestampNanos => {
                ExtractedValue::TimestampNanos(self.accessor.as_chrono_naive(row_idx))
            }
            ExtractionStrategy::TimestampTzMillis => unsafe {
                ExtractedValue::TimestampTzMillis(self.accessor.as_chrono_tz(row_idx))
            },
            ExtractionStrategy::TimestampTzMicros => unsafe {
                ExtractedValue::TimestampTzMicros(self.accessor.as_chrono_tz(row_idx))
            },
            ExtractionStrategy::TimestampTzNanos => unsafe {
                ExtractedValue::TimestampTzNanos(self.accessor.as_chrono_tz(row_idx))
            },
            _ => panic!("Unsupported conversion strategy: {:?}", self.strategy),
        }
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
