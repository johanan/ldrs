use chrono::{DateTime, NaiveDateTime, Utc};
use pg_bigdecimal::PgNumeric;
use serde_json::Value;

use crate::{arrow_access::TypedColumnAccessor, types::ColumnSchema};

#[derive(Debug)]
pub enum ExtractedValue<'a> {
    Boolean(Option<bool>),
    Int64(Option<i64>),
    Int32(Option<i32>),
    Double(Option<f64>),
    Real(Option<f32>),
    Decimal(Option<PgNumeric>),
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
    Jsonb(Option<Value>),
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
        col_schema: &ColumnSchema,
    ) -> Result<Self, anyhow::Error> {
        let strategy = match col_schema {
            ColumnSchema::Boolean(_) => ExtractionStrategy::Boolean,
            ColumnSchema::BigInt(_) => ExtractionStrategy::BigInt,
            ColumnSchema::Integer(_) => ExtractionStrategy::Integer,
            ColumnSchema::Double(_, _) => ExtractionStrategy::Double,
            ColumnSchema::Text(_) | ColumnSchema::Varchar(_, _) => ExtractionStrategy::Text,
            ColumnSchema::Numeric(_, _, scale) => ExtractionStrategy::Numeric { scale: *scale },
            ColumnSchema::Timestamp(_, crate::types::TimeUnit::Millis) => {
                ExtractionStrategy::TimestampMillis
            }
            ColumnSchema::Timestamp(_, crate::types::TimeUnit::Micros) => {
                ExtractionStrategy::TimestampMicros
            }
            ColumnSchema::Timestamp(_, crate::types::TimeUnit::Nanos) => {
                ExtractionStrategy::TimestampNanos
            }
            ColumnSchema::TimestampTz(_, crate::types::TimeUnit::Millis) => {
                ExtractionStrategy::TimestampTzMillis
            }
            ColumnSchema::TimestampTz(_, crate::types::TimeUnit::Micros) => {
                ExtractionStrategy::TimestampTzMicros
            }
            ColumnSchema::TimestampTz(_, crate::types::TimeUnit::Nanos) => {
                ExtractionStrategy::TimestampTzNanos
            }
            ColumnSchema::Uuid(_) => ExtractionStrategy::Uuid,
            ColumnSchema::Jsonb(_) => ExtractionStrategy::Jsonb,
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported column schema: {:?}",
                    col_schema
                ))
            }
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
            ExtractionStrategy::Text => unsafe {
                ExtractedValue::Utf8(self.accessor.Utf8(row_idx))
            },
            ExtractionStrategy::Numeric { scale } => {
                ExtractedValue::Decimal(self.accessor.as_pg_numeric(row_idx, *scale))
            }
            ExtractionStrategy::Uuid => unsafe {
                ExtractedValue::Uuid(self.accessor.as_uuid(row_idx))
            },
            ExtractionStrategy::Jsonb => ExtractedValue::Jsonb(self.accessor.as_serde(row_idx)),
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
