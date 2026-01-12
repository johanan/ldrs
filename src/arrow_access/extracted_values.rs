use chrono::{DateTime, NaiveDateTime, Utc};
use pg_bigdecimal::PgNumeric;
use serde_json::Value;

use crate::{
    arrow_access::TypedColumnAccessor,
    types::{ColumnSchema, ColumnType},
};

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
    TimestampMillis(Option<NaiveDateTime>),
    TimestampMicros(Option<NaiveDateTime>),
    TimestampNanos(Option<NaiveDateTime>),
    TimestampTzMillis(Option<DateTime<Utc>>),
    TimestampTzMicros(Option<DateTime<Utc>>),
    TimestampTzNanos(Option<DateTime<Utc>>),
    Jsonb(Option<Value>),
}

#[derive(Debug)]
enum ConversionStrategy {
    Boolean,
    BigInt,
    Integer,
    Double { scale: Option<i32> },
    Real,
    Text,
    Numeric { precision: i32, scale: i32 },
    TimestampMillis,
    TimestampMicros,
    TimestampNanos,
    TimestampTzMillis,
    TimestampTzMicros,
    TimestampTzNanos,
    TransformBigInt,
    TransformInteger,
    TransformUuid,
    TransformJsonb,
    TransformNumeric { precision: i32, scale: i32 },
    TransformDouble { scale: Option<i32> },
}

#[derive(Debug)]
pub struct ColumnConverter<'a> {
    accessor: &'a TypedColumnAccessor<'a>,
    strategy: ConversionStrategy,
}

impl<'a> ColumnConverter<'a> {
    pub fn new(
        accessor: &'a TypedColumnAccessor<'a>,
        col_schema: &ColumnSchema,
        transform: Option<&ColumnType>,
    ) -> Result<Self, anyhow::Error> {
        let strategy = match transform {
            // all of these have an output type that is not the same as the arrow array type
            Some(ColumnType::BigInt) => ConversionStrategy::TransformBigInt,
            Some(ColumnType::Integer) => ConversionStrategy::TransformInteger,
            // I know doubles do not have scale, but this could be a different type underneath
            Some(ColumnType::Double(precision)) => {
                ConversionStrategy::TransformDouble { scale: *precision }
            }
            Some(ColumnType::Numeric(precision, scale)) => ConversionStrategy::TransformNumeric {
                precision: *precision,
                scale: *scale,
            },
            Some(ColumnType::Uuid) => ConversionStrategy::TransformUuid,
            Some(ColumnType::Jsonb) => ConversionStrategy::TransformJsonb,
            Some(ColumnType::TimestampTz(time_unit)) => match time_unit {
                crate::types::TimeUnit::Millis => ConversionStrategy::TimestampTzMillis,
                crate::types::TimeUnit::Micros => ConversionStrategy::TimestampTzMicros,
                crate::types::TimeUnit::Nanos => ConversionStrategy::TimestampTzNanos,
            },
            None => match col_schema {
                // these all have the same output as arrow array type
                ColumnSchema::Boolean(_) => ConversionStrategy::Boolean,
                ColumnSchema::BigInt(_) => ConversionStrategy::BigInt,
                ColumnSchema::Integer(_) => ConversionStrategy::Integer,
                ColumnSchema::Double(_, scale) => ConversionStrategy::Double {
                    scale: scale.clone(),
                },
                ColumnSchema::Text(_) | ColumnSchema::Varchar(_, _) => ConversionStrategy::Text,
                ColumnSchema::Numeric(_, precision, scale) => ConversionStrategy::Numeric {
                    precision: *precision,
                    scale: *scale,
                },
                ColumnSchema::Timestamp(_, crate::types::TimeUnit::Millis) => {
                    ConversionStrategy::TimestampMillis
                }
                ColumnSchema::Timestamp(_, crate::types::TimeUnit::Micros) => {
                    ConversionStrategy::TimestampMicros
                }
                ColumnSchema::Timestamp(_, crate::types::TimeUnit::Nanos) => {
                    ConversionStrategy::TimestampNanos
                }
                ColumnSchema::TimestampTz(_, crate::types::TimeUnit::Millis) => {
                    ConversionStrategy::TimestampTzMillis
                }
                ColumnSchema::TimestampTz(_, crate::types::TimeUnit::Micros) => {
                    ConversionStrategy::TimestampTzMicros
                }
                ColumnSchema::TimestampTz(_, crate::types::TimeUnit::Nanos) => {
                    ConversionStrategy::TimestampTzNanos
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported column schema: {:?}",
                        col_schema
                    ))
                }
            },
            _ => return Err(anyhow::anyhow!("Unsupported transform: {:?}", transform)),
        };

        Ok(ColumnConverter { accessor, strategy })
    }

    #[inline]
    pub fn extract_value(&self, row_idx: usize) -> ExtractedValue<'_> {
        match &self.strategy {
            ConversionStrategy::Boolean => unsafe {
                ExtractedValue::Boolean(self.accessor.Boolean(row_idx))
            },
            ConversionStrategy::BigInt => unsafe {
                ExtractedValue::Int64(self.accessor.Int64(row_idx))
            },
            ConversionStrategy::Integer => unsafe {
                ExtractedValue::Int32(self.accessor.Int32(row_idx))
            },
            ConversionStrategy::Double { scale: _ } => unsafe {
                ExtractedValue::Double(self.accessor.Float64(row_idx))
            },
            ConversionStrategy::Text => unsafe {
                ExtractedValue::Utf8(self.accessor.Utf8(row_idx))
            },
            ConversionStrategy::Numeric { precision, scale } => {
                ExtractedValue::Decimal(self.accessor.as_pg_numeric(row_idx, *scale))
            }
            ConversionStrategy::TransformBigInt => {
                ExtractedValue::Int64(self.accessor.as_int64(row_idx))
            }
            ConversionStrategy::TransformInteger => {
                ExtractedValue::Int32(self.accessor.as_int32(row_idx))
            }
            ConversionStrategy::TransformUuid => unsafe {
                ExtractedValue::Uuid(self.accessor.as_uuid(row_idx))
            },
            ConversionStrategy::TransformJsonb => {
                ExtractedValue::Jsonb(self.accessor.as_serde(row_idx))
            }
            ConversionStrategy::TransformNumeric { precision, scale } => {
                ExtractedValue::Decimal(self.accessor.as_pg_numeric(row_idx, *scale))
            }
            ConversionStrategy::TransformDouble { scale } => {
                ExtractedValue::Double(self.accessor.as_float64(row_idx, *scale))
            }
            ConversionStrategy::TimestampMillis => {
                ExtractedValue::TimestampMillis(self.accessor.as_chrono_naive(row_idx))
            }
            ConversionStrategy::TimestampMicros => {
                ExtractedValue::TimestampMicros(self.accessor.as_chrono_naive(row_idx))
            }
            ConversionStrategy::TimestampNanos => {
                ExtractedValue::TimestampNanos(self.accessor.as_chrono_naive(row_idx))
            }
            ConversionStrategy::TimestampTzMillis => unsafe {
                ExtractedValue::TimestampTzMillis(self.accessor.as_chrono_tz(row_idx))
            },
            ConversionStrategy::TimestampTzMicros => unsafe {
                ExtractedValue::TimestampTzMicros(self.accessor.as_chrono_tz(row_idx))
            },
            ConversionStrategy::TimestampTzNanos => unsafe {
                ExtractedValue::TimestampTzNanos(self.accessor.as_chrono_tz(row_idx))
            },
            _ => panic!("Unsupported conversion strategy: {:?}", self.strategy),
        }
    }
}
