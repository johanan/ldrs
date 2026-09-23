use bytes::BufMut;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use ldrs_arrow::{ColumnSpec, TypedColumnAccessor};
use postgres_types::{to_sql_checked, ToSql, Type};

use crate::{arrow_bridge::ToPgNumeric, pg_numeric::PgFixedNumeric};

#[derive(Debug)]
pub enum ExtractedValue<'a> {
    Boolean(Option<bool>),
    Int64(Option<i64>),
    Int32(Option<i32>),
    Int16(Option<i16>),
    Double(Option<f64>),
    Real(Option<f32>),
    Decimal(Option<PgFixedNumeric>),
    Uuid(Option<uuid::Uuid>),
    Utf8(Option<&'a str>),
    Date(Option<NaiveDate>),
    TimestampSeconds(Option<NaiveDateTime>),
    TimestampMillis(Option<NaiveDateTime>),
    TimestampMicros(Option<NaiveDateTime>),
    TimestampNanos(Option<NaiveDateTime>),
    TimestampTzSeconds(Option<DateTime<Utc>>),
    TimestampTzMillis(Option<DateTime<Utc>>),
    TimestampTzMicros(Option<DateTime<Utc>>),
    TimestampTzNanos(Option<DateTime<Utc>>),
    Jsonb(Option<&'a str>),
    Bytea(Option<&'a [u8]>),
}

#[derive(Debug)]
enum ExtractionStrategy {
    Boolean,
    BigInt,
    Integer,
    SmallInt,
    Double,
    Real,
    Text,
    Numeric { scale: i32 },
    Uuid,
    Jsonb,
    Bytea,
    Date,
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
            ColumnSpec::SmallInt { .. } => ExtractionStrategy::SmallInt,
            ColumnSpec::Integer { .. } => ExtractionStrategy::Integer,
            ColumnSpec::Double { .. } => ExtractionStrategy::Double,
            ColumnSpec::Real { .. } => ExtractionStrategy::Real,
            ColumnSpec::Text { .. } | ColumnSpec::Varchar { .. } => ExtractionStrategy::Text,
            ColumnSpec::Numeric { scale, .. } => ExtractionStrategy::Numeric { scale: *scale },
            ColumnSpec::Date { .. } => ExtractionStrategy::Date,
            ColumnSpec::Timestamp {
                time_unit: ldrs_arrow::TimeUnit::Second,
                ..
            } => ExtractionStrategy::TimestampSeconds,
            ColumnSpec::Timestamp {
                time_unit: ldrs_arrow::TimeUnit::Millis,
                ..
            } => ExtractionStrategy::TimestampMillis,
            ColumnSpec::Timestamp {
                time_unit: ldrs_arrow::TimeUnit::Micros,
                ..
            } => ExtractionStrategy::TimestampMicros,
            ColumnSpec::Timestamp {
                time_unit: ldrs_arrow::TimeUnit::Nanos,
                ..
            } => ExtractionStrategy::TimestampNanos,
            ColumnSpec::TimestampTz {
                time_unit: ldrs_arrow::TimeUnit::Second,
                ..
            } => ExtractionStrategy::TimestampTzSeconds,
            ColumnSpec::TimestampTz {
                time_unit: ldrs_arrow::TimeUnit::Millis,
                ..
            } => ExtractionStrategy::TimestampTzMillis,
            ColumnSpec::TimestampTz {
                time_unit: ldrs_arrow::TimeUnit::Micros,
                ..
            } => ExtractionStrategy::TimestampTzMicros,
            ColumnSpec::TimestampTz {
                time_unit: ldrs_arrow::TimeUnit::Nanos,
                ..
            } => ExtractionStrategy::TimestampTzNanos,
            ColumnSpec::Uuid { .. } => ExtractionStrategy::Uuid,
            ColumnSpec::Jsonb { .. } => ExtractionStrategy::Jsonb,
            ColumnSpec::Bytea { .. } => ExtractionStrategy::Bytea,
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
            ExtractionStrategy::SmallInt => unsafe {
                ExtractedValue::Int16(self.accessor.Int16(row_idx))
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
            ExtractionStrategy::Bytea => unsafe {
                ExtractedValue::Bytea(self.accessor.Binary(row_idx))
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
            // Date32 counts days from 1970-01-01, which is day 719_163 of the common era
            ExtractionStrategy::Date => unsafe {
                ExtractedValue::Date(
                    self.accessor
                        .Date32(row_idx)
                        .and_then(|days| NaiveDate::from_num_days_from_ce_opt(days + 719_163)),
                )
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
            ExtractedValue::Int16(v) => v.to_sql(ty, out),
            ExtractedValue::Double(v) => v.to_sql(ty, out),
            ExtractedValue::Uuid(v) => v.to_sql(ty, out),
            ExtractedValue::Date(v) => v.to_sql(ty, out),
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
            ExtractedValue::Bytea(v) => v.to_sql(ty, out),
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Date32Array};
    use bytes::BytesMut;
    use chrono::NaiveDate;
    use postgres_types::IsNull;
    use std::sync::Arc;

    /// Encodes the same bytes chrono's `NaiveDate` does, across both epochs: arrow counts days from
    /// 1970-01-01, postgres from 2000-01-01.
    #[test]
    fn a_date_column_encodes_as_a_postgres_date() {
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let dates = [
            Some(NaiveDate::from_ymd_opt(2024, 6, 1).unwrap()),
            Some(NaiveDate::from_ymd_opt(1969, 12, 31).unwrap()),
            None,
        ];
        let array: ArrayRef = Arc::new(Date32Array::from(
            dates
                .iter()
                .map(|d| d.map(|d| (d - epoch).num_days() as i32))
                .collect::<Vec<_>>(),
        ));
        let accessor = TypedColumnAccessor::new(&array);
        let converter =
            ColumnConverter::new(&accessor, &ColumnSpec::Date { name: "d".into() }).unwrap();

        for (row, expected) in dates.iter().enumerate() {
            let mut got = BytesMut::new();
            let mut want = BytesMut::new();
            let got_null = converter
                .extract_value(row)
                .to_sql(&Type::DATE, &mut got)
                .unwrap();
            let want_null = expected.to_sql(&Type::DATE, &mut want).unwrap();
            assert_eq!(
                matches!(got_null, IsNull::Yes),
                matches!(want_null, IsNull::Yes),
                "row {row} nullness"
            );
            assert_eq!(got, want, "row {row} bytes");
        }
    }
}
