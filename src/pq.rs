use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use parquet::basic::LogicalType;
use parquet::schema::types::Type::{GroupType, PrimitiveType};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnDefintion {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub len: i32,
}

/// Maps a Parquet schema’s type to a corresponding PostgreSQL type. PostgreSQL types are used as
/// the abstract representation of the Parquet schema.
///
/// This enum is used to represent the PostgreSQL type information for a given Parquet column.
/// Each variant carries the column name (as a string slice) along with any additional
/// details required to generate the proper DDL for PostgreSQL (for example, length for `VARCHAR`
/// or precision and scale for `NUMERIC`).
///
/// # Variants
///
/// - `Varchar(&'a str, i32)` — Represents a `VARCHAR` column; the first field is the column name,
///   and the second field is the maximum length.
/// - `Text(&'a str)` — Represents a text type without length restrictions.
/// - `TimestampTz(&'a str)` — Represents a `TIMESTAMPTZ` (timestamp with time zone) column.
/// - `Timestamp(&'a str)` — Represents a `TIMESTAMP` column (without time zone).
/// - `Uuid(&'a str)` — Represents a `UUID` column.
/// - `Jsonb(&'a str)` — Represents a `JSONB` column.
/// - `Numeric(&'a str, i32, i32)` — Represents a `NUMERIC` column with specified precision and scale.
/// - `Real(&'a str)` — Represents a `REAL` (float4) column.
/// - `Double(&'a str)` — Represents a `DOUBLE PRECISION` (float8) column.
/// - `Integer(&'a str)` — Represents a 32-bit integer column (`INT4`).
/// - `BigInt(&'a str)` — Represents a 64-bit integer column (`INT8`).
/// - `Boolean(&'a str)` — Represents a boolean column.
///
/// # Examples
///
/// ```rust
/// use crate::pq::ParquetType;
///
/// // Create a Numeric type for a column named "price" with precision 10 and scale 2.
/// let pg_type = ParquetType::Numeric("price", 10, 2);
/// // Later, you can use `pg_type` to help generate your PostgreSQL DDL statement.
/// ```
///
pub enum ParquetType<'a> {
    Varchar(&'a str, i32),
    Text(&'a str),
    TimestampTz(&'a str, parquet::basic::TimeUnit),
    Timestamp(&'a str, parquet::basic::TimeUnit),
    Uuid(&'a str),
    Jsonb(&'a str),
    Numeric(&'a str, i32, i32),
    Real(&'a str),
    Double(&'a str),
    Integer(&'a str),
    BigInt(&'a str),
    Boolean(&'a str),
}

pub fn get_fields(
    metadata: &parquet::file::metadata::FileMetaData,
) -> anyhow::Result<&Vec<Arc<parquet::schema::types::Type>>, anyhow::Error> {
    let root = metadata.schema();
    match root {
        parquet::schema::types::Type::GroupType { fields, .. } => Ok(fields),
        _ => Err(anyhow::Error::msg("Invalid schema")),
    }
}

pub fn get_kv_fields(metadata: &parquet::file::metadata::FileMetaData) -> Vec<ColumnDefintion> {
    match metadata.key_value_metadata() {
        Some(kv) => {
            let cols = kv
                .iter()
                .find(|k| k.key == "cols")
                .and_then(|k| k.value.clone());
            let try_cols = cols.map(|cols| serde_json::from_str(&cols)).transpose();
            match try_cols {
                Ok(Some(cols)) => cols,
                _ => Vec::new(),
            }
        }
        None => Vec::new(),
    }
}

pub fn map_parquet_to_abstract<'a>(
    pq: &'a Arc<parquet::schema::types::Type>,
    user_defs: &[ColumnDefintion],
) -> Option<ParquetType<'a>> {
    match pq.as_ref() {
        GroupType { .. } => None,
        PrimitiveType { basic_info, .. } => {
            let name = pq.name();
            let pq_type: ParquetType<'_> = match basic_info {
                bi if bi.logical_type().is_some() => {
                    // we have a logical type, use that
                    match bi.logical_type().unwrap() {
                        LogicalType::String => user_defs
                            .iter()
                            .find(|cd| cd.name == name)
                            .and_then(|cd| {
                                if cd.ty == "VARCHAR" {
                                    Some(ParquetType::Varchar(name, cd.len))
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(ParquetType::Text(name)),
                        LogicalType::Timestamp {
                            is_adjusted_to_u_t_c: true,
                            unit,
                        } => ParquetType::TimestampTz(name, unit),
                        LogicalType::Timestamp {
                            is_adjusted_to_u_t_c: false,
                            unit,
                        } => ParquetType::Timestamp(name, unit),
                        LogicalType::Uuid => ParquetType::Uuid(name),
                        LogicalType::Json => ParquetType::Jsonb(name),
                        LogicalType::Decimal { scale, precision } => {
                            ParquetType::Numeric(name, precision, scale)
                        }
                        _ => ParquetType::Text(name),
                    }
                }
                _ => match pq.get_physical_type() {
                    parquet::basic::Type::FLOAT => ParquetType::Real(name),
                    parquet::basic::Type::DOUBLE => ParquetType::Double(name),
                    parquet::basic::Type::INT32 => ParquetType::Integer(name),
                    parquet::basic::Type::INT64 => ParquetType::BigInt(name),
                    parquet::basic::Type::BOOLEAN => ParquetType::Boolean(name),
                    _ => ParquetType::Text(name),
                },
            };
            Some(pq_type)
        }
    }
}

#[derive(Debug)]
pub enum ArrowArrayRef<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Decimal128(&'a Decimal128Array, u8, i8),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Jsonb(&'a StringArray),
    Utf8(&'a StringArray),
    Boolean(&'a BooleanArray),
    TimestampMillisecond(&'a TimestampMillisecondArray, bool),
    TimestampMicrosecond(&'a TimestampMicrosecondArray, bool),
    TimestampNanosecond(&'a TimestampNanosecondArray, bool),
    FixedSizeBinaryArray(&'a FixedSizeBinaryArray, i32),
}

pub fn downcast_array<'a>((array, pg_type): (&'a ArrayRef, &ParquetType<'a>)) -> ArrowArrayRef<'a> {
    match array.data_type() {
        DataType::Int32 => ArrowArrayRef::Int32(array.as_primitive()),
        DataType::Int64 => ArrowArrayRef::Int64(array.as_primitive()),
        DataType::Float32 => ArrowArrayRef::Float32(array.as_primitive()),
        DataType::Float64 => ArrowArrayRef::Float64(array.as_primitive()),
        DataType::Timestamp(unit, is_utc) => match unit {
            TimeUnit::Millisecond => {
                ArrowArrayRef::TimestampMillisecond(array.as_primitive(), is_utc.is_some())
            }
            TimeUnit::Microsecond => {
                ArrowArrayRef::TimestampMicrosecond(array.as_primitive(), is_utc.is_some())
            }
            TimeUnit::Nanosecond => {
                ArrowArrayRef::TimestampNanosecond(array.as_primitive(), is_utc.is_some())
            }
            _ => unimplemented!(),
        },
        DataType::FixedSizeBinary(size) => {
            ArrowArrayRef::FixedSizeBinaryArray(array.as_fixed_size_binary(), *size)
        }
        DataType::Boolean => ArrowArrayRef::Boolean(array.as_boolean()),
        DataType::Decimal128(precision, scale) => {
            ArrowArrayRef::Decimal128(array.as_primitive(), *precision, *scale)
        }
        DataType::Utf8 => match pg_type {
            &ParquetType::Jsonb(_) => ArrowArrayRef::Jsonb(array.as_string()),
            _ => ArrowArrayRef::Utf8(array.as_string()),
        },
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_provider::builder_from_string;
    use crate::test_utils::{create_runtime, drop_runtime};

    #[tokio::test]
    async fn test_get_file_metadata() {
        let cd = std::env::current_dir().unwrap();
        let path = format!(
            "file://{}/test_data/public.users.snappy.parquet",
            cd.display()
        );
        let rt = create_runtime();
        let result = builder_from_string(path, rt.handle().clone()).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        let metadata = result.metadata().file_metadata();
        println!("metadata: {:?}", metadata);
        assert_eq!(metadata.num_rows(), 2);

        // now get the fields
        let fields = get_fields(metadata).unwrap();
        assert_eq!(fields.len(), 6);
        drop_runtime(rt);
    }

    #[tokio::test]
    async fn test_get_file_metadata_invalid_url() {
        let path = "path/to/file".to_string();
        let rt = create_runtime();
        let result = builder_from_string(path, rt.handle().clone()).await;
        assert!(result.is_err());
        drop_runtime(rt);
    }
}
