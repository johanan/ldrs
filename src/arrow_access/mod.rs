use std::borrow::Cow;

use arrow::datatypes::ArrowNativeType;
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use bigdecimal::FromPrimitive;
use chrono::{DateTime, NaiveDateTime, Utc};
use pg_bigdecimal::{BigDecimal, BigInt, PgNumeric};
use postgres_types::Date;
use serde_json::Value;

pub fn fast_pow10(exp: i32) -> f64 {
    match exp {
        0 => 1.0,
        1 => 10.0,
        2 => 100.0,
        3 => 1_000.0,
        4 => 10_000.0,
        5 => 100_000.0,
        6 => 1_000_000.0,
        7 => 10_000_000.0,
        8 => 100_000_000.0,
        9 => 1_000_000_000.0,
        10 => 10_000_000_000.0,
        11 => 100_000_000_000.0,
        12 => 1_000_000_000_000.0,
        13 => 10_000_000_000_000.0,
        14 => 100_000_000_000_000.0,
        15 => 1_000_000_000_000_000.0,
        16 => 10_000_000_000_000_000.0,
        17 => 100_000_000_000_000_000.0,
        18 => 1_000_000_000_000_000_000.0,
        _ => 10_f64.powi(exp),
    }
}

macro_rules! define_column_accessor {
    ($(($variant:ident, $array_type:ty, $value_type:ty)),*) => {
        #[derive(Debug)]
        pub enum TypedColumnAccessor<'a> {
            $(
                $variant(&'a $array_type),
            )*

            // Complex types added manually
            TimestampMillisecond(&'a TimestampMillisecondArray, bool),
            TimestampMicrosecond(&'a TimestampMicrosecondArray, bool),
            TimestampNanosecond(&'a TimestampNanosecondArray, bool),
            Decimal128(&'a Decimal128Array, u8, i8),
            FixedSizeBinary(&'a FixedSizeBinaryArray, i32),
            Struct(&'a StructArray),
        }

        impl<'a> TypedColumnAccessor<'a> {
            pub fn new(array: &'a ArrayRef) -> Self {
                match array.data_type() {
                    $(
                        DataType::$variant => Self::$variant(array.as_any().downcast_ref::<$array_type>().unwrap()),
                    )*

                    // Complex types handled explicitly
                    DataType::Timestamp(TimeUnit::Millisecond, tz) =>
                        Self::TimestampMillisecond(array.as_primitive(), tz.is_some()),
                    DataType::Timestamp(TimeUnit::Microsecond, tz) =>
                        Self::TimestampMicrosecond(array.as_primitive(), tz.is_some()),
                    DataType::Timestamp(TimeUnit::Nanosecond, tz) =>
                        Self::TimestampNanosecond(array.as_primitive(), tz.is_some()),
                    DataType::Decimal128(precision, scale) =>
                        Self::Decimal128(array.as_primitive(), *precision, *scale),
                    DataType::FixedSizeBinary(size) =>
                        Self::FixedSizeBinary(array.as_fixed_size_binary(), *size),
                    DataType::Struct(_) =>
                        Self::Struct(array.as_any().downcast_ref::<StructArray>().unwrap()),

                    _ => panic!("Unsupported data type for TypedColumnAccessor: {:?}", array.data_type()),
                }
            }

            // Generate getter methods for each type
            $(
                #[inline]
                pub unsafe fn $variant(&self, row: usize) -> Option<$value_type> {
                    match self {
                        Self::$variant(arr) => {
                            if arr.is_null(row) { None } else { Some(arr.value_unchecked(row)) }
                        },
                        _ => panic!(concat!("Not a ", stringify!($variant), " array")),
                    }
                }
            )*
        }
    };
}

// Use the macro
define_column_accessor!(
    (Boolean, BooleanArray, bool),
    (Int8, Int8Array, i8),
    (Int16, Int16Array, i16),
    (Int32, Int32Array, i32),
    (Int64, Int64Array, i64),
    (Float32, Float32Array, f32),
    (Float64, Float64Array, f64),
    (Utf8, StringArray, &'a str),
    (Date32, Date32Array, i32)
);

impl<'a> TypedColumnAccessor<'a> {
    pub fn as_int64(&self, row: usize) -> Option<i64> {
        match self {
            Self::Int64(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value(row))
                }
            }
            Self::Int32(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(i64::from(arr.value(row)))
                }
            }
            Self::Int16(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(i64::from(arr.value(row)))
                }
            }
            Self::Int8(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(i64::from(arr.value(row)))
                }
            }
            Self::Decimal128(arr, _, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    let value = arr.value(row);
                    Some(value as i64)
                }
            }
            Self::Utf8(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    let v = arr.value(row);
                    v.parse::<i64>()
                        .unwrap_or_else(|e| {
                            panic!("Failed to parse i64 from string for row {}: {}", row, e)
                        })
                        .into()
                }
            }
            _ => panic!("Not an Integer array"),
        }
    }

    pub fn as_serde(&self, row: usize) -> Option<Value> {
        match self {
            Self::Utf8(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    let v = arr.value(row);
                    serde_json::from_str::<Value>(v)
                        .map(Some)
                        .unwrap_or_else(|e| panic!("Failed to parse JSON for row {}: {}", row, e))
                }
            }
            _ => panic!("Not a Utf8 array"),
        }
    }

    pub fn as_int32(&self, row: usize) -> Option<i32> {
        match self {
            Self::Int32(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value(row))
                }
            }
            Self::Int16(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(i32::from(arr.value(row)))
                }
            }
            Self::Int8(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(i32::from(arr.value(row)))
                }
            }
            Self::Int64(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    let value = arr.value(row);
                    if value < i32::MIN as i64 || value > i32::MAX as i64 {
                        panic!("Value out of range for i32: {}", value);
                    }
                    Some(value as i32)
                }
            }
            Self::Decimal128(arr, _, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    let value = arr.value(row);
                    Some(value as i32)
                }
            }
            _ => panic!("Not an Integer array"),
        }
    }

    pub fn as_decimal128(&self, row: usize) -> Option<i128> {
        match self {
            Self::Decimal128(arr, _, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value(row))
                }
            }
            Self::Int64(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(i128::from(arr.value(row)))
                }
            }
            Self::Int32(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(i128::from(arr.value(row)))
                }
            }
            Self::Int16(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(i128::from(arr.value(row)))
                }
            }
            _ => panic!("Not a Decimal128 array"),
        }
    }

    pub fn as_pg_numeric(&self, row: usize, scale: i32) -> Option<PgNumeric> {
        match self {
            Self::Decimal128(arr, _, scale) => {
                if arr.is_null(row) {
                    None
                } else {
                    let big_int = BigInt::from_i128(arr.value(row));
                    Some(pg_bigdecimal::PgNumeric::new(big_int.map(|bi| {
                        BigDecimal::new(bi, (*scale).to_i64().expect("Scale failed"))
                    })))
                }
            }
            Self::Int64(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    let big_int = BigInt::from_i64(arr.value(row));
                    Some(pg_bigdecimal::PgNumeric::new(
                        big_int.map(|bi| BigDecimal::new(bi, i64::from(scale))),
                    ))
                }
            }
            Self::Int32(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    let big_int = BigInt::from_i32(arr.value(row));
                    Some(pg_bigdecimal::PgNumeric::new(
                        big_int.map(|bi| BigDecimal::new(bi, i64::from(scale))),
                    ))
                }
            }
            Self::Int16(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    let big_int = BigInt::from_i16(arr.value(row));
                    Some(pg_bigdecimal::PgNumeric::new(
                        big_int.map(|bi| BigDecimal::new(bi, i64::from(scale))),
                    ))
                }
            }
            Self::Int8(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    let big_int = BigInt::from_i8(arr.value(row));
                    Some(pg_bigdecimal::PgNumeric::new(
                        big_int.map(|bi| BigDecimal::new(bi, i64::from(scale))),
                    ))
                }
            }
            _ => panic!("Not a Decimal128 or Integer array"),
        }
    }

    pub fn as_float64(&self, row: usize, scale: Option<i32>) -> Option<f64> {
        match self {
            Self::Float64(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value(row))
                }
            }
            Self::Float32(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(f64::from(arr.value(row)))
                }
            }
            Self::Decimal128(arr, _, scale) => {
                if arr.is_null(row) {
                    None
                } else {
                    let value = arr.value(row);
                    Some(value as f64 / fast_pow10(*scale as i32))
                }
            }
            Self::Int32(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    scale
                        .map(|s| f64::from(arr.value(row)) / fast_pow10(s))
                        .or_else(|| Some(f64::from(arr.value(row))))
                }
            }
            Self::Int16(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    scale
                        .map(|s| f64::from(arr.value(row)) / fast_pow10(s))
                        .or_else(|| Some(f64::from(arr.value(row))))
                }
            }
            Self::Int8(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    scale
                        .map(|s| f64::from(arr.value(row)) / fast_pow10(s))
                        .or_else(|| Some(f64::from(arr.value(row))))
                }
            }
            _ => panic!("Not a Float64 or Float32 array"),
        }
    }

    pub unsafe fn as_uuid(&self, row: usize) -> Option<uuid::Uuid> {
        match self {
            Self::FixedSizeBinary(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    uuid::Uuid::from_slice(arr.value_unchecked(row))
                        .map(Some)
                        .unwrap_or_else(|e| {
                            panic!("Failed to parse UUID from FixedSizeBinary: {}", e)
                        })
                }
            }
            Self::Utf8(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    uuid::Uuid::parse_str(arr.value_unchecked(row))
                        .map(Some)
                        .unwrap_or_else(|e| panic!("Failed to parse UUID from String: {}", e))
                }
            }
            _ => panic!("Not a FixedSizeBinary or Utf8 array"),
        }
    }

    pub fn as_chrono_naive(&self, row: usize) -> Option<NaiveDateTime> {
        match self {
            Self::TimestampMillisecond(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    arr.value_as_datetime(row)
                }
            }
            Self::TimestampMicrosecond(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    arr.value_as_datetime(row)
                }
            }
            Self::TimestampNanosecond(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    arr.value_as_datetime(row)
                }
            }
            _ => None,
        }
    }

    pub unsafe fn as_chrono_tz(&self, row: usize) -> Option<chrono::DateTime<chrono::Utc>> {
        match self {
            Self::TimestampMillisecond(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    let ts = arr.value_unchecked(row);
                    DateTime::<Utc>::from_timestamp_millis(ts)
                }
            }
            Self::TimestampMicrosecond(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    let ts = arr.value_unchecked(row);
                    DateTime::<Utc>::from_timestamp_micros(ts)
                }
            }
            Self::TimestampNanosecond(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    let ts = arr.value_unchecked(row);
                    Some(DateTime::<Utc>::from_timestamp_nanos(ts))
                }
            }
            _ => None,
        }
    }

    pub fn as_text(&self, row: usize) -> Option<Cow<'a, str>> {
        match self {
            Self::FixedSizeBinary(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    let slice = arr.value(row);
                    let hex = slice
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>();
                    Some(Cow::Owned(hex))
                }
            }
            Self::Utf8(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(Cow::Borrowed(arr.value(row)))
                }
            }
            _ => None,
        }
    }

    #[inline]
    pub unsafe fn TimestampMillisecond(&self, row: usize) -> Option<i64> {
        match self {
            Self::TimestampMillisecond(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value_unchecked(row))
                }
            }
            _ => panic!("Not a TimestampMillisecond array"),
        }
    }

    #[inline]
    pub fn TimestampMicrosecond(&self, row: usize) -> Option<i64> {
        match self {
            Self::TimestampMicrosecond(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value(row))
                }
            }
            _ => panic!("Not a TimestampMicrosecond array"),
        }
    }

    #[inline]
    pub fn TimestampNanosecond(&self, row: usize) -> Option<i64> {
        match self {
            Self::TimestampNanosecond(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value(row))
                }
            }
            _ => panic!("Not a TimestampNanosecond array"),
        }
    }

    #[inline]
    pub unsafe fn Decimal128(&self, row: usize) -> Option<i128> {
        match self {
            Self::Decimal128(arr, _, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value_unchecked(row))
                }
            }
            _ => panic!("Not a Decimal128 array"),
        }
    }

    #[inline]
    pub fn FixedSizeBinary(&self, row: usize) -> Option<&'a [u8]> {
        match self {
            Self::FixedSizeBinary(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value(row))
                }
            }
            _ => panic!("Not a FixedSizeBinary array"),
        }
    }

    #[inline]
    pub fn Struct(&self) -> Option<Value> {
        // to be implemented later
        None
    }

    #[inline]
    pub fn has_timezone(&self) -> Option<bool> {
        match self {
            Self::TimestampMillisecond(_, tz) => Some(*tz),
            Self::TimestampMicrosecond(_, tz) => Some(*tz),
            Self::TimestampNanosecond(_, tz) => Some(*tz),
            _ => None,
        }
    }

    #[inline]
    pub fn decimal_scale(&self) -> Option<(u8, i8)> {
        match self {
            Self::Decimal128(_, precision, scale) => Some((*precision, *scale)),
            _ => None,
        }
    }

    #[inline]
    pub fn binary_size(&self) -> Option<i32> {
        match self {
            Self::FixedSizeBinary(_, size) => Some(*size),
            _ => None,
        }
    }
}
