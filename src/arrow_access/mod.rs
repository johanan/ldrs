use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray
};
use arrow_schema::{DataType, TimeUnit};

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

                    _ => panic!("Unsupported data type for TypedColumnAccessor: {:?}", array.data_type()),
                }
            }

            // Generate getter methods for each type
            $(
                #[inline]
                pub fn $variant(&self, row: usize) -> Option<$value_type> {
                    match self {
                        Self::$variant(arr) => {
                            if arr.is_null(row) { None } else { Some(arr.value(row)) }
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
    (Int16, Int16Array, i16),
    (Int32, Int32Array, i32),
    (Int64, Int64Array, i64),
    (Float32, Float32Array, f32),
    (Float64, Float64Array, f64),
    (Utf8, StringArray, &'a str),
    (Date32, Date32Array, i32)
);

impl<'a> TypedColumnAccessor<'a> {
    #[inline]
    pub fn TimestampMillisecond(&self, row: usize) -> Option<i64> {
        match self {
            Self::TimestampMillisecond(arr, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value(row))
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
    pub fn Decimal128(&self, row: usize) -> Option<i128> {
        match self {
            Self::Decimal128(arr, _, _) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(arr.value(row))
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
