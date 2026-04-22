use arrow_array::Array;

use crate::{arrow_access::TypedColumnAccessor, ldrs_postgres::pg_numeric::PgFixedNumeric};

pub trait ToPgNumeric {
    fn as_pg_numeric(&self, row: usize, scale: i32) -> Option<PgFixedNumeric>;
}

impl ToPgNumeric for TypedColumnAccessor<'_> {
    fn as_pg_numeric(&self, row: usize, scale: i32) -> Option<PgFixedNumeric> {
        match self {
            Self::Decimal128(arr, _, scale) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(PgFixedNumeric {
                        value: arr.value(row),
                        scale: *scale as i16,
                    })
                }
            }
            Self::Decimal64(arr, _, scale) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(PgFixedNumeric {
                        value: arr.value(row) as i128,
                        scale: *scale as i16,
                    })
                }
            }
            Self::Decimal32(arr, _, scale) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(PgFixedNumeric {
                        value: arr.value(row) as i128,
                        scale: *scale as i16,
                    })
                }
            }
            Self::Int64(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(PgFixedNumeric {
                        value: arr.value(row) as i128,
                        scale: scale as i16,
                    })
                }
            }
            Self::Int32(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(PgFixedNumeric {
                        value: arr.value(row) as i128,
                        scale: scale as i16,
                    })
                }
            }
            Self::Int16(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(PgFixedNumeric {
                        value: arr.value(row) as i128,
                        scale: scale as i16,
                    })
                }
            }
            Self::Int8(arr) => {
                if arr.is_null(row) {
                    None
                } else {
                    Some(PgFixedNumeric {
                        value: arr.value(row) as i128,
                        scale: scale as i16,
                    })
                }
            }
            _ => panic!("Not a Decimal128 or Integer array"),
        }
    }
}
