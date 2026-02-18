use std::sync::Arc;

use anyhow::Context;
use arrow::{
    array::AsArray, buffer::Buffer, compute::CastOptions, datatypes::Decimal128Type,
    util::display::FormatOptions,
};
use arrow_array::{
    Array, ArrayRef, Decimal32Array, Decimal64Array, FixedSizeBinaryArray, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, SchemaRef};

use crate::types::{ColumnSchema, ColumnType, TimeUnit};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArrowCustomTransform {
    Uuid,
    UuidToString,
    Double {
        scale: i32,
    },
    RescaleDecimal {
        precision: u8,
        scale: i8,
        target_type: arrow_schema::DataType,
    },
    IntToDecimal {
        precision: u8,
        scale: i8,
        target_type: arrow_schema::DataType,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArrowColumnTransformStrategy {
    ArrowCast { target_type: arrow_schema::DataType },
    Custom(ArrowCustomTransform),
}

impl TimeUnit {
    pub fn to_arrow_timeunit(&self) -> arrow_schema::TimeUnit {
        match self {
            TimeUnit::Second => arrow_schema::TimeUnit::Second,
            TimeUnit::Millis => arrow_schema::TimeUnit::Millisecond,
            TimeUnit::Micros => arrow_schema::TimeUnit::Microsecond,
            TimeUnit::Nanos => arrow_schema::TimeUnit::Nanosecond,
        }
    }
}

impl<'a> ColumnSchema<'a> {
    pub fn to_arrow_field(&self) -> Field {
        let col_type = ColumnType::from(self);
        Field::new(self.name(), col_type.to_arrow_datatype(), true)
    }
}

impl ColumnType {
    pub fn to_arrow_datatype(&self) -> DataType {
        match self {
            ColumnType::Boolean => DataType::Boolean,
            ColumnType::SmallInt => DataType::Int16,
            ColumnType::Integer => DataType::Int32,
            ColumnType::BigInt => DataType::Int64,
            ColumnType::Real => DataType::Float32,
            ColumnType::Double(_) => DataType::Float64,
            ColumnType::Numeric(precision, scale) => {
                if *precision <= 9 {
                    DataType::Decimal32(*precision as u8, *scale as i8)
                } else if *precision <= 18 {
                    DataType::Decimal64(*precision as u8, *scale as i8)
                } else {
                    DataType::Decimal128(*precision as u8, *scale as i8)
                }
            }
            ColumnType::Text | ColumnType::Varchar(_) => DataType::Utf8,
            ColumnType::Uuid => DataType::FixedSizeBinary(16),
            ColumnType::Jsonb => DataType::Utf8,
            ColumnType::Timestamp(unit) => DataType::Timestamp(unit.to_arrow_timeunit(), None),
            ColumnType::TimestampTz(unit) => {
                DataType::Timestamp(unit.to_arrow_timeunit(), Some("UTC".into()))
            }
            ColumnType::Date => DataType::Date32,
            ColumnType::Bytea => DataType::Binary,
            ColumnType::FixedSizeBinary(size) => DataType::FixedSizeBinary(*size),
            ColumnType::Custom(_) => DataType::Binary,
        }
    }
}

pub fn build_arrow_transform_strategy(
    source_logical: impl Into<ColumnType>,
    target_logical: impl Into<ColumnType>,
    source_physical: &DataType,
) -> Result<Option<ArrowColumnTransformStrategy>, anyhow::Error> {
    let source_logical = source_logical.into();
    let target_logical = target_logical.into();
    let target_physical = target_logical.to_arrow_datatype();
    // logical matching is for the outer context, physicals match so return
    // only exception is numeric as it has multiple physical types
    if source_physical == &target_physical && !matches!(source_logical, ColumnType::Numeric(_, _)) {
        return Ok(None);
    }

    match (source_logical, target_logical) {
        // uuid logic from string or binary
        (_, ColumnType::Uuid) => match source_physical {
            DataType::Utf8 | DataType::LargeUtf8 => Ok(Some(ArrowColumnTransformStrategy::Custom(
                ArrowCustomTransform::Uuid,
            ))),
            DataType::FixedSizeBinary(16) => Ok(None),
            _ => Err(anyhow::anyhow!(
                "Unsupported source physical type for UUID target"
            )),
        },
        // uuid going back to string
        (ColumnType::Uuid, ColumnType::Text | ColumnType::Varchar(_)) => match source_physical {
            DataType::FixedSizeBinary(16) => Ok(Some(ArrowColumnTransformStrategy::Custom(
                ArrowCustomTransform::UuidToString,
            ))),
            _ => Err(anyhow::anyhow!(
                "Unsupported source physical type for UUID to string"
            )),
        },
        // logical numeric type has the most rules
        (ColumnType::Numeric(p, s), target) if s > 0 => {
            // decimals can have an integer physical type
            let is_int_backed = matches!(
                source_physical,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            );

            match target {
                // do not cast from scale > 0 to integer
                ColumnType::SmallInt | ColumnType::Integer | ColumnType::BigInt => Err(
                    anyhow::anyhow!("Cannot cast numeric with scale > 0 to integer"),
                ),
                // numeric and the physical type is the same, nothing to do
                ColumnType::Numeric(_, _) if source_physical == &target_physical => Ok(None),
                // if it is int backed then we just want to tell arrow to use the int values but as decimals
                // only works if scale is exactly the same
                ColumnType::Numeric(p, scale) if is_int_backed && s == scale => Ok(Some(
                    ArrowColumnTransformStrategy::Custom(ArrowCustomTransform::IntToDecimal {
                        precision: p as u8,
                        scale: s as i8,
                        target_type: target_physical,
                    }),
                )),
                // we need to cast to a double so we need the scale to divide the int
                ColumnType::Double(_) if is_int_backed => Ok(Some(
                    ArrowColumnTransformStrategy::Custom(ArrowCustomTransform::Double { scale: s }),
                )),
                // nothing matched and we have an integer, just turn it into a decimal128
                // so that arrow can cast it to the target type
                _ if is_int_backed => Ok(Some(ArrowColumnTransformStrategy::Custom(
                    ArrowCustomTransform::RescaleDecimal {
                        precision: p as u8,
                        scale: s as i8,
                        target_type: target_physical,
                    },
                ))),
                // if we are here and arrow can cast it, let it cast it
                _ if arrow::compute::can_cast_types(source_physical, &target_physical) => {
                    Ok(Some(ArrowColumnTransformStrategy::ArrowCast {
                        target_type: target_physical,
                    }))
                }
                _ => Err(anyhow::anyhow!(
                    "Unsupported cast from Numeric to {:?}",
                    target
                )),
            }
        }
        _ if arrow::compute::can_cast_types(source_physical, &target_physical) => {
            Ok(Some(ArrowColumnTransformStrategy::ArrowCast {
                target_type: target_physical,
            }))
        }
        _ => Err(anyhow::anyhow!(
            "Unsupported source logical type for target"
        )),
    }
}

pub fn transform_batch(
    batch: &RecordBatch,
    strategies: &[Option<ArrowColumnTransformStrategy>],
    target_schema: SchemaRef,
) -> Result<RecordBatch, anyhow::Error> {
    let columns = batch
        .columns()
        .iter()
        .zip(strategies.iter())
        .map(|(col, strategy)| match strategy {
            None => Ok(Arc::clone(col)),
            Some(ArrowColumnTransformStrategy::ArrowCast { target_type }) => {
                arrow::compute::cast_with_options(
                    col,
                    target_type,
                    &CastOptions {
                        safe: false,
                        format_options: FormatOptions::default(),
                    },
                )
                .with_context(|| format!("Failed to cast column to type {:?} ", target_type))
            }
            Some(ArrowColumnTransformStrategy::Custom(custom)) => {
                apply_custom_transform(col, custom)
            }
        })
        .collect::<Result<Vec<ArrayRef>, anyhow::Error>>()?;

    RecordBatch::try_new(target_schema, columns)
        .with_context(|| "Failed to create RecordBatch with schema")
}

fn apply_custom_transform(
    col: &ArrayRef,
    transform: &ArrowCustomTransform,
) -> Result<ArrayRef, anyhow::Error> {
    match transform {
        ArrowCustomTransform::Uuid => {
            let string_arr = col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Failed to downcast column to StringArray"))?;
            Ok(string_to_uuid_array(string_arr))
        }
        ArrowCustomTransform::UuidToString => {
            let uuid_arr = col
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    anyhow::anyhow!("Failed to downcast column to FixedSizeBinaryArray")
                })?;
            Ok(uuid_to_string_array(uuid_arr))
        }
        ArrowCustomTransform::Double { scale } => int_to_double_array(col, *scale),
        ArrowCustomTransform::RescaleDecimal {
            precision,
            scale,
            target_type,
        } => rescale_decimal_array(col, *precision, *scale, target_type),
        ArrowCustomTransform::IntToDecimal {
            precision,
            scale,
            target_type,
        } => int_to_decimal_arrow_cast(col, *precision, *scale, target_type),
    }
}

pub fn string_to_uuid_array(source: &StringArray) -> Arc<FixedSizeBinaryArray> {
    let num_rows = source.len();
    let mut values = vec![0u8; num_rows * 16];

    for i in 0..num_rows {
        if !source.is_null(i) {
            let uuid = uuid::Uuid::parse_str(source.value(i)).unwrap();
            values[i * 16..(i + 1) * 16].copy_from_slice(uuid.as_bytes());
        }
    }

    let nulls = source.nulls().cloned();
    Arc::new(FixedSizeBinaryArray::try_new(16, Buffer::from(values), nulls).unwrap())
}

pub fn uuid_to_string_array(source: &FixedSizeBinaryArray) -> ArrayRef {
    let result: StringArray = (0..source.len())
        .map(|i| {
            if source.is_null(i) {
                None
            } else {
                Some(uuid::Uuid::from_bytes_ref(source.value(i).try_into().unwrap()).to_string())
            }
        })
        .collect();
    Arc::new(result)
}

pub fn int_to_decimal_arrow_cast(
    source: &ArrayRef,
    precision: u8,
    scale: i8,
    target_type: &DataType,
) -> Result<ArrayRef, anyhow::Error> {
    // match on type and then tell arrow that the int array is actually a decimal array
    // we run this unsafe as we know that the values have to fit inside of the decimal no matter the precision
    let (intermediate_type, decimal_arr): (DataType, ArrayRef) = match source.data_type() {
        DataType::Int8 | DataType::Int16 => {
            let int32_arr = arrow::compute::cast(source, &DataType::Int32)?;
            let arrow_dec = DataType::Decimal32(precision, scale);
            let array_data = int32_arr.to_data();
            let decimal_data = unsafe {
                array_data
                    .into_builder()
                    .data_type(arrow_dec.clone())
                    .build_unchecked()
            };
            (arrow_dec, Arc::new(Decimal32Array::from(decimal_data)))
        }
        DataType::Int32 => {
            let arrow_dec = DataType::Decimal32(precision, scale);
            let array_data = source.to_data();
            let decimal_data = unsafe {
                array_data
                    .into_builder()
                    .data_type(arrow_dec.clone())
                    .build_unchecked()
            };
            (arrow_dec, Arc::new(Decimal32Array::from(decimal_data)))
        }
        DataType::Int64 => {
            let arrow_dec = DataType::Decimal64(precision, scale);
            let array_data = source.to_data();
            let decimal_data = unsafe {
                array_data
                    .into_builder()
                    .data_type(arrow_dec.clone())
                    .build_unchecked()
            };
            (arrow_dec, Arc::new(Decimal64Array::from(decimal_data)))
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported source type for int_to_decimal: {:?}",
                source.data_type()
            ))
        }
    };

    if &intermediate_type == target_type {
        return Ok(decimal_arr);
    }
    // arrow cast will now take the correct action and return the correct value
    arrow::compute::cast_with_options(
        &decimal_arr,
        target_type,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    )
    .map_err(|e| anyhow::anyhow!("Failed to cast decimal: {}", e))
}

pub fn rescale_decimal_array(
    source: &ArrayRef,
    precision: u8,
    scale: i8,
    target_type: &DataType,
) -> Result<ArrayRef, anyhow::Error> {
    // int -> Decimal with 0 maintaining the value
    // Decimal128 should cover all the values and precision we need
    let decimal_type = match source.data_type() {
        DataType::Int8 => DataType::Decimal128(precision, 0),
        DataType::Int16 => DataType::Decimal128(precision, 0),
        DataType::Int32 => DataType::Decimal128(precision, 0),
        DataType::Int64 => DataType::Decimal128(precision, 0),
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported source type for rescale_decimal_array"
            ))
        }
    };
    let decimal_arr = arrow::compute::cast_with_options(
        source,
        &decimal_type,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    )
    .with_context(|| format!("Failed to cast to Decimal({}, {})", precision, scale))?;
    // Decimal -> Decimal with the desired scale
    let rescaled = decimal_arr
        .as_primitive::<Decimal128Type>()
        .clone()
        .with_precision_and_scale(precision, scale)?;

    // Decimal -> target
    arrow::compute::cast_with_options(
        &rescaled,
        target_type,
        &CastOptions {
            safe: false,
            format_options: FormatOptions::default(),
        },
    )
    .with_context(|| format!("Failed to cast Decimal to {:?}", target_type))
}

macro_rules! int_to_double {
    ($arr:expr, $array_type:ty, $divisor:expr) => {{
        let typed = $arr.as_any().downcast_ref::<$array_type>().unwrap();
        let result: Float64Array = typed.unary(|v| v as f64 / $divisor);
        Arc::new(result) as ArrayRef
    }};
}

pub fn int_to_double_array(source: &ArrayRef, scale: i32) -> Result<ArrayRef, anyhow::Error> {
    let divisor = fast_pow10(scale);
    match source.data_type() {
        DataType::Int8 => Ok(int_to_double!(source, Int8Array, divisor)),
        DataType::Int16 => Ok(int_to_double!(source, Int16Array, divisor)),
        DataType::Int32 => Ok(int_to_double!(source, Int32Array, divisor)),
        DataType::Int64 => Ok(int_to_double!(source, Int64Array, divisor)),
        _ => Err(anyhow::anyhow!("Expected integer array")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Decimal128Array, Int32Array};
    use arrow_schema::{DataType, Field, Schema};
    use tracing::debug;
    use uuid::Uuid;

    fn assert_transform(
        source_logical: ColumnType,
        target_logical: ColumnType,
        source_physical: DataType,
        source_col: ArrayRef,
        expected_col: ArrayRef,
    ) {
        let strategy = build_arrow_transform_strategy(
            source_logical,
            target_logical.clone(),
            &source_physical,
        )
        .unwrap();
        debug!("Strategy {:?}", strategy);

        let target_physical = target_logical.to_arrow_datatype();

        let source_schema = Arc::new(Schema::new(vec![Field::new("a", source_physical, true)]));
        let target_schema = Arc::new(Schema::new(vec![Field::new("a", target_physical, true)]));

        let batch = RecordBatch::try_new(source_schema, vec![source_col]).unwrap();
        let result = transform_batch(&batch, &[strategy], target_schema).unwrap();
        assert_eq!(result.column(0).as_ref(), expected_col.as_ref());
    }

    #[test]
    fn test_transform_identity_int32() {
        assert_transform(
            ColumnType::Integer,
            ColumnType::Integer,
            DataType::Int32,
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
        );
    }

    #[test]
    fn test_transform_custom_uuid_strategy() {
        let uuid1 = Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap();
        let uuid2 = Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12").unwrap();
        let bytes1 = uuid1.as_bytes();
        let bytes2 = uuid2.as_bytes();
        assert_transform(
            ColumnType::Text,
            ColumnType::Uuid,
            DataType::Utf8,
            Arc::new(StringArray::from(vec![
                Some("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
                Some("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12"),
                None,
            ])),
            Arc::new(FixedSizeBinaryArray::from(vec![
                Some(bytes1.as_slice()),
                Some(bytes2.as_slice()),
                None,
            ])),
        );
    }

    #[test]
    fn test_transform_uuid_to_string() {
        let uuid1 = Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap();
        let uuid2 = Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12").unwrap();
        let bytes1 = uuid1.as_bytes();
        let bytes2 = uuid2.as_bytes();
        assert_transform(
            ColumnType::Uuid,
            ColumnType::Text,
            DataType::FixedSizeBinary(16),
            Arc::new(FixedSizeBinaryArray::from(vec![
                Some(bytes1.as_slice()),
                Some(bytes2.as_slice()),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
                Some("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12"),
                None,
            ])),
        );
    }

    #[test]
    fn test_transform_bigint_to_float() {
        assert_transform(
            ColumnType::Numeric(18, 2),
            ColumnType::Double(None),
            DataType::Int64,
            Arc::new(Int64Array::from(vec![
                Some(100),
                Some(200),
                Some(300),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1.00),
                Some(2.00),
                Some(3.00),
                None,
            ])),
        );
    }

    #[test]
    fn test_transform_arrow_string() {
        assert_transform(
            ColumnType::Varchar(10),
            ColumnType::Integer,
            DataType::Utf8,
            Arc::new(StringArray::from(vec![Some("1"), Some("2"), None])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None])),
        );
    }

    #[test]
    fn test_transform_arrow_cast_numeric() {
        // this should widen the bits as the precision increases, but not the scale
        assert_transform(
            ColumnType::Numeric(9, 2),
            ColumnType::Numeric(18, 2),
            DataType::Int32,
            Arc::new(Int32Array::from(vec![
                Some(100),
                Some(200),
                Some(300),
                None,
            ])),
            Arc::new(
                Decimal64Array::from(vec![Some(100), Some(200), Some(300), None])
                    .with_data_type(DataType::Decimal64(18, 2)),
            ),
        );

        // this should compact the bits as the precision decreases, but not the scale
        assert_transform(
            ColumnType::Numeric(18, 2),
            ColumnType::Numeric(9, 2),
            DataType::Int64,
            Arc::new(Int64Array::from(vec![
                Some(100),
                Some(200),
                Some(300),
                None,
            ])),
            Arc::new(
                Decimal32Array::from(vec![Some(100), Some(200), Some(300), None])
                    .with_data_type(DataType::Decimal32(9, 2)),
            ),
        );
    }

    #[test]
    fn test_transform_arrow_rescale_numeric() {
        // rescale is used to go from numeric to pretty much any other type
        // we let the arrow cast do the transform
        assert_transform(
            ColumnType::Numeric(9, 2),
            ColumnType::Text,
            DataType::Int32,
            Arc::new(Int32Array::from(vec![
                Some(100),
                Some(200),
                Some(300),
                None,
            ])),
            Arc::new(StringArray::from(vec![
                Some("1.00"),
                Some("2.00"),
                Some("3.00"),
                None,
            ])),
        );
    }

    #[test_log::test]
    fn test_decimal_to_decimal_int_source() {
        assert_transform(
            ColumnType::Numeric(38, 5),
            ColumnType::Numeric(38, 5),
            DataType::Int64,
            Arc::new(Int64Array::from(vec![
                Some(1234500000),
                Some(6789000000),
                None,
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(1234500000), Some(6789000000), None])
                    .with_data_type(DataType::Decimal128(38, 5)),
            ),
        )
    }

    #[test_log::test]
    fn test_decimal_to_decimal_int_diff_scale() {
        assert_transform(
            ColumnType::Numeric(38, 5),
            ColumnType::Numeric(38, 2),
            DataType::Int64,
            Arc::new(Int64Array::from(vec![
                Some(1234500000),
                Some(6789000000),
                None,
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(1234500), Some(6789000), None])
                    .with_data_type(DataType::Decimal128(38, 2)),
            ),
        );
        // now try a different precision
        assert_transform(
            ColumnType::Numeric(38, 5),
            ColumnType::Numeric(18, 2),
            DataType::Int64,
            Arc::new(Int64Array::from(vec![
                Some(1234500000),
                Some(6789000000),
                None,
            ])),
            Arc::new(
                Decimal64Array::from(vec![Some(1234500), Some(6789000), None])
                    .with_data_type(DataType::Decimal64(18, 2)),
            ),
        )
    }
}
