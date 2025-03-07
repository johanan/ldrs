use std::sync::Arc;

use arrow::{array::GenericStringBuilder, compute::cast};
use arrow_array::{Array, ArrayRef, FixedSizeBinaryArray, OffsetSizeTrait, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConversion {
    None,
    TimestampNanosToMicros,
    UUIDToString,
}

pub fn analyze_schema_conversions(schema: &SchemaRef) -> Vec<ColumnConversion> {
    schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                ColumnConversion::TimestampNanosToMicros
            }
            DataType::FixedSizeBinary(16) => ColumnConversion::UUIDToString,
            _ => ColumnConversion::None,
        })
        .collect()
}

pub fn map_convert_schema(field: &Arc<Field>, conversion: &ColumnConversion) -> Arc<Field> {
    match conversion {
        ColumnConversion::TimestampNanosToMicros => {
            if let DataType::Timestamp(TimeUnit::Nanosecond, tz) = field.data_type() {
                // Convert to microsecond timestamp
                Arc::new(
                    Field::new(
                        field.name(),
                        DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                        field.is_nullable(),
                    )
                    .with_metadata(field.metadata().clone()),
                )
            } else {
                field.clone()
            }
        }
        ColumnConversion::UUIDToString => Arc::new(
            Field::new(field.name(), DataType::Utf8, field.is_nullable())
                .with_metadata(field.metadata().clone()),
        ),
        ColumnConversion::None => field.clone(),
    }
}

fn cast_fixed_size_binary_to_string<O: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<ArrayRef, anyhow::Error> {
    let binary = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| anyhow::anyhow!("Expected FixedSizeBinaryArray"))?;

    let mut builder = GenericStringBuilder::<O>::new();
    for i in 0..binary.len() {
        if binary.is_null(i) {
            builder.append_null();
            continue;
        } else {
            let uuid = Uuid::from_slice(&binary.value(i))
                .map(|u| u.hyphenated().to_string())
                .map_err(|e| anyhow::anyhow!("Failed to parse UUID: {}", e))?;

            builder.append_value(uuid.to_string().as_str());
        }
    }

    Ok(Arc::new(builder.finish()))
}

pub fn convert_batch(
    batch: &RecordBatch,
    conversions: &[ColumnConversion],
) -> Result<RecordBatch, anyhow::Error> {
    let schema = batch.schema();
    let mut columns = Vec::with_capacity(batch.num_columns());
    let mut fields = Vec::with_capacity(batch.num_columns());

    for (i, field) in schema.fields().iter().enumerate() {
        let column = batch.column(i);

        let (new_column, new_field) = match conversions.get(i).unwrap_or(&ColumnConversion::None) {
            ColumnConversion::TimestampNanosToMicros => {
                if let DataType::Timestamp(TimeUnit::Nanosecond, tz) = field.data_type() {
                    // Convert nanosecond to microsecond
                    let target_type = DataType::Timestamp(TimeUnit::Microsecond, tz.clone());
                    let casted = cast(column, &target_type)
                        .map_err(|e| anyhow::anyhow!("Failed to cast timestamp: {}", e))?;

                    let new_field =
                        map_convert_schema(field, &ColumnConversion::TimestampNanosToMicros);

                    (casted, new_field)
                } else {
                    (column.clone(), field.clone())
                }
            }
            ColumnConversion::UUIDToString => {
                // Convert UUID to string
                let casted = cast_fixed_size_binary_to_string::<i32>(&column)
                    .map_err(|e| anyhow::anyhow!("Failed to cast UUID to string: {}", e))?;

                let new_field = map_convert_schema(field, &ColumnConversion::UUIDToString);

                (casted, new_field)
            }
            ColumnConversion::None => (column.clone(), field.clone()),
        };

        columns.push(new_column);
        fields.push(new_field);
    }

    // Create new batch with converted columns and schema
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| anyhow::anyhow!("Failed to create record batch: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, TimestampMicrosecondArray, TimestampNanosecondArray};
    use arrow::datatypes::TimeUnit;
    use arrow::record_batch::RecordBatch;
    use arrow_array::StringArray;
    use arrow_schema::DataType;
    use chrono::NaiveDateTime;

    #[test]
    fn test_analyze_schema_conversions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("c", DataType::FixedSizeBinary(16), false),
        ]));

        let conversions = analyze_schema_conversions(&schema);
        assert_eq!(
            conversions,
            vec![
                ColumnConversion::None,
                ColumnConversion::TimestampNanosToMicros,
                ColumnConversion::UUIDToString
            ]
        );
    }

    #[test]
    fn test_map_convert_schema() {
        let field = Arc::new(Field::new("a", DataType::Int64, false));
        let new_field = map_convert_schema(&field, &ColumnConversion::None);
        assert_eq!(field, new_field);

        let field = Arc::new(Field::new(
            "b",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ));
        let new_field = map_convert_schema(&field, &ColumnConversion::TimestampNanosToMicros);
        assert_eq!(
            new_field.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );

        let field = Arc::new(Field::new("c", DataType::FixedSizeBinary(16), false));
        let new_field = map_convert_schema(&field, &ColumnConversion::UUIDToString);
        assert_eq!(new_field.data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_convert_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            Field::new("c", DataType::FixedSizeBinary(16), false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(TimestampNanosecondArray::from_vec(
                    vec![1000000000, 2000000000, 3000000000],
                    None,
                )),
                Arc::new(FixedSizeBinaryArray::from(vec![
                    &[0; 16],
                    Uuid::parse_str("c2beae0a-2f21-4f23-a5a2-94b68ed1c213")
                        .unwrap()
                        .as_bytes(),
                    Uuid::parse_str("9d2013df-1181-460e-8ebe-2c159bbaa1ef")
                        .unwrap()
                        .as_bytes(),
                ])),
            ],
        )
        .unwrap();

        let conversions = vec![
            ColumnConversion::None,
            ColumnConversion::TimestampNanosToMicros,
            ColumnConversion::UUIDToString,
        ];

        let new_batch = convert_batch(&batch, &conversions).unwrap();
        assert_eq!(new_batch.num_columns(), 3);
        assert_eq!(
            new_batch.column_by_name("a").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            new_batch.column_by_name("b").unwrap().data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            new_batch.column_by_name("c").unwrap().data_type(),
            &DataType::Utf8
        );
        // test the values
        let ts = new_batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1000000);
        assert_eq!(ts.value(1), 2000000);
        assert_eq!(ts.value(2), 3000000);

        let uuids = new_batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(uuids.value(0), "00000000-0000-0000-0000-000000000000");
        assert_eq!(uuids.value(1), "c2beae0a-2f21-4f23-a5a2-94b68ed1c213");
        assert_eq!(uuids.value(2), "9d2013df-1181-460e-8ebe-2c159bbaa1ef");
    }
}
