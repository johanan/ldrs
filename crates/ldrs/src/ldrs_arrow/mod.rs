use std::sync::Arc;

use arrow_schema::{Field, FieldRef};

use crate::types::{ColumnSpec, TimeUnit};

fn map_arrow_time_to_pq_time(time_unit: &arrow_schema::TimeUnit) -> TimeUnit {
    match time_unit {
        arrow_schema::TimeUnit::Nanosecond => TimeUnit::Nanos,
        arrow_schema::TimeUnit::Microsecond => TimeUnit::Micros,
        arrow_schema::TimeUnit::Millisecond => TimeUnit::Millis,
        arrow_schema::TimeUnit::Second => TimeUnit::Millis,
    }
}

pub fn fill_vec_with_none<'a>(
    fields: &[Arc<Field>],
    mut schemas: Vec<ColumnSpec>,
) -> Vec<Option<ColumnSpec>> {
    fields
        .iter()
        .map(|field| {
            let maybe_index = schemas
                .iter()
                .position(|s| s.name().eq_ignore_ascii_case(field.name()));
            match maybe_index {
                Some(index) => Some(schemas.swap_remove(index)),
                None => None,
            }
        })
        .collect()
}

pub fn get_sf_arrow_schema(field: &FieldRef) -> Option<ColumnSpec> {
    let name = field.name().clone();
    // get other fields to help parse the type
    let precision = field
        .metadata()
        .get("precision")
        .and_then(|p| p.parse::<i32>().ok());
    let scale = field
        .metadata()
        .get("scale")
        .and_then(|s| s.parse::<i32>().ok());
    let precision_scale = precision.zip(scale);

    let char_length = field
        .metadata()
        .get("charLength")
        .and_then(|l| l.parse::<i32>().ok());

    // finally match on the logical type
    field
        .metadata()
        .get("logicalType")
        .and_then(|log_type| match log_type.as_str() {
            "FIXED" => match precision_scale {
                Some((p, s)) => Some(ColumnSpec::Numeric {
                    name,
                    precision: p,
                    scale: s,
                }),
                None => None,
            },
            "TEXT" => match char_length {
                // this is the max length for text in Snowflake so we treat it as full text
                Some(len) if len == 16777216 => Some(ColumnSpec::Text { name }),
                // this is a postgres limit for varchar, so we treat it as text
                Some(len) if len > 10485760 => Some(ColumnSpec::Text { name }),
                Some(length) => Some(ColumnSpec::Varchar { name, length }),
                None => Some(ColumnSpec::Text { name }),
            },
            "TIMESTAMP_TZ" => match field.data_type() {
                arrow_schema::DataType::Timestamp(time_unit, _) => Some(ColumnSpec::TimestampTz {
                    name,
                    time_unit: map_arrow_time_to_pq_time(time_unit),
                }),
                _ => None,
            },
            _ => None,
        })
}

impl TryFrom<&FieldRef> for ColumnSpec {
    type Error = anyhow::Error;

    fn try_from(field: &FieldRef) -> Result<Self, Self::Error> {
        let mapping = map_arrow_to_abstract(field);
        mapping.ok_or_else(|| anyhow::anyhow!("Failed to map arrow field to abstract schema"))
    }
}

pub fn map_arrow_to_abstract(field: &FieldRef) -> Option<ColumnSpec> {
    let name = field.name().clone();
    match field.data_type() {
        arrow_schema::DataType::Utf8 => Some(ColumnSpec::Text { name }),
        arrow_schema::DataType::Timestamp(time_unit, tz) => match tz {
            Some(_) => Some(ColumnSpec::TimestampTz {
                name,
                time_unit: map_arrow_time_to_pq_time(time_unit),
            }),
            None => Some(ColumnSpec::Timestamp {
                name,
                time_unit: map_arrow_time_to_pq_time(time_unit),
            }),
        },
        arrow_schema::DataType::Int8 => Some(ColumnSpec::SmallInt { name }),
        arrow_schema::DataType::Int16 => Some(ColumnSpec::SmallInt { name }),
        arrow_schema::DataType::Int64 => Some(ColumnSpec::BigInt { name }),
        arrow_schema::DataType::Int32 => Some(ColumnSpec::Integer { name }),
        arrow_schema::DataType::Float64 => Some(ColumnSpec::Double { name }),
        arrow_schema::DataType::Float32 => Some(ColumnSpec::Real { name }),
        arrow_schema::DataType::Boolean => Some(ColumnSpec::Boolean { name }),
        arrow_schema::DataType::Binary => Some(ColumnSpec::Text { name }),
        arrow_schema::DataType::Date32 => Some(ColumnSpec::Date { name }),
        arrow_schema::DataType::FixedSizeBinary(size) => {
            Some(ColumnSpec::FixedSizeBinary { name, size: *size })
        }
        arrow_schema::DataType::Decimal128(precision, scale) => Some(ColumnSpec::Numeric {
            name,
            precision: (*precision).into(),
            scale: (*scale).into(),
        }),
        _ => None,
    }
}

pub fn build_source_and_target_schema<'a>(
    schema: &'a arrow_schema::SchemaRef,
    src_logical: Vec<ColumnSpec>,
    overrides: Vec<Vec<ColumnSpec>>,
) -> Result<(Vec<ColumnSpec>, Vec<ColumnSpec>), anyhow::Error> {
    // base mappings from arrow schema types
    let arrow_inferred = schema
        .fields()
        .iter()
        .filter_map(map_arrow_to_abstract)
        .map(Some)
        .collect::<Vec<_>>();

    // we need a colschema for every field to process the arrow batch
    anyhow::ensure!(
        arrow_inferred.len() == schema.fields().len(),
        "Columns length does not match schema fields length"
    );

    let sf_inferred = schema
        .fields()
        .iter()
        .map(get_sf_arrow_schema)
        .collect::<Vec<_>>();

    let filled_src = fill_vec_with_none(&schema.fields, src_logical);

    // create the layers from the ground up with every column
    let mut layers: Vec<Vec<Option<ColumnSpec>>> = vec![arrow_inferred, sf_inferred, filled_src];
    // make sure the layers are the same length
    let filled_overrides = overrides
        .into_iter()
        .map(|o| fill_vec_with_none(&schema.fields, o));

    layers.extend(filled_overrides);

    let source = (0..schema.fields().len())
        .map(|i| layers[..3].iter().rev().find_map(|layer| layer[i].clone()))
        .flatten()
        .collect::<Vec<_>>();

    let target = (0..schema.fields().len())
        .map(|i| layers.iter().rev().find_map(|layer| layer[i].clone()))
        .flatten()
        .collect::<Vec<_>>();

    Ok((source, target))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow_schema::{DataType, Schema};

    use super::*;

    #[test]
    fn test_source_target_schema() {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let (src, target) =
            build_source_and_target_schema(&arrow_schema, Vec::new(), Vec::new()).unwrap();
        let basic_col = vec![
            ColumnSpec::Text { name: "a".into() },
            ColumnSpec::BigInt { name: "b".into() },
        ];
        assert_eq!(src, basic_col);
        assert_eq!(target, basic_col);

        // add snowflake override
        let mut sf_numeric = Field::new("b", DataType::Int64, true);
        // set metadata to set this int64 as numeric
        let sf_hashmap = HashMap::from([
            ("scale".to_string(), "10".to_string()),
            ("precision".to_string(), "18".to_string()),
            ("logicalType".to_string(), "FIXED".to_string()),
        ]);
        sf_numeric.set_metadata(sf_hashmap);
        let snowflake = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            sf_numeric,
        ]));
        let (src, target) =
            build_source_and_target_schema(&snowflake, Vec::new(), Vec::new()).unwrap();
        let snowflake = vec![
            ColumnSpec::Text { name: "a".into() },
            ColumnSpec::Numeric {
                name: "b".into(),
                precision: 18,
                scale: 10,
            },
        ];
        assert_eq!(src, snowflake);
        assert_eq!(target, snowflake);

        // keep simple arrow but override
        // this will create different source and target schemas
        let overrides = vec![vec![ColumnSpec::Numeric {
            name: "b".into(),
            precision: 18,
            scale: 10,
        }]];
        let (src, target) =
            build_source_and_target_schema(&arrow_schema, Vec::new(), overrides).unwrap();
        let src_cols = vec![
            ColumnSpec::Text { name: "a".into() },
            ColumnSpec::BigInt { name: "b".into() },
        ];
        let target_cols = vec![
            ColumnSpec::Text { name: "a".into() },
            ColumnSpec::Numeric {
                name: "b".into(),
                precision: 18,
                scale: 10,
            },
        ];
        assert_eq!(src, src_cols);
        assert_eq!(target, target_cols);

        // override the same column multiple times
        // this shows that the last override will be applied
        let overrides = vec![
            vec![
                ColumnSpec::Uuid { name: "a".into() },
                ColumnSpec::Numeric {
                    name: "b".into(),
                    precision: 18,
                    scale: 10,
                },
            ],
            vec![ColumnSpec::Numeric {
                name: "b".into(),
                precision: 12,
                scale: 8,
            }],
        ];
        let (src, target) =
            build_source_and_target_schema(&arrow_schema, Vec::new(), overrides).unwrap();
        let src_cols = vec![
            ColumnSpec::Text { name: "a".into() },
            ColumnSpec::BigInt { name: "b".into() },
        ];
        let target_cols = vec![
            ColumnSpec::Uuid { name: "a".into() },
            ColumnSpec::Numeric {
                name: "b".into(),
                precision: 12,
                scale: 8,
            },
        ];
        assert_eq!(src, src_cols);
        assert_eq!(target, target_cols);
    }
}
