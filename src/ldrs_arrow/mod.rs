use std::{
    iter::{zip, Zip},
    sync::Arc,
    vec::IntoIter,
};

use arrow_schema::{Field, FieldRef};

use crate::types::{ColumnSchema, ColumnType, TimeUnit};

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
    mut schemas: Vec<ColumnSchema<'a>>,
) -> Vec<Option<ColumnSchema<'a>>> {
    fields
        .iter()
        .map(|field| {
            let maybe_index = schemas
                .iter()
                .position(|s| s.name().to_lowercase() == field.name().to_lowercase());
            match maybe_index {
                Some(index) => Some(schemas.swap_remove(index)),
                None => None,
            }
        })
        .collect()
}

pub fn get_sf_arrow_schema<'a>(field: &'a FieldRef) -> Option<ColumnSchema<'a>> {
    let name = field.name();
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
                Some((p, s)) => Some(ColumnSchema::Numeric(name, p, s)),
                None => None,
            },
            "TEXT" => match char_length {
                // this is the max length for text in Snowflake so we treat it as full text
                Some(len) if len == 16777216 => Some(ColumnSchema::Text(name)),
                // this is a postgres limit for varchar, so we treat it as text
                Some(len) if len > 10485760 => Some(ColumnSchema::Text(name)),
                Some(len) => Some(ColumnSchema::Varchar(name, len)),
                None => Some(ColumnSchema::Text(name)),
            },
            "TIMESTAMP_TZ" => match field.data_type() {
                arrow_schema::DataType::Timestamp(time_unit, _) => Some(ColumnSchema::TimestampTz(
                    name,
                    map_arrow_time_to_pq_time(time_unit),
                )),
                _ => None,
            },
            _ => None,
        })
}

impl<'a> TryFrom<&'a FieldRef> for ColumnSchema<'a> {
    type Error = anyhow::Error;

    fn try_from(field: &'a FieldRef) -> Result<Self, Self::Error> {
        let mapping = map_arrow_to_abstract(field);
        mapping.ok_or_else(|| anyhow::anyhow!("Failed to map arrow field to abstract schema"))
    }
}

pub fn map_arrow_to_abstract<'a>(field: &'a FieldRef) -> Option<ColumnSchema<'a>> {
    let name = field.name();
    match field.data_type() {
        arrow_schema::DataType::Utf8 => Some(ColumnSchema::Text(name)),
        arrow_schema::DataType::Timestamp(time_unit, tz) => match tz {
            Some(_) => Some(ColumnSchema::TimestampTz(
                name,
                map_arrow_time_to_pq_time(time_unit),
            )),
            None => Some(ColumnSchema::Timestamp(
                name,
                map_arrow_time_to_pq_time(time_unit),
            )),
        },
        arrow_schema::DataType::Int8 => Some(ColumnSchema::SmallInt(name)),
        arrow_schema::DataType::Int16 => Some(ColumnSchema::SmallInt(name)),
        arrow_schema::DataType::Int64 => Some(ColumnSchema::BigInt(name)),
        arrow_schema::DataType::Int32 => Some(ColumnSchema::Integer(name)),
        arrow_schema::DataType::Float64 => Some(ColumnSchema::Double(name, None)),
        arrow_schema::DataType::Float32 => Some(ColumnSchema::Real(name)),
        arrow_schema::DataType::Boolean => Some(ColumnSchema::Boolean(name)),
        arrow_schema::DataType::Binary => Some(ColumnSchema::Text(name)),
        arrow_schema::DataType::Date32 => Some(ColumnSchema::Date(name)),
        arrow_schema::DataType::FixedSizeBinary(size) => {
            Some(ColumnSchema::FixedSizeBinary(name, *size))
        }
        arrow_schema::DataType::Decimal128(precision, scale) => Some(ColumnSchema::Numeric(
            name,
            (*precision).into(),
            (*scale).into(),
        )),
        _ => None,
    }
}

pub fn build_source_and_target_schema<'a>(
    schema: &'a arrow_schema::SchemaRef,
    src_logical: Vec<ColumnSchema<'a>>,
    overrides: Vec<Vec<ColumnSchema<'a>>>,
) -> Result<(Vec<ColumnSchema<'a>>, Vec<ColumnSchema<'a>>), anyhow::Error> {
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
    let mut layers: Vec<Vec<Option<ColumnSchema<'a>>>> =
        vec![arrow_inferred, sf_inferred, filled_src];
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

pub fn build_final_schema<'a>(
    schema: &'a arrow_schema::SchemaRef,
    overrides: Vec<Vec<ColumnSchema<'a>>>,
) -> Result<(Vec<ColumnSchema<'a>>, Vec<Option<ColumnType>>), anyhow::Error> {
    let src_fields = schema
        .fields()
        .iter()
        .filter_map(map_arrow_to_abstract)
        .collect::<Vec<_>>();

    // we need a colschema for every field to process the arrow batch
    anyhow::ensure!(
        src_fields.len() == schema.fields().len(),
        "Columns length does not match schema fields length"
    );

    let source_types = src_fields.iter().map(ColumnType::from).collect::<Vec<_>>();
    let some_src_fields = src_fields.into_iter().map(Some).collect::<Vec<_>>();

    // check arrow metadata for Snowflake logical types
    let sf_fields = schema
        .fields()
        .iter()
        .map(get_sf_arrow_schema)
        .collect::<Vec<_>>();

    // we have a known schema override so do it here
    let init_src = flatten_schema_zip(zip(some_src_fields, sf_fields)).collect();

    let folded = overrides
        .into_iter()
        .fold(init_src, |acc, override_fields| {
            let filled = fill_vec_with_none(&schema.fields, override_fields);
            flatten_schema_zip(zip(acc, filled))
                .into_iter()
                .collect::<Vec<_>>()
        });
    // remove the Option as we should have a full vec
    let final_cols = folded.into_iter().flatten().collect::<Vec<_>>();
    let final_types = final_cols.iter().map(ColumnType::from).collect::<Vec<_>>();
    let transforms = flatten_arrow_transforms_zip(zip(source_types, final_types));
    Ok((final_cols, transforms))
}

pub fn flatten_arrow_transforms_zip(
    zip: Zip<IntoIter<ColumnType>, IntoIter<ColumnType>>,
) -> Vec<Option<ColumnType>> {
    zip.map(|(arr_source, out_col)| match (arr_source, out_col) {
        (a, b) if a == b => None,
        (ColumnType::Text, ColumnType::Varchar(_)) => None,
        // no uuid type in arrow so we always override to uuid
        (_, ColumnType::Uuid) => Some(ColumnType::Uuid),
        (_, ColumnType::Jsonb) => Some(ColumnType::Jsonb),
        (_, b) => Some(b),
    })
    .collect::<Vec<_>>()
}

pub fn flatten_schema_zip<'a>(
    zip: Zip<IntoIter<Option<ColumnSchema<'a>>>, IntoIter<Option<ColumnSchema<'a>>>>,
) -> IntoIter<Option<ColumnSchema<'a>>> {
    // special combination rules
    zip.map(|(a, b)| match (a, b) {
        // we can output a double, but the underlying type is an integer so we need the scale
        (Some(ColumnSchema::Numeric(_, _, scale)), Some(ColumnSchema::Double(name, None))) => {
            Some(ColumnSchema::Double(name, Some(scale)))
        }
        (Some(_), Some(b)) => Some(b),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    })
    .collect::<Vec<_>>()
    .into_iter()
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
        let basic_col = vec![ColumnSchema::Text("a"), ColumnSchema::BigInt("b")];
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
        let snowflake = vec![ColumnSchema::Text("a"), ColumnSchema::Numeric("b", 18, 10)];
        assert_eq!(src, snowflake);
        assert_eq!(target, snowflake);

        // keep simple arrow but override
        // this will create different source and target schemas
        let overrides = vec![vec![ColumnSchema::Numeric("b", 18, 10)]];
        let (src, target) =
            build_source_and_target_schema(&arrow_schema, Vec::new(), overrides).unwrap();
        let src_cols = vec![ColumnSchema::Text("a"), ColumnSchema::BigInt("b")];
        let target_cols = vec![ColumnSchema::Text("a"), ColumnSchema::Numeric("b", 18, 10)];
        assert_eq!(src, src_cols);
        assert_eq!(target, target_cols);

        // override the same column multiple times
        // this shows that the last override will be applied
        let overrides = vec![
            vec![ColumnSchema::Uuid("a"), ColumnSchema::Numeric("b", 18, 10)],
            vec![ColumnSchema::Numeric("b", 12, 8)],
        ];
        let (src, target) =
            build_source_and_target_schema(&arrow_schema, Vec::new(), overrides).unwrap();
        let src_cols = vec![ColumnSchema::Text("a"), ColumnSchema::BigInt("b")];
        let target_cols = vec![ColumnSchema::Uuid("a"), ColumnSchema::Numeric("b", 12, 8)];
        assert_eq!(src, src_cols);
        assert_eq!(target, target_cols);
    }
}
