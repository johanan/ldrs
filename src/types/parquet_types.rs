use crate::types::{ColumnSchema, TimeUnit};
use parquet::basic::LogicalType;
use parquet::schema::types::Type::{GroupType, PrimitiveType};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetSchema<'a> {
    pub name: &'a str,
    pub columns: Vec<ParquetColumn<'a>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetColumn<'a> {
    pub name: &'a str,
    pub physical_type: ParquetPhysicalType,
    pub logical_type: Option<ParquetLogicalType>,
    pub repetition: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParquetLogicalType {
    String,
    Decimal { precision: i32, scale: i32 },
    Timestamp { unit: TimeUnit, is_utc: bool },
    Uuid,
    Json,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParquetPhysicalType {
    Boolean,
    Int32,
    Int64,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray(i32),
    Unknown,
}

impl From<parquet::basic::Type> for ParquetPhysicalType {
    fn from(pq_type: parquet::basic::Type) -> Self {
        match pq_type {
            parquet::basic::Type::BOOLEAN => ParquetPhysicalType::Boolean,
            parquet::basic::Type::INT32 => ParquetPhysicalType::Int32,
            parquet::basic::Type::INT64 => ParquetPhysicalType::Int64,
            parquet::basic::Type::FLOAT => ParquetPhysicalType::Float,
            parquet::basic::Type::DOUBLE => ParquetPhysicalType::Double,
            parquet::basic::Type::BYTE_ARRAY => ParquetPhysicalType::ByteArray,
            parquet::basic::Type::FIXED_LEN_BYTE_ARRAY => {
                // Note: We'd need the length from the schema, defaulting to 0 for now
                ParquetPhysicalType::FixedLenByteArray(0)
            }
            _ => ParquetPhysicalType::Unknown,
        }
    }
}

impl From<parquet::basic::LogicalType> for ParquetLogicalType {
    fn from(logical_type: parquet::basic::LogicalType) -> Self {
        use parquet::basic::LogicalType;

        match logical_type {
            LogicalType::String => ParquetLogicalType::String,
            LogicalType::Decimal { scale, precision } => {
                ParquetLogicalType::Decimal { precision, scale }
            }
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c,
                unit,
            } => ParquetLogicalType::Timestamp {
                unit: TimeUnit::from(&unit),
                is_utc: is_adjusted_to_u_t_c,
            },
            LogicalType::Uuid => ParquetLogicalType::Uuid,
            LogicalType::Json => ParquetLogicalType::Json,
            _ => ParquetLogicalType::Unknown,
        }
    }
}

impl<'a> From<&'a parquet::schema::types::Type> for ParquetColumn<'a> {
    fn from(pq_field: &'a parquet::schema::types::Type) -> Self {
        use parquet::schema::types::Type;

        match pq_field {
            Type::PrimitiveType { basic_info, .. } => {
                let name = pq_field.name();
                let physical_type = ParquetPhysicalType::from(pq_field.get_physical_type());
                let logical_type = basic_info
                    .logical_type()
                    .map(|lt| ParquetLogicalType::from(lt));
                let repetition = format!("{:?}", basic_info.repetition());

                ParquetColumn {
                    name,
                    physical_type,
                    logical_type,
                    repetition,
                }
            }
            Type::GroupType { .. } => {
                // For group types, we'll create a placeholder
                ParquetColumn {
                    name: pq_field.name(),
                    physical_type: ParquetPhysicalType::ByteArray, // Placeholder
                    logical_type: None,
                    repetition: "REQUIRED".to_string(), // Placeholder
                }
            }
        }
    }
}

impl<'a> TryFrom<&'a parquet::file::metadata::FileMetaData> for ParquetSchema<'a> {
    type Error = anyhow::Error;

    fn try_from(metadata: &'a parquet::file::metadata::FileMetaData) -> Result<Self, Self::Error> {
        let schema_descr = metadata.schema_descr();
        let name = schema_descr.name();
        let fields = get_fields(metadata)?;

        let columns = fields
            .iter()
            .map(|field| ParquetColumn::from(field.as_ref()))
            .collect();

        Ok(ParquetSchema { name, columns })
    }
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

impl<'a> TryFrom<&'a Arc<parquet::schema::types::Type>> for ColumnSchema<'a> {
    type Error = anyhow::Error;

    fn try_from(field: &'a Arc<parquet::schema::types::Type>) -> Result<Self, Self::Error> {
        match field.as_ref() {
            GroupType { .. } => Err(anyhow::Error::msg(
                "Cannot convert GroupType to ParquetColumn",
            )),
            PrimitiveType { basic_info, .. } => {
                let name = field.name();
                let pq_type: ColumnSchema<'_> = match basic_info {
                    bi if bi.logical_type().is_some() => {
                        // we have a logical type, use that
                        match bi.logical_type().unwrap() {
                            LogicalType::String => ColumnSchema::Text(name),
                            LogicalType::Timestamp {
                                is_adjusted_to_u_t_c: true,
                                unit,
                            } => ColumnSchema::TimestampTz(name, TimeUnit::from(&unit)),
                            LogicalType::Timestamp {
                                is_adjusted_to_u_t_c: false,
                                unit,
                            } => ColumnSchema::Timestamp(name, TimeUnit::from(&unit)),
                            LogicalType::Uuid => ColumnSchema::Uuid(name),
                            LogicalType::Json => ColumnSchema::Jsonb(name),
                            LogicalType::Decimal { scale, precision } => {
                                ColumnSchema::Numeric(name, precision, scale)
                            }
                            _ => ColumnSchema::Text(name),
                        }
                    }
                    _ => match field.get_physical_type() {
                        parquet::basic::Type::FLOAT => ColumnSchema::Real(name),
                        parquet::basic::Type::DOUBLE => ColumnSchema::Double(name, None),
                        parquet::basic::Type::INT32 => ColumnSchema::Integer(name),
                        parquet::basic::Type::INT64 => ColumnSchema::BigInt(name),
                        parquet::basic::Type::BOOLEAN => ColumnSchema::Boolean(name),
                        _ => ColumnSchema::Text(name),
                    },
                };
                Ok(pq_type)
            }
        }
    }
}
