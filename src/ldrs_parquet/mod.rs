use parquet::basic::LogicalType;
use parquet::schema::types::Type::{GroupType, PrimitiveType};
use std::sync::Arc;

use crate::types::{ColumnSchema, ColumnSpec, TimeUnit};

impl<'a> TryFrom<&'a Arc<parquet::schema::types::Type>> for ColumnSchema<'a> {
    type Error = anyhow::Error;

    fn try_from(field: &'a Arc<parquet::schema::types::Type>) -> Result<Self, Self::Error> {
        match field.as_ref() {
            GroupType { .. } => Err(anyhow::Error::msg(
                "Cannot convert GroupType to ColumnSchema",
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

impl TryFrom<&Arc<parquet::schema::types::Type>> for ColumnSpec {
    type Error = anyhow::Error;

    fn try_from(field: &Arc<parquet::schema::types::Type>) -> Result<Self, Self::Error> {
        match field.as_ref() {
            GroupType { .. } => Err(anyhow::Error::msg("Cannot convert GroupType to ColumnSpec")),
            PrimitiveType { basic_info, .. } => {
                let name = field.name().to_string();
                let pq_type: ColumnSpec = match basic_info {
                    bi if bi.logical_type().is_some() => {
                        // we have a logical type, use that
                        match bi.logical_type().unwrap() {
                            LogicalType::String => ColumnSpec::Text { name },
                            LogicalType::Timestamp {
                                is_adjusted_to_u_t_c: true,
                                unit,
                            } => ColumnSpec::TimestampTz {
                                name,
                                time_unit: TimeUnit::from(&unit),
                            },
                            LogicalType::Timestamp {
                                is_adjusted_to_u_t_c: false,
                                unit,
                            } => ColumnSpec::Timestamp {
                                name,
                                time_unit: TimeUnit::from(&unit),
                            },
                            LogicalType::Uuid => ColumnSpec::Uuid { name },
                            LogicalType::Json => ColumnSpec::Jsonb { name },
                            LogicalType::Decimal { scale, precision } => ColumnSpec::Numeric {
                                name,
                                precision,
                                scale,
                            },
                            _ => ColumnSpec::Text { name },
                        }
                    }
                    _ => match field.get_physical_type() {
                        parquet::basic::Type::FLOAT => ColumnSpec::Real { name },
                        parquet::basic::Type::DOUBLE => ColumnSpec::Double { name },
                        parquet::basic::Type::INT32 => ColumnSpec::Integer { name },
                        parquet::basic::Type::INT64 => ColumnSpec::BigInt { name },
                        parquet::basic::Type::BOOLEAN => ColumnSpec::Boolean { name },
                        _ => ColumnSpec::Text { name },
                    },
                };
                Ok(pq_type)
            }
        }
    }
}
