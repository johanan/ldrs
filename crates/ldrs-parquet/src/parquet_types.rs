use ldrs_arrow::{ColumnSpec, TimeUnit};
use parquet::basic::{DecimalType, LogicalType, TimestampType};
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

impl From<&parquet::basic::LogicalType> for ParquetLogicalType {
    fn from(logical_type: &parquet::basic::LogicalType) -> Self {
        use parquet::basic::LogicalType;

        match logical_type {
            LogicalType::String => ParquetLogicalType::String,
            LogicalType::Decimal(DecimalType { scale, precision }) => ParquetLogicalType::Decimal {
                precision: *precision,
                scale: *scale,
            },
            LogicalType::Timestamp(TimestampType {
                is_adjusted_to_u_t_c,
                unit,
            }) => ParquetLogicalType::Timestamp {
                unit: time_unit_from(unit),
                is_utc: *is_adjusted_to_u_t_c,
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
                    .logical_type_ref()
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

fn time_unit_from(pq_unit: &parquet::basic::TimeUnit) -> TimeUnit {
    match pq_unit {
        parquet::basic::TimeUnit::MILLIS => TimeUnit::Millis,
        parquet::basic::TimeUnit::MICROS => TimeUnit::Micros,
        parquet::basic::TimeUnit::NANOS => TimeUnit::Nanos,
    }
}

/// Map a parquet primitive field to a `ColumnSpec` from its metadata.
pub fn columnspec_from_parquet(field: &Arc<parquet::schema::types::Type>) -> Option<ColumnSpec> {
    match field.as_ref() {
        GroupType { .. } => None,
        PrimitiveType { basic_info, .. } => {
            let name = field.name().to_string();
            match basic_info {
                bi if bi.logical_type_ref().is_some() => match bi.logical_type_ref().unwrap() {
                    LogicalType::String => Some(ColumnSpec::Text { name }),
                    LogicalType::Timestamp(TimestampType {
                        is_adjusted_to_u_t_c: true,
                        unit,
                    }) => Some(ColumnSpec::TimestampTz {
                        name,
                        time_unit: time_unit_from(unit),
                    }),
                    LogicalType::Timestamp(TimestampType {
                        is_adjusted_to_u_t_c: false,
                        unit,
                    }) => Some(ColumnSpec::Timestamp {
                        name,
                        time_unit: time_unit_from(unit),
                    }),
                    LogicalType::Uuid => Some(ColumnSpec::Uuid { name }),
                    LogicalType::Json => Some(ColumnSpec::Jsonb { name }),
                    LogicalType::Decimal(DecimalType { scale, precision }) => {
                        Some(ColumnSpec::Numeric {
                            name,
                            precision: *precision,
                            scale: *scale,
                        })
                    }
                    _ => None,
                },
                _ => match field.get_physical_type() {
                    parquet::basic::Type::FLOAT => Some(ColumnSpec::Real { name }),
                    parquet::basic::Type::DOUBLE => Some(ColumnSpec::Double { name }),
                    parquet::basic::Type::INT32 => Some(ColumnSpec::Integer { name }),
                    parquet::basic::Type::INT64 => Some(ColumnSpec::BigInt { name }),
                    parquet::basic::Type::BOOLEAN => Some(ColumnSpec::Boolean { name }),
                    parquet::basic::Type::INT96 => Some(ColumnSpec::Timestamp {
                        name,
                        time_unit: TimeUnit::Micros,
                    }),
                    _ => None,
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder_from_string;
    use arrow_schema::DataType;
    use ldrs_test_fixtures::fixture_url;

    pub fn create_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    pub fn drop_runtime(rt: tokio::runtime::Runtime) {
        tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
    }

    #[tokio::test]
    async fn test_get_file_metadata() {
        let path = fixture_url("public.users/public.users.snappy.parquet");
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

    const INT96_FILE: &str =
        "delta_spark/penguins_timestamps/part-00000-e9077e78-29e5-443c-af05-b254237e5ccb-c000.snappy.parquet";

    #[tokio::test]
    async fn int96_reads_as_zoneless_micros() {
        use arrow::array::AsArray;
        use arrow::datatypes::TimestampMicrosecondType;
        use futures::StreamExt;

        let rt = create_runtime();
        let builder = builder_from_string(fixture_url(INT96_FILE), rt.handle().clone())
            .await
            .unwrap();
        let micros = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None);
        let field = builder
            .schema()
            .field_with_name("observed_at")
            .unwrap()
            .clone();
        assert_eq!(field.data_type(), &micros);

        let fields = get_fields(builder.metadata().file_metadata()).unwrap();
        let observed = fields.iter().find(|f| f.name() == "observed_at").unwrap();
        assert_eq!(
            columnspec_from_parquet(observed),
            Some(ColumnSpec::Timestamp {
                name: "observed_at".into(),
                time_unit: TimeUnit::Micros,
            })
        );

        let mut stream = builder.build().unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let column = batch.column_by_name("observed_at").unwrap();
        // 2008-11-06T05:00:51.123456Z, the fraction intact
        assert_eq!(
            column.as_primitive::<TimestampMicrosecondType>().value(0),
            1_225_947_651_123_456
        );
        drop_runtime(rt);
    }

    #[tokio::test]
    async fn int96_reads_as_micros_beside_the_row_number() {
        use futures::StreamExt;
        use ldrs_storage::build_store;
        use object_store::ObjectStoreExt;

        let rt = create_runtime();
        let url = url::Url::parse(&fixture_url(INT96_FILE)).unwrap();
        let (store, path, _) = build_store(&url).unwrap();
        let size = store.head(&path).await.unwrap().size;
        let mut stream = crate::stream_projected_parquet(
            store,
            &path,
            &["observed_at".to_string()],
            None,
            size,
            rt.handle().clone(),
        )
        .await
        .unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(
            batch
                .schema()
                .field_with_name("observed_at")
                .unwrap()
                .data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
        assert!(batch.column_by_name(crate::ROW_NUMBER_COLUMN).is_some());
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
