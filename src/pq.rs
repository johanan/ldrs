use anyhow::Context;
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::{parse_url, parse_url_opts, ClientOptions, ObjectStore, ObjectStoreScheme};
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::basic::LogicalType;
use parquet::schema::types::Type::{GroupType, PrimitiveType};
use serde::{Deserialize, Serialize};
use tracing::instrument::WithSubscriber;
use std::collections::HashMap;
use std::fs::File;
use object_store::path::Path;
use std::sync::Arc;
use tracing::info;
use url::{ParseError, Url};
use std::io::Write;
use futures::StreamExt;

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnDefintion {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub len: i32,
}

/// Maps a Parquet schema’s type to a corresponding PostgreSQL type. PostgreSQL types are used as
/// the abstract representation of the Parquet schema.
///
/// This enum is used to represent the PostgreSQL type information for a given Parquet column.
/// Each variant carries the column name (as a string slice) along with any additional
/// details required to generate the proper DDL for PostgreSQL (for example, length for `VARCHAR`
/// or precision and scale for `NUMERIC`).
///
/// # Variants
///
/// - `Varchar(&'a str, i32)` — Represents a `VARCHAR` column; the first field is the column name,
///   and the second field is the maximum length.
/// - `Text(&'a str)` — Represents a text type without length restrictions.
/// - `TimestampTz(&'a str)` — Represents a `TIMESTAMPTZ` (timestamp with time zone) column.
/// - `Timestamp(&'a str)` — Represents a `TIMESTAMP` column (without time zone).
/// - `Uuid(&'a str)` — Represents a `UUID` column.
/// - `Jsonb(&'a str)` — Represents a `JSONB` column.
/// - `Numeric(&'a str, i32, i32)` — Represents a `NUMERIC` column with specified precision and scale.
/// - `Real(&'a str)` — Represents a `REAL` (float4) column.
/// - `Double(&'a str)` — Represents a `DOUBLE PRECISION` (float8) column.
/// - `Integer(&'a str)` — Represents a 32-bit integer column (`INT4`).
/// - `BigInt(&'a str)` — Represents a 64-bit integer column (`INT8`).
/// - `Boolean(&'a str)` — Represents a boolean column.
///
/// # Examples
///
/// ```rust
/// use crate::pq::ParquetType;
///
/// // Create a Numeric type for a column named "price" with precision 10 and scale 2.
/// let pg_type = ParquetType::Numeric("price", 10, 2);
/// // Later, you can use `pg_type` to help generate your PostgreSQL DDL statement.
/// ```
///
pub enum ParquetType<'a> {
    Varchar(&'a str, i32),
    Text(&'a str),
    TimestampTz(&'a str, parquet::basic::TimeUnit),
    Timestamp(&'a str, parquet::basic::TimeUnit),
    Uuid(&'a str),
    Jsonb(&'a str),
    Numeric(&'a str, i32, i32),
    Real(&'a str),
    Double(&'a str),
    Integer(&'a str),
    BigInt(&'a str),
    Boolean(&'a str),
}

pub fn base_or_relative_path(path: &str) -> Result<Url, anyhow::Error> {
    let try_parse = Url::parse(path);
    match try_parse {
        Ok(url) => Ok(url),
        Err(ParseError::RelativeUrlWithoutBase) => {
            let current_dir =
                std::env::current_dir().with_context(|| "Could not get current dir")?;
            let base = Url::parse(&format!("file://{}/", current_dir.display()))
                .with_context(|| "Could not parse base path file URL")?;
            Url::options()
                .base_url(Some(&base))
                .parse(path)
                .with_context(|| "Could not parse relative path file URL")
        }
        _ => Err(anyhow::Error::msg("Could not parse path URL")),
    }
}

struct AzureUrl {
    storage_account: String,
    blob_path: String,
}

impl AzureUrl {
    fn to_object_store_url(&self) -> Result<Url, anyhow::Error> {
        let url = format!("azure://{}", self.blob_path);
        Url::parse(&url).with_context(|| format!("Failed to parse URL: {}", url))
    }

    fn to_object_store_options(&self) -> HashMap<String, String> {
        HashMap::from([
            ("azure_storage_account_name".to_string(), self.storage_account.clone()),
            ("azure_use_azure_cli".to_string(), "true".to_string()),
        ])
    }
}

fn is_azure_url(url: &str) -> Result<Url, anyhow::Error> {
    let url = Url::parse(url).with_context(|| format!("Failed to parse URL: {}", url))?;
    match url.scheme() {
        "azure" | "az" => Ok(url),
        "https" => {
            let host = url.host_str().ok_or_else(|| anyhow::anyhow!("No host found"))?;
            if host.ends_with("blob.core.windows.net") {
                Ok(url)
            } else {
                Err(anyhow::anyhow!("Invalid host"))
            }
        },
        _ => Err(anyhow::anyhow!("Invalid scheme")),
    }
}
    

fn parse_az_url(url: Url) -> Result<AzureUrl, anyhow::Error> {
    let host = url.host_str().ok_or_else(|| anyhow::anyhow!("No host found"))?;
    let path = match url.path() {
        "/" => Err(anyhow::anyhow!("No path found")),
        p => Ok(p),
    }?;

    let clean_path = path.strip_prefix('/').unwrap_or(path);

    match url.scheme() {
        "https" | "azure" => {
            match host.split_once('.') {
                Some((account, _)) => Ok(AzureUrl {
                    storage_account: account.to_string(),
                    blob_path: clean_path.to_string(),
                }),
                None => Ok(AzureUrl {
                    storage_account: host.to_string(),
                    blob_path: clean_path.to_string(),
                }),
            }
        },
        _ => Err(anyhow::anyhow!("Invalid scheme")),
    }
}

pub async fn get_file_metadata(
    path_url: String,
    handle: Option<tokio::runtime::Handle>,
) -> Result<ParquetRecordBatchStreamBuilder<ParquetObjectReader>, anyhow::Error> {
    let path_parsed = base_or_relative_path(&path_url)?;

    let azure_url = is_azure_url(&path_url).and_then(parse_az_url);
    let mut options = std::collections::HashMap::from([
        (String::from("timeout"), String::from("30s")),
    ]);

    /*let (uri, options) = match azure_url {
        Ok(az) => {
            let uri = az.to_object_store_url()?;
            let az_options = az.to_object_store_options();
            options.extend(az_options);
            (uri, options)
        },
        Err(_) => (path_parsed, options),
    };*/

    let (scheme, path) = ObjectStoreScheme::parse(&path_parsed)?;
    info!("scheme: {:?}, path: {:?}", scheme, path);

    /*let (store, path) =
        parse_url_opts(&path_parsed, options).with_context(|| "Could not find a valid object store")?;*/
    
    let store = MicrosoftAzureBuilder::from_env().with_url(path_parsed.clone())
        .with_config(object_store::azure::AzureConfigKey::Client(object_store::ClientConfigKey::Timeout), "120s").build()?;
    
    info!("store: {:?}", store);
    let path = Path::from_url_path("fixme.snappy.parquet")?;

    //let (store, path) = parse_url(&path_parsed)?;
    let store = Arc::new(store);
    let meta = store
        .head(&path)
        .await
        .with_context(|| "Could not find file in store")?;
    info!("meta: {:?}", meta);

    

    let reader = handle.map(|h| ParquetObjectReader::new(store.clone(), meta.clone()).with_runtime(h))
    .unwrap_or_else(|| ParquetObjectReader::new(store, meta));

    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .with_context(|| "Could not create parquet record batch stream builder")?;
    Ok(builder)
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

pub fn get_kv_fields(metadata: &parquet::file::metadata::FileMetaData) -> Vec<ColumnDefintion> {
    match metadata.key_value_metadata() {
        Some(kv) => {
            let cols = kv
                .iter()
                .find(|k| k.key == "cols")
                .and_then(|k| k.value.clone());
            let try_cols = cols.map(|cols| serde_json::from_str(&cols)).transpose();
            match try_cols {
                Ok(Some(cols)) => cols,
                _ => Vec::new(),
            }
        }
        None => Vec::new(),
    }
}

pub fn map_parquet_to_abstract<'a>(
    pq: &'a Arc<parquet::schema::types::Type>,
    user_defs: &[ColumnDefintion],
) -> Option<ParquetType<'a>> {
    match pq.as_ref() {
        GroupType { .. } => None,
        PrimitiveType { basic_info, .. } => {
            let name = pq.name();
            let pq_type: ParquetType<'_> = match basic_info {
                bi if bi.logical_type().is_some() => {
                    // we have a logical type, use that
                    match bi.logical_type().unwrap() {
                        LogicalType::String => user_defs
                            .iter()
                            .find(|cd| cd.name == name)
                            .and_then(|cd| {
                                if cd.ty == "VARCHAR" {
                                    Some(ParquetType::Varchar(name, cd.len))
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(ParquetType::Text(name)),
                        LogicalType::Timestamp {
                            is_adjusted_to_u_t_c: true,
                            unit,
                        } => ParquetType::TimestampTz(name, unit),
                        LogicalType::Timestamp {
                            is_adjusted_to_u_t_c: false,
                            unit,
                        } => ParquetType::Timestamp(name, unit),
                        LogicalType::Uuid => ParquetType::Uuid(name),
                        LogicalType::Json => ParquetType::Jsonb(name),
                        LogicalType::Decimal { scale, precision } => {
                            ParquetType::Numeric(name, precision, scale)
                        }
                        _ => ParquetType::Text(name),
                    }
                }
                _ => match pq.get_physical_type() {
                    parquet::basic::Type::FLOAT => ParquetType::Real(name),
                    parquet::basic::Type::DOUBLE => ParquetType::Double(name),
                    parquet::basic::Type::INT32 => ParquetType::Integer(name),
                    parquet::basic::Type::INT64 => ParquetType::BigInt(name),
                    parquet::basic::Type::BOOLEAN => ParquetType::Boolean(name),
                    _ => ParquetType::Text(name),
                },
            };
            Some(pq_type)
        }
    }
}

#[derive(Debug)]
pub enum ArrowArrayRef<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Decimal128(&'a Decimal128Array, u8, i8),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Jsonb(&'a StringArray),
    Utf8(&'a StringArray),
    Boolean(&'a BooleanArray),
    TimestampMillisecond(&'a TimestampMillisecondArray, bool),
    TimestampMicrosecond(&'a TimestampMicrosecondArray, bool),
    TimestampNanosecond(&'a TimestampNanosecondArray, bool),
    FixedSizeBinaryArray(&'a FixedSizeBinaryArray, i32),
}

pub fn downcast_array<'a>((array, pg_type): (&'a ArrayRef, &ParquetType<'a>)) -> ArrowArrayRef<'a> {
    match array.data_type() {
        DataType::Int32 => ArrowArrayRef::Int32(array.as_primitive()),
        DataType::Int64 => ArrowArrayRef::Int64(array.as_primitive()),
        DataType::Float32 => ArrowArrayRef::Float32(array.as_primitive()),
        DataType::Float64 => ArrowArrayRef::Float64(array.as_primitive()),
        DataType::Timestamp(unit, is_utc) => match unit {
            TimeUnit::Millisecond => {
                ArrowArrayRef::TimestampMillisecond(array.as_primitive(), is_utc.is_some())
            }
            TimeUnit::Microsecond => {
                ArrowArrayRef::TimestampMicrosecond(array.as_primitive(), is_utc.is_some())
            }
            TimeUnit::Nanosecond => {
                ArrowArrayRef::TimestampNanosecond(array.as_primitive(), is_utc.is_some())
            }
            _ => unimplemented!(),
        },
        DataType::FixedSizeBinary(size) => {
            ArrowArrayRef::FixedSizeBinaryArray(array.as_fixed_size_binary(), *size)
        }
        DataType::Boolean => ArrowArrayRef::Boolean(array.as_boolean()),
        DataType::Decimal128(precision, scale) => {
            ArrowArrayRef::Decimal128(array.as_primitive(), *precision, *scale)
        }
        DataType::Utf8 => match pg_type {
            &ParquetType::Jsonb(_) => ArrowArrayRef::Jsonb(array.as_string()),
            _ => ArrowArrayRef::Utf8(array.as_string()),
        },
        _ => unimplemented!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_or_relative_path() {
        let cd = std::env::current_dir().unwrap();
        let base_path = format!("file://{}/home/data", cd.display());
        let no_file_base = format!("{}/home/data", cd.display());
        let tests = [
            ("file:///home/data", "file:///home/data", true),
            ("home/data", base_path.as_str(), true),
            (no_file_base.as_str(), base_path.as_str(), true),
        ];
        for (path, expected, is_ok) in tests.iter() {
            let result = base_or_relative_path(path);
            assert_eq!(result.is_ok(), *is_ok);
            if *is_ok {
                assert_eq!(result.unwrap().as_str(), *expected);
            }
        }
    }

    #[tokio::test]
    async fn test_get_file_metadata() {
        let cd = std::env::current_dir().unwrap();
        let path = format!(
            "file://{}/test_data/public.users.snappy.parquet",
            cd.display()
        );
        let result = get_file_metadata(path, None).await;
        assert!(result.is_ok());
        let result = result.unwrap();
        let metadata = result.metadata().file_metadata();
        println!("metadata: {:?}", metadata);
        assert_eq!(metadata.num_rows(), 2);

        // now get the fields
        let fields = get_fields(metadata).unwrap();
        assert_eq!(fields.len(), 6);
    }

    #[tokio::test]
    async fn test_get_file_metadata_invalid_url() {
        let path = "path/to/file".to_string();
        let result = get_file_metadata(path, None).await;
        assert!(result.is_err());
    }
}
