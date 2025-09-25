use serde::{Deserialize, Serialize};

pub mod lua_args;
pub mod parquet_types;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimeUnit {
    Millis,
    Micros,
    Nanos,
}

impl From<&parquet::basic::TimeUnit> for TimeUnit {
    fn from(pq_unit: &parquet::basic::TimeUnit) -> Self {
        match pq_unit {
            parquet::basic::TimeUnit::MILLIS(_) => TimeUnit::Millis,
            parquet::basic::TimeUnit::MICROS(_) => TimeUnit::Micros,
            parquet::basic::TimeUnit::NANOS(_) => TimeUnit::Nanos,
        }
    }
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
/// use ldrs::types::ColumnSchema;
///
/// // Create a Numeric type for a column named "price" with precision 10 and scale 2.
/// let pg_type = ColumnSchema::Numeric("price", 10, 2);
/// // Later, you can use `pg_type` to help generate your PostgreSQL DDL statement.
/// ```
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnSchema<'a> {
    Varchar(&'a str, i32),
    Text(&'a str),
    TimestampTz(&'a str, TimeUnit),
    Timestamp(&'a str, TimeUnit),
    Uuid(&'a str),
    Jsonb(&'a str),
    Numeric(&'a str, i32, i32),
    Real(&'a str),
    // I know doubles do not have scale, but there are situations
    // where we want a double and the underlying arrow is an integer
    // we need the source scale to properly convert it to double
    Double(&'a str, Option<i32>),
    SmallInt(&'a str),
    Integer(&'a str),
    BigInt(&'a str),
    Boolean(&'a str),
    Date(&'a str),
    Custom(&'a str, String), // (column_name, ddl_type)
    Bytea(&'a str),
}

impl<'a> ColumnSchema<'a> {
    pub fn name(&self) -> &'a str {
        match self {
            ColumnSchema::Varchar(name, _) => name,
            ColumnSchema::Text(name) => name,
            ColumnSchema::TimestampTz(name, _) => name,
            ColumnSchema::Timestamp(name, _) => name,
            ColumnSchema::Uuid(name) => name,
            ColumnSchema::Jsonb(name) => name,
            ColumnSchema::Numeric(name, _, _) => name,
            ColumnSchema::Real(name) => name,
            ColumnSchema::Double(name, _) => name,
            ColumnSchema::SmallInt(name) => name,
            ColumnSchema::Integer(name) => name,
            ColumnSchema::BigInt(name) => name,
            ColumnSchema::Boolean(name) => name,
            ColumnSchema::Date(name) => name,
            ColumnSchema::Custom(name, _) => name,
            ColumnSchema::Bytea(name) => name,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ColumnType {
    Varchar(i32),
    Text,
    TimestampTz(TimeUnit),
    Timestamp(TimeUnit),
    Uuid,
    Jsonb,
    Numeric(i32, i32),
    Real,
    Double(Option<i32>),
    SmallInt,
    Integer,
    BigInt,
    Boolean,
    Date,
    Custom(String), // ddl_type
    Bytea,
}

impl From<&ColumnSchema<'_>> for ColumnType {
    fn from(col: &ColumnSchema) -> Self {
        match col {
            ColumnSchema::Varchar(_, i) => ColumnType::Varchar(*i),
            ColumnSchema::Text(_) => ColumnType::Text,
            ColumnSchema::TimestampTz(_, tu) => ColumnType::TimestampTz(tu.clone()),
            ColumnSchema::Timestamp(_, tu) => ColumnType::Timestamp(tu.clone()),
            ColumnSchema::Uuid(_) => ColumnType::Uuid,
            ColumnSchema::Jsonb(_) => ColumnType::Jsonb,
            ColumnSchema::Numeric(_, p, s) => ColumnType::Numeric(*p, *s),
            ColumnSchema::Real(_) => ColumnType::Real,
            ColumnSchema::Double(_, scale) => ColumnType::Double(*scale),
            ColumnSchema::SmallInt(_) => ColumnType::SmallInt,
            ColumnSchema::Integer(_) => ColumnType::Integer,
            ColumnSchema::BigInt(_) => ColumnType::BigInt,
            ColumnSchema::Boolean(_) => ColumnType::Boolean,
            ColumnSchema::Date(_) => ColumnType::Date,
            ColumnSchema::Custom(_, ddl_type) => ColumnType::Custom(ddl_type.clone()),
            ColumnSchema::Bytea(_) => ColumnType::Bytea,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnDefintion {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub length: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileLoadData {
    pub full_url: String,
    pub database: Option<String>,
    pub schema: String,
    pub table: String,
    pub path_parts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ColumnSpec {
    #[serde(rename = "varchar", alias = "VARCHAR")]
    Varchar { name: String, length: i32 },
    #[serde(rename = "text", alias = "TEXT")]
    Text { name: String },
    #[serde(rename = "timestamptz", alias = "TIMESTAMPTZ")]
    TimestampTz { name: String, time_unit: TimeUnit },
    #[serde(rename = "timestamp", alias = "TIMESTAMP")]
    Timestamp { name: String, time_unit: TimeUnit },
    #[serde(rename = "uuid", alias = "UUID")]
    Uuid { name: String },
    #[serde(rename = "jsonb", alias = "JSONB")]
    Jsonb { name: String },
    #[serde(rename = "numeric", alias = "NUMERIC")]
    Numeric {
        name: String,
        precision: i32,
        scale: i32,
    },
    #[serde(rename = "real", alias = "REAL")]
    Real { name: String },
    #[serde(
        rename = "double",
        alias = "DOUBLE",
        alias = "float8",
        alias = "FLOAT8"
    )]
    Double { name: String },
    #[serde(rename = "smallint", alias = "SMALLINT")]
    SmallInt { name: String },
    #[serde(
        rename = "integer",
        alias = "INTEGER",
        alias = "int4",
        alias = "INT4",
        alias = "int",
        alias = "INT"
    )]
    Integer { name: String },
    #[serde(rename = "bigint", alias = "BIGINT", alias = "int8", alias = "INT8")]
    BigInt { name: String },
    #[serde(rename = "boolean", alias = "BOOLEAN")]
    Boolean { name: String },
    #[serde(rename = "date", alias = "DATE")]
    Date { name: String },
    #[serde(rename = "custom", alias = "CUSTOM")]
    Custom { name: String, ddl_type: String },
    #[serde(rename = "bytea", alias = "BYTEA")]
    Bytea { name: String },
}

impl<'a> TryFrom<&'a ColumnSpec> for ColumnSchema<'a> {
    type Error = anyhow::Error;

    fn try_from(spec: &'a ColumnSpec) -> Result<Self, Self::Error> {
        match spec {
            ColumnSpec::Varchar { name, length } => Ok(ColumnSchema::Varchar(name, *length)),
            ColumnSpec::Text { name } => Ok(ColumnSchema::Text(name)),
            ColumnSpec::Numeric {
                name,
                precision,
                scale,
            } => Ok(ColumnSchema::Numeric(name, *precision, *scale)),
            ColumnSpec::Uuid { name } => Ok(ColumnSchema::Uuid(name)),
            ColumnSpec::Timestamp { name, time_unit } => {
                Ok(ColumnSchema::Timestamp(name, time_unit.clone()))
            }
            ColumnSpec::TimestampTz { name, time_unit } => {
                Ok(ColumnSchema::TimestampTz(name, time_unit.clone()))
            }
            ColumnSpec::Boolean { name } => Ok(ColumnSchema::Boolean(name)),
            ColumnSpec::Integer { name } => Ok(ColumnSchema::Integer(name)),
            ColumnSpec::BigInt { name } => Ok(ColumnSchema::BigInt(name)),
            ColumnSpec::SmallInt { name } => Ok(ColumnSchema::SmallInt(name)),
            ColumnSpec::Real { name } => Ok(ColumnSchema::Real(name)),
            ColumnSpec::Double { name } => Ok(ColumnSchema::Double(name, None)),
            ColumnSpec::Date { name } => Ok(ColumnSchema::Date(name)),
            ColumnSpec::Jsonb { name } => Ok(ColumnSchema::Jsonb(name)),
            ColumnSpec::Custom { name, ddl_type } => {
                Ok(ColumnSchema::Custom(name, ddl_type.clone()))
            }
            ColumnSpec::Bytea { name } => Ok(ColumnSchema::Bytea(name)),
        }
    }
}
