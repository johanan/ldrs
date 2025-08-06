use serde::{Deserialize, Serialize};

pub mod lua_args;
pub mod parquet_types;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone)]
pub enum ColumnSchema<'a> {
    Varchar(&'a str, i32),
    Text(&'a str),
    TimestampTz(&'a str, TimeUnit),
    Timestamp(&'a str, TimeUnit),
    Uuid(&'a str),
    Jsonb(&'a str),
    Numeric(&'a str, i32, i32),
    Real(&'a str),
    Double(&'a str),
    SmallInt(&'a str),
    Integer(&'a str),
    BigInt(&'a str),
    Boolean(&'a str),
    Date(&'a str),
    Custom(&'a str, &'a str), // (column_name, ddl_type)
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
            ColumnSchema::Double(name) => name,
            ColumnSchema::SmallInt(name) => name,
            ColumnSchema::Integer(name) => name,
            ColumnSchema::BigInt(name) => name,
            ColumnSchema::Boolean(name) => name,
            ColumnSchema::Date(name) => name,
            ColumnSchema::Custom(name, _) => name,
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct MvrColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub length: Option<i32>,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileLoadData {
    pub full_url: String,
    pub database: Option<String>,
    pub schema: String,
    pub table: String,
    pub path_parts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMapping {
    pub name: String,
    pub column_type: String,       // Type name (case-insensitive)
    pub processor: Option<String>, // Processor for data conversion
    pub precision: Option<i32>,    // For NUMERIC types
    pub scale: Option<i32>,        // For NUMERIC types
    pub length: Option<i32>,       // For VARCHAR types
    pub time_unit: Option<String>, // For TIMESTAMP types: "millis", "micros", "nanos"
}

use std::collections::HashMap;

impl ColumnMapping {
    pub fn to_column_schema<'a>(
        &'a self,
        name: &'a str,
    ) -> Result<ColumnSchema<'a>, anyhow::Error> {
        let type_map: HashMap<
            &str,
            for<'b> fn(&ColumnMapping, &'b str) -> Result<ColumnSchema<'b>, anyhow::Error>,
        > = [
            (
                "varchar",
                Self::parse_varchar
                    as for<'b> fn(
                        &ColumnMapping,
                        &'b str,
                    ) -> Result<ColumnSchema<'b>, anyhow::Error>,
            ),
            ("text", Self::parse_text),
            ("jsonb", Self::parse_jsonb),
            ("numeric", Self::parse_numeric),
            ("uuid", Self::parse_uuid),
            ("timestamp", Self::parse_timestamp),
            ("timestamptz", Self::parse_timestamptz),
            ("boolean", Self::parse_boolean),
            ("integer", Self::parse_integer),
            ("bigint", Self::parse_bigint),
            ("smallint", Self::parse_smallint),
            ("real", Self::parse_real),
            ("double", Self::parse_double),
            ("date", Self::parse_date),
        ]
        .iter()
        .cloned()
        .collect();

        // Find matching parser (case-insensitive)
        let parser = type_map
            .iter()
            .find(|(key, _)| self.column_type.eq_ignore_ascii_case(key))
            .map(|(_, parser)| *parser);

        match parser {
            Some(parse_fn) => Ok(parse_fn(self, name)?),
            None => Ok(ColumnSchema::Custom(name, &self.column_type)),
        }
    }

    // Individual type parsers that return ColumnSchema directly
    fn parse_varchar<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        let length = self.length.ok_or_else(|| {
            anyhow::anyhow!(
                "VARCHAR requires length parameter for column '{}'",
                self.name
            )
        })?;
        Ok(ColumnSchema::Varchar(name, length))
    }

    fn parse_text<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        Ok(ColumnSchema::Text(name))
    }

    fn parse_jsonb<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        Ok(ColumnSchema::Jsonb(name))
    }

    fn parse_numeric<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        let precision = self.precision.ok_or_else(|| {
            anyhow::anyhow!(
                "NUMERIC requires precision parameter for column '{}'",
                self.name
            )
        })?;
        let scale = self.scale.ok_or_else(|| {
            anyhow::anyhow!(
                "NUMERIC requires scale parameter for column '{}'",
                self.name
            )
        })?;
        Ok(ColumnSchema::Numeric(name, precision, scale))
    }

    fn parse_uuid<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        Ok(ColumnSchema::Uuid(name))
    }

    fn parse_timestamp<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        let time_unit = self.parse_time_unit()?;
        Ok(ColumnSchema::Timestamp(name, time_unit))
    }

    fn parse_timestamptz<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        let time_unit = self.parse_time_unit()?;
        Ok(ColumnSchema::TimestampTz(name, time_unit))
    }

    fn parse_time_unit(&self) -> Result<TimeUnit, anyhow::Error> {
        match self.time_unit.as_deref() {
            Some(unit) if unit.eq_ignore_ascii_case("millis") => Ok(TimeUnit::Millis),
            Some(unit) if unit.eq_ignore_ascii_case("micros") => Ok(TimeUnit::Micros),
            Some(unit) if unit.eq_ignore_ascii_case("nanos") => Ok(TimeUnit::Nanos),
            Some(unit) => Err(anyhow::anyhow!(
                "Invalid time unit '{}' for column '{}'. Valid options: millis, micros, nanos",
                unit,
                self.name
            )),
            None => Ok(TimeUnit::Millis), // Default to milliseconds
        }
    }

    fn parse_boolean<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        Ok(ColumnSchema::Boolean(name))
    }

    fn parse_integer<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        Ok(ColumnSchema::Integer(name))
    }

    fn parse_bigint<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        Ok(ColumnSchema::BigInt(name))
    }

    fn parse_smallint<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        Ok(ColumnSchema::SmallInt(name))
    }

    fn parse_real<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        Ok(ColumnSchema::Real(name))
    }

    fn parse_double<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        Ok(ColumnSchema::Double(name))
    }

    fn parse_date<'a>(&self, name: &'a str) -> Result<ColumnSchema<'a>, anyhow::Error> {
        Ok(ColumnSchema::Date(name))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: Vec<ColumnMapping>,
}
