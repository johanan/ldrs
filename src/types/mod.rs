use serde::{Deserialize, Serialize};

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
    TimestampTz(&'a str, parquet::basic::TimeUnit),
    Timestamp(&'a str, parquet::basic::TimeUnit),
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
