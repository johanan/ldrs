pub mod client;
pub mod config;
pub mod mvr_config;
pub mod postgres_destination;
pub mod schema;
pub mod schema_change;

use crate::arrow_access::TypedColumnAccessor;
use crate::ldrs_arrow::fill_vec_with_none;
use crate::ldrs_arrow::flatten_arrow_transforms_zip;
use crate::ldrs_arrow::flatten_schema_zip;
use crate::ldrs_arrow::get_sf_arrow_schema;
use crate::ldrs_arrow::map_arrow_to_abstract;
use crate::ldrs_arrow::mvr_to_stream;
use crate::ldrs_schema::SchemaChange;
use crate::parquet_provider::builder_from_string;
use crate::pq::get_column_schema;
use crate::types::ColumnSchema;
use crate::types::ColumnType;
use anyhow::Context;
use arrow_array::RecordBatch;
use chrono::{DateTime, NaiveDateTime, Utc};
use clap::Subcommand;
use client::create_connection;
use config::{LoadArgs, PGFileLoad, PGFileLoadArgs, ProcessedPGFileLoad};
use futures::StreamExt;
use futures::TryStreamExt;
use mvr_config::{load_config, MvrConfig};
use pg_bigdecimal::PgNumeric;
use postgres_types::{to_sql_checked, ToSql, Type};
use serde_json::Value;
use std::iter::zip;
use std::pin::pin;
use std::pin::Pin;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tracing::info;

#[derive(Subcommand)]
pub enum PgCommands {
    /// Load data into PostgreSQL from a file
    Load(LoadArgs),
    /// Load data into PostgreSQL using a configuration file
    Config(PGFileLoadArgs),
    /// Load data into PostgreSQL using MVR configuration
    Mvr(MvrConfig),
}

#[derive(Debug)]
pub struct LoadDefintion {
    pub schema_ddl: String,
    pub drop_ddl: String,
    pub create_ddl: String,
    pub drop_target_ddl: String,
    pub set_search_ddl: String,
    pub rename_ddl: String,
    pub binary_ddl: String,
    pub post_sql: Option<String>,
    pub role_sql: Option<String>,
}
impl LoadDefintion {
    pub fn create_batch(&self) -> String {
        format!("{} {} {}", self.schema_ddl, self.drop_ddl, self.create_ddl)
    }

    pub fn rename_and_move(&self) -> String {
        format!(
            "{} {} {}",
            self.drop_target_ddl, self.set_search_ddl, self.rename_ddl
        )
    }
}

pub fn load_table_name<'a>(schema: &'a str, table: &'a str) -> String {
    format!("{}.{}_load", schema, table)
}

pub fn build_fqtn(table_name: &str) -> Result<(&str, &str), anyhow::Error> {
    let fqtn_tup = table_name.split_once('.');
    match fqtn_tup {
        Some((schema, table)) => Ok((schema, table)),
        None => Err(anyhow::Error::msg("Invalid table name")),
    }
}

pub fn build_ddl(load_table: String, fields: &[String]) -> String {
    let mut ddl = format!("CREATE TABLE if not exists {} (", load_table);
    let fields_ddl = fields.join(", ");
    ddl.push_str(&fields_ddl);
    ddl.push_str(");");
    ddl
}

pub fn map_col_schema_to_pg_type(pq: &ColumnSchema) -> postgres_types::Type {
    match pq {
        ColumnSchema::SmallInt(_) => postgres_types::Type::INT2,
        ColumnSchema::BigInt(_) => postgres_types::Type::INT8,
        ColumnSchema::Boolean(_) => postgres_types::Type::BOOL,
        ColumnSchema::Double(_, _) => postgres_types::Type::FLOAT8,
        ColumnSchema::Integer(_) => postgres_types::Type::INT4,
        ColumnSchema::Jsonb(_) => postgres_types::Type::JSONB,
        ColumnSchema::Numeric(_, _, _) => postgres_types::Type::NUMERIC,
        ColumnSchema::Timestamp(_, _) => postgres_types::Type::TIMESTAMP,
        ColumnSchema::TimestampTz(_, _) => postgres_types::Type::TIMESTAMPTZ,
        ColumnSchema::Date(_) => postgres_types::Type::DATE,
        ColumnSchema::Real(_) => postgres_types::Type::FLOAT4,
        ColumnSchema::Text(_) => postgres_types::Type::TEXT,
        ColumnSchema::Uuid(_) => postgres_types::Type::UUID,
        ColumnSchema::Varchar(_, _) => postgres_types::Type::VARCHAR,
        ColumnSchema::Custom(_, _) => postgres_types::Type::VARCHAR,
        ColumnSchema::Bytea(_) => postgres_types::Type::BYTEA,
    }
}

pub fn build_definition<'a>(
    full_table: &'a str,
    types: &[ColumnSchema<'a>],
    post_sql: Option<String>,
    role: Option<String>,
) -> Result<LoadDefintion, anyhow::Error> {
    let (schema, table) = build_fqtn(full_table)?;
    let load_table = load_table_name(schema, table);
    let schema_ddl = format!("CREATE SCHEMA IF NOT EXISTS {};", schema);
    let drop_ddl = format!("DROP TABLE IF EXISTS {};", load_table);
    let schema_change = SchemaChange::build_from_columns(&[], &types);
    let fields_ddl: Vec<String> = schema_change.to_postgres_final_column_ddl();
    let create_ddl = build_ddl(load_table.clone(), &fields_ddl);
    let drop_target_ddl = format!("DROP TABLE IF EXISTS {}.{};", schema, table);
    let set_search_ddl = format!("SET search_path TO {};", schema);
    let rename_ddl = format!("ALTER TABLE {} rename to {};", load_table, table);
    let binary_ddl = format!("COPY {} FROM STDIN BINARY;", load_table);
    let role_sql = role.map(|r| format!("SET ROLE {};", r));
    Ok(LoadDefintion {
        schema_ddl,
        drop_ddl,
        create_ddl,
        drop_target_ddl,
        set_search_ddl,
        rename_ddl,
        binary_ddl,
        post_sql,
        role_sql,
    })
}

pub async fn prepare_to_copy(
    client: &mut tokio_postgres::Client,
    definition: &LoadDefintion,
) -> Result<(), anyhow::Error> {
    let tx = client
        .transaction()
        .await
        .with_context(|| "Could not create transaction")?;
    // set role for the transaction
    if let Some(sql) = &definition.role_sql {
        tx.batch_execute(sql).await?;
    }
    tx.batch_execute(&definition.create_batch())
        .await
        .with_context(|| "Could not create table")?;
    tx.commit()
        .await
        .with_context(|| "Could not commit transaction")?;
    Ok(())
}

pub async fn process_record_batch<'a>(
    writer: &mut Pin<&mut BinaryCopyInWriter>,
    batch: &RecordBatch,
    parquet_types: &[ColumnSchema<'a>],
    transforms: &[Option<ColumnType>],
) -> Result<(), anyhow::Error> {
    let num_rows = batch.num_rows();
    let columns = batch.columns();

    // Create TypedColumnAccessor for each column
    let accessors: Vec<TypedColumnAccessor> =
        columns.iter().map(TypedColumnAccessor::new).collect();

    let converters = accessors
        .iter()
        .zip(parquet_types.iter().zip(transforms.iter()))
        .map(|(accessor, (col_schema, transform))| {
            ColumnConverter::new(accessor, col_schema, transform.as_ref())
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut batch_buffer = Vec::<Vec<ExtractedValue>>::with_capacity(num_rows);

    // Process each row
    for row_idx in 0..num_rows {
        let row_values: Vec<ExtractedValue> = converters
            .iter()
            .map(|converter| converter.extract_value(row_idx))
            .collect();

        batch_buffer.push(row_values);
    }

    for row in batch_buffer.iter() {
        writer
            .as_mut()
            .write_raw(row)
            .await
            .with_context(|| "Failed to write row to PostgreSQL")?;
    }

    Ok(())
}

pub async fn execute_binary_copy<'a, S, E>(
    client: &mut tokio_postgres::Client,
    definition: &LoadDefintion,
    columns: &[ColumnSchema<'a>],
    transforms: &[Option<ColumnType>],
    stream: S,
) -> Result<(), anyhow::Error>
where
    S: futures::TryStream<Ok = RecordBatch, Error = E> + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    let pg_types = columns
        .iter()
        .map(map_col_schema_to_pg_type)
        .collect::<Vec<_>>();

    let sink_tx = client
        .transaction()
        .await
        .with_context(|| "Could not create transaction")?;

    if let Some(sql) = &definition.role_sql {
        sink_tx.batch_execute(sql).await?;
    }

    let sink = sink_tx
        .copy_in(&definition.binary_ddl)
        .await
        .with_context(|| "Could not create binary copy")?;
    let writer = BinaryCopyInWriter::new(sink, &pg_types);
    let mut pinned_writer = pin!(writer);

    let mut stream = stream.into_stream();
    let mut stream = pin!(stream);

    while let Some(batch_result) = stream.next().await {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => return Err(anyhow::anyhow!("Stream error: {}", e)),
        };

        process_record_batch(&mut pinned_writer, &batch, columns, transforms).await?;
    }

    pinned_writer
        .finish()
        .await
        .with_context(|| "Could not finish copy")?;

    sink_tx.batch_execute(&definition.rename_and_move()).await?;
    if let Some(sql) = &definition.post_sql {
        sink_tx.batch_execute(sql).await?;
    }

    sink_tx
        .commit()
        .await
        .with_context(|| "Could not commit transaction")?;

    Ok(())
}

pub async fn load_postgres<'a, S, E>(
    pq_types: &[ColumnSchema<'a>],
    transforms: &[Option<ColumnType>],
    table: &str,
    post_sql: Option<String>,
    pg_url: &str,
    role: Option<String>,
    stream: S,
) -> Result<(), anyhow::Error>
where
    S: futures::TryStream<Ok = RecordBatch, Error = E> + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    let definition = build_definition(table, pq_types, post_sql, role)?;
    info!("PG definition: {:?}", definition);

    let mut client = create_connection(pg_url).await?;
    prepare_to_copy(&mut client, &definition).await?;

    execute_binary_copy(&mut client, &definition, pq_types, transforms, stream).await?;

    Ok(())
}

pub async fn load_postgres_cmd(
    args: &LoadArgs,
    pg_url: String,
    handle: tokio::runtime::Handle,
) -> Result<(), anyhow::Error> {
    let builder = builder_from_string(args.file.clone(), handle).await?;
    let file_md = builder.metadata().file_metadata().clone();
    info!("num rows: {:?}", file_md.num_rows());

    let mapped = get_column_schema(&file_md)?;
    let transforms = vec![None; mapped.len()];

    let stream = builder.with_batch_size(args.batch_size).build()?;
    load_postgres(
        &mapped,
        &transforms,
        &args.table,
        args.post_sql.clone(),
        &pg_url,
        args.role.clone(),
        stream,
    )
    .await?;
    Ok(())
}

pub async fn load_from_file(
    args: PGFileLoadArgs,
    pg_url: String,
    handle: tokio::runtime::Handle,
) -> Result<(), anyhow::Error> {
    let pg_file_load: ProcessedPGFileLoad = std::fs::read_to_string(args.config_path.clone())
        .with_context(|| "Unable to read file")
        .and_then(|f| {
            serde_yaml::from_str::<'_, PGFileLoad>(&f).with_context(|| "Unable to parse yaml")
        })
        .map(|pg_file_load| pg_file_load.merge_cli_args(args))
        .and_then(|pg_file_load| pg_file_load.try_into())?;

    let total_tasks = pg_file_load.tables.len();
    for (i, pg_load) in pg_file_load.tables.iter().enumerate() {
        let task_start = std::time::Instant::now();
        info!("Running task: {}/{}", i + 1, total_tasks);
        load_postgres_cmd(pg_load, pg_url.clone(), handle.clone()).await?;
        let task_end = std::time::Instant::now();
        info!("Task time: {:?}", task_end - task_start);
    }
    Ok(())
}

/// Load data into PostgreSQL using MVR configuration file
///
/// This function reads an MVR configuration file, processes each configuration item,
/// executes the SQL query in each, and loads the resulting data into PostgreSQL tables.
/// It handles the full workflow from MVR config to data loading.
///
/// # Arguments
///
/// * `args` - MVR configuration including file path and optional overrides
/// * `pg_url` - PostgreSQL connection URL
///
/// # Returns
///
/// * `Result<(), anyhow::Error>`
pub async fn load_from_mvr_config(args: MvrConfig, pg_url: String) -> Result<(), anyhow::Error> {
    let tables = load_config(&args)?;
    let total_tasks = tables.len();
    for (i, config) in tables.iter().enumerate() {
        let task_start = std::time::Instant::now();
        info!("Running task: {}/{}", i + 1, total_tasks);
        let mvr_arrow = mvr_to_stream(&config.sql).await?;
        let schema = mvr_arrow.schema_stream.await;
        //info!("Schema: {:?}", schema);
        let config_cols = config
            .columns
            .iter()
            .map(ColumnSchema::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        match schema {
            Some(schema) => {
                let filled_cols = fill_vec_with_none(&schema.fields, config_cols);
                info!("Filled cols: {:?}", filled_cols);

                let sf_schema = schema
                    .fields()
                    .iter()
                    .map(get_sf_arrow_schema)
                    .collect::<Vec<_>>();
                info!("SF Schema: {:?}", sf_schema);

                let mapped = schema
                    .fields()
                    .iter()
                    .filter_map(map_arrow_to_abstract)
                    .collect::<Vec<_>>();
                info!("Mapped cols: {:?}", mapped);
                let source_types = mapped.iter().map(ColumnType::from).collect::<Vec<_>>();
                // let's start zipping, returning early if any columns do not work together
                let mapped_option = mapped.into_iter().map(Some).collect::<Vec<_>>();
                let override_cols = flatten_schema_zip(zip(
                    flatten_schema_zip(zip(mapped_option, sf_schema)),
                    filled_cols,
                ))
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
                info!("Override cols: {:?}", override_cols);
                // see if we need to translate any columns
                let final_types = override_cols
                    .iter()
                    .map(ColumnType::from)
                    .collect::<Vec<_>>();
                let transforms = flatten_arrow_transforms_zip(zip(source_types, final_types));
                info!("Transforms: {:?}", transforms);

                anyhow::ensure!(
                    override_cols.len() == schema.fields().len(),
                    "Final columns length does not match schema fields length"
                );

                load_postgres(
                    &override_cols,
                    &transforms,
                    &config.table,
                    config.post_sql.clone(),
                    &pg_url,
                    config.role.clone(),
                    mvr_arrow.batch_stream,
                )
                .await?
            }
            None => {
                info!("No schema found. Most likely mvr failed.");
            }
        }
        match mvr_arrow.command_handle.await {
            Ok(Ok(())) => (), // Command completed successfully
            Ok(Err(e)) => return Err(anyhow::anyhow!("Command failed: {}", e)),
            Err(e) => return Err(anyhow::anyhow!("Command task panicked: {}", e)),
        }
        let task_end = std::time::Instant::now();
        info!("Task time: {:?}", task_end - task_start);
    }
    Ok(())
}

#[derive(Debug)]
pub enum ExtractedValue<'a> {
    Boolean(Option<bool>),
    Int64(Option<i64>),
    Int32(Option<i32>),
    Double(Option<f64>),
    Real(Option<f32>),
    Decimal(Option<PgNumeric>),
    Uuid(Option<uuid::Uuid>),
    Utf8(Option<&'a str>),
    TimestampMillis(Option<NaiveDateTime>),
    TimestampMicros(Option<NaiveDateTime>),
    TimestampNanos(Option<NaiveDateTime>),
    TimestampTzMillis(Option<DateTime<Utc>>),
    TimestampTzMicros(Option<DateTime<Utc>>),
    TimestampTzNanos(Option<DateTime<Utc>>),
    Jsonb(Option<Value>),
}

#[derive(Debug)]
enum ConversionStrategy {
    Boolean,
    BigInt,
    Integer,
    Double { scale: Option<i32> },
    Real,
    Text,
    Numeric { precision: i32, scale: i32 },
    TimestampMillis,
    TimestampMicros,
    TimestampNanos,
    TimestampTzMillis,
    TimestampTzMicros,
    TimestampTzNanos,
    TransformBigInt,
    TransformInteger,
    TransformUuid,
    TransformJsonb,
    TransformNumeric { precision: i32, scale: i32 },
    TransformDouble { scale: Option<i32> },
}

#[derive(Debug)]
struct ColumnConverter<'a> {
    accessor: &'a TypedColumnAccessor<'a>,
    strategy: ConversionStrategy,
}

impl<'a> ColumnConverter<'a> {
    fn new(
        accessor: &'a TypedColumnAccessor<'a>,
        col_schema: &ColumnSchema,
        transform: Option<&ColumnType>,
    ) -> Result<Self, anyhow::Error> {
        let strategy = match transform {
            // all of these have an output type that is not the same as the arrow array type
            Some(ColumnType::BigInt) => ConversionStrategy::TransformBigInt,
            Some(ColumnType::Integer) => ConversionStrategy::TransformInteger,
            // I know doubles do not have scale, but this could be a different type underneath
            Some(ColumnType::Double(precision)) => {
                ConversionStrategy::TransformDouble { scale: *precision }
            }
            Some(ColumnType::Numeric(precision, scale)) => ConversionStrategy::TransformNumeric {
                precision: *precision,
                scale: *scale,
            },
            Some(ColumnType::Uuid) => ConversionStrategy::TransformUuid,
            Some(ColumnType::Jsonb) => ConversionStrategy::TransformJsonb,
            Some(ColumnType::TimestampTz(time_unit)) => match time_unit {
                crate::types::TimeUnit::Millis => ConversionStrategy::TimestampTzMillis,
                crate::types::TimeUnit::Micros => ConversionStrategy::TimestampTzMicros,
                crate::types::TimeUnit::Nanos => ConversionStrategy::TimestampTzNanos,
            },
            None => match col_schema {
                // these all have the same output as arrow array type
                ColumnSchema::Boolean(_) => ConversionStrategy::Boolean,
                ColumnSchema::BigInt(_) => ConversionStrategy::BigInt,
                ColumnSchema::Integer(_) => ConversionStrategy::Integer,
                ColumnSchema::Double(_, scale) => ConversionStrategy::Double {
                    scale: scale.clone(),
                },
                ColumnSchema::Text(_) | ColumnSchema::Varchar(_, _) => ConversionStrategy::Text,
                ColumnSchema::Numeric(_, precision, scale) => ConversionStrategy::Numeric {
                    precision: *precision,
                    scale: *scale,
                },
                ColumnSchema::Timestamp(_, crate::types::TimeUnit::Millis) => {
                    ConversionStrategy::TimestampMillis
                }
                ColumnSchema::Timestamp(_, crate::types::TimeUnit::Micros) => {
                    ConversionStrategy::TimestampMicros
                }
                ColumnSchema::Timestamp(_, crate::types::TimeUnit::Nanos) => {
                    ConversionStrategy::TimestampNanos
                }
                ColumnSchema::TimestampTz(_, crate::types::TimeUnit::Millis) => {
                    ConversionStrategy::TimestampTzMillis
                }
                ColumnSchema::TimestampTz(_, crate::types::TimeUnit::Micros) => {
                    ConversionStrategy::TimestampTzMicros
                }
                ColumnSchema::TimestampTz(_, crate::types::TimeUnit::Nanos) => {
                    ConversionStrategy::TimestampTzNanos
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported column schema: {:?}",
                        col_schema
                    ))
                }
            },
            _ => return Err(anyhow::anyhow!("Unsupported transform: {:?}", transform)),
        };

        Ok(ColumnConverter { accessor, strategy })
    }

    #[inline]
    fn extract_value(&self, row_idx: usize) -> ExtractedValue<'_> {
        match &self.strategy {
            ConversionStrategy::Boolean => unsafe {
                ExtractedValue::Boolean(self.accessor.Boolean(row_idx))
            },
            ConversionStrategy::BigInt => unsafe {
                ExtractedValue::Int64(self.accessor.Int64(row_idx))
            },
            ConversionStrategy::Integer => unsafe {
                ExtractedValue::Int32(self.accessor.Int32(row_idx))
            },
            ConversionStrategy::Double { scale: _ } => unsafe {
                ExtractedValue::Double(self.accessor.Float64(row_idx))
            },
            ConversionStrategy::Text => unsafe {
                ExtractedValue::Utf8(self.accessor.Utf8(row_idx))
            },
            ConversionStrategy::Numeric { precision, scale } => {
                ExtractedValue::Decimal(self.accessor.as_pg_numeric(row_idx, *scale))
            }
            ConversionStrategy::TransformBigInt => {
                ExtractedValue::Int64(self.accessor.as_int64(row_idx))
            }
            ConversionStrategy::TransformInteger => {
                ExtractedValue::Int32(self.accessor.as_int32(row_idx))
            }
            ConversionStrategy::TransformUuid => unsafe {
                ExtractedValue::Uuid(self.accessor.as_uuid(row_idx))
            },
            ConversionStrategy::TransformJsonb => {
                ExtractedValue::Jsonb(self.accessor.as_serde(row_idx))
            }
            ConversionStrategy::TransformNumeric { precision, scale } => {
                ExtractedValue::Decimal(self.accessor.as_pg_numeric(row_idx, *scale))
            }
            ConversionStrategy::TransformDouble { scale } => {
                ExtractedValue::Double(self.accessor.as_float64(row_idx, *scale))
            }
            ConversionStrategy::TimestampMillis => {
                ExtractedValue::TimestampMillis(self.accessor.as_chrono_naive(row_idx))
            }
            ConversionStrategy::TimestampMicros => {
                ExtractedValue::TimestampMicros(self.accessor.as_chrono_naive(row_idx))
            }
            ConversionStrategy::TimestampNanos => {
                ExtractedValue::TimestampNanos(self.accessor.as_chrono_naive(row_idx))
            }
            ConversionStrategy::TimestampTzMillis => unsafe {
                ExtractedValue::TimestampTzMillis(self.accessor.as_chrono_tz(row_idx))
            },
            ConversionStrategy::TimestampTzMicros => unsafe {
                ExtractedValue::TimestampTzMicros(self.accessor.as_chrono_tz(row_idx))
            },
            ConversionStrategy::TimestampTzNanos => unsafe {
                ExtractedValue::TimestampTzNanos(self.accessor.as_chrono_tz(row_idx))
            },
            _ => panic!("Unsupported conversion strategy: {:?}", self.strategy),
        }
    }
}

impl<'a> ToSql for ExtractedValue<'a> {
    #[inline]
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            ExtractedValue::Boolean(v) => v.to_sql(ty, out),
            ExtractedValue::Int64(v) => v.to_sql(ty, out),
            ExtractedValue::Int32(v) => v.to_sql(ty, out),
            ExtractedValue::Double(v) => v.to_sql(ty, out),
            ExtractedValue::Uuid(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampMillis(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampMicros(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampNanos(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampTzMillis(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampTzMicros(v) => v.to_sql(ty, out),
            ExtractedValue::TimestampTzNanos(v) => v.to_sql(ty, out),
            ExtractedValue::Real(v) => v.to_sql(ty, out),
            ExtractedValue::Decimal(v) => v.to_sql(ty, out),
            ExtractedValue::Utf8(v) => v.to_sql(ty, out),
            ExtractedValue::Jsonb(v) => v.to_sql(ty, out),
            _ => Err(format!("Unsupported ExtractedValue variant: {:?}", self).into()),
        }
    }

    to_sql_checked!();

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::parquet_provider::builder_from_string;
    use crate::types::TimeUnit;
    use crate::{
        parquet_provider,
        pq::{self, get_fields, map_parquet_to_abstract},
        test_utils::{create_runtime, drop_runtime},
    };

    use super::*;

    #[tokio::test]
    async fn test_definition_build() {
        let full_table = "public.users";
        let pq_types = [
            ColumnSchema::BigInt("id"),
            ColumnSchema::Text("name"),
            ColumnSchema::Timestamp("created_at", TimeUnit::Millis),
        ];

        let first_def = build_definition(full_table, &pq_types, None, None).unwrap();
        assert_eq!(first_def.schema_ddl, "CREATE SCHEMA IF NOT EXISTS public;");
        assert_eq!(
            first_def.drop_ddl,
            "DROP TABLE IF EXISTS public.users_load;"
        );
        assert_eq!(first_def.create_ddl, "CREATE TABLE if not exists public.users_load (id bigint, name text, created_at timestamp);");
        assert_eq!(
            first_def.drop_target_ddl,
            "DROP TABLE IF EXISTS public.users;"
        );
        assert_eq!(first_def.set_search_ddl, "SET search_path TO public;");
        assert_eq!(
            first_def.rename_ddl,
            "ALTER TABLE public.users_load rename to users;"
        );
        assert_eq!(
            first_def.binary_ddl,
            "COPY public.users_load FROM STDIN BINARY;"
        );
    }

    #[tokio::test]
    async fn full_round_trip() {
        let cd = std::env::current_dir().unwrap();
        let path = format!("{}/test_data/public.users.snappy.parquet", cd.display());
        let rt = create_runtime();
        let builder = parquet_provider::builder_from_string(path.clone(), rt.handle().clone())
            .await
            .unwrap();
        let file_md = builder.metadata().file_metadata().clone();
        let kv = pq::get_kv_fields(&file_md);

        let fields = get_fields(&file_md).unwrap();
        let mapped = fields
            .iter()
            .filter_map(|pq| map_parquet_to_abstract(pq, &kv))
            .collect::<Vec<ColumnSchema>>();
        let transforms = vec![
            None,
            None,
            None,
            Some(ColumnType::Uuid),
            Some(ColumnType::Uuid),
            None,
        ];
        let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
        let stream = builder.with_batch_size(1024).build().unwrap();

        load_postgres(
            &mapped,
            &transforms,
            "not_public.users_test",
            None,
            pg_url,
            Some(String::from("test_role")),
            stream,
        )
        .await
        .unwrap();

        // check table owner
        let client = create_connection(pg_url).await.unwrap();
        let owner = client
            .query_one("SELECT tableowner FROM pg_catalog.pg_tables WHERE schemaname = 'not_public' AND tablename = 'users_test'", &[])
            .await
            .unwrap();
        assert_eq!(owner.get::<_, String>(0), "test_role");

        // run it again to ensure the process can load a table that exists
        let post_sql = "CREATE INDEX ON not_public.users_test (unique_id); CREATE UNIQUE INDEX ON not_public.users_test (name);";
        let builder = builder_from_string(path, rt.handle().clone())
            .await
            .unwrap();
        let stream = builder.with_batch_size(1024).build().unwrap();
        load_postgres(
            &mapped,
            &transforms,
            "not_public.users_test",
            Some(post_sql.to_string()),
            pg_url,
            None,
            stream,
        )
        .await
        .unwrap();

        let row_count = client
            .query_one("select count(*) from not_public.users_test", &[])
            .await
            .unwrap();
        assert_eq!(row_count.get::<_, i64>(0), 2);

        let check_indexes = client
            .query_one(
                "select count(*) from pg_indexes where tablename = 'users_test'",
                &[],
            )
            .await
            .unwrap();
        // should be user as no role is sent
        let owner = client
            .query_one("SELECT tableowner FROM pg_catalog.pg_tables WHERE schemaname = 'not_public' AND tablename = 'users_test'", &[])
            .await
            .unwrap();
        assert_eq!(owner.get::<_, String>(0), "postgres");
        assert_eq!(2, check_indexes.get::<_, i64>(0));
        drop_runtime(rt);
    }
}
