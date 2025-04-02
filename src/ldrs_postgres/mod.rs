pub mod config;

use crate::parquet_provider::builder_from_string;
use crate::pq::{
    downcast_array, get_fields, get_kv_fields, map_parquet_to_abstract, ArrowArrayRef, ParquetType,
};
use anyhow::Context;
use arrow::datatypes::ArrowNativeType;
use arrow_array::RecordBatch;
use arrow_array::{Array, ArrayAccessor};
use bigdecimal::FromPrimitive;
use chrono::{DateTime, NaiveDateTime, Utc};
use config::{LoadArgs, PGFileLoad, PGFileLoadArgs, ProcessedPGFileLoad};
use futures::TryStreamExt;
use native_tls::TlsConnector;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStream};
use pg_bigdecimal::{BigDecimal, BigInt};
use postgres_native_tls::MakeTlsConnector;
use postgres_types::{to_sql_checked, ToSql, Type};
use serde_json::Value;
use std::{pin::pin, sync::Arc};
use tokio::sync::Mutex;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tracing::{debug, info};

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

pub async fn create_connection(conn_url: &str) -> Result<tokio_postgres::Client, anyhow::Error> {
    let connector = TlsConnector::new().with_context(|| "Could not create TLS connector")?;
    let connector = MakeTlsConnector::new(connector);
    let (client, connection) = tokio_postgres::connect(conn_url, connector).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(client)
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

pub fn map_parquet_to_ddl(pq: &ParquetType) -> String {
    match pq {
        ParquetType::BigInt(name) => format!("{} bigint", name),
        ParquetType::Boolean(name) => format!("{} boolean", name),
        ParquetType::Double(name) => format!("{} double precision", name),
        ParquetType::Integer(name) => format!("{} integer", name),
        ParquetType::Jsonb(name) => format!("{} jsonb", name),
        ParquetType::Numeric(name, precision, scale) => {
            format!("{} numeric({}, {})", name, precision, scale)
        }
        ParquetType::Timestamp(name, ..) => format!("{} timestamp", name),
        ParquetType::TimestampTz(name, ..) => format!("{} timestamptz", name),
        ParquetType::Real(name) => format!("{} real", name),
        ParquetType::Text(name) => format!("{} text", name),
        ParquetType::Uuid(name) => format!("{} uuid", name),
        ParquetType::Varchar(name, size) => format!("{} varchar({})", name, size),
    }
}

pub fn map_parquet_to_pg_type(pq: &ParquetType) -> postgres_types::Type {
    match pq {
        ParquetType::BigInt(_) => postgres_types::Type::INT8,
        ParquetType::Boolean(_) => postgres_types::Type::BOOL,
        ParquetType::Double(_) => postgres_types::Type::FLOAT8,
        ParquetType::Integer(_) => postgres_types::Type::INT4,
        ParquetType::Jsonb(_) => postgres_types::Type::JSONB,
        ParquetType::Numeric(_, _, _) => postgres_types::Type::NUMERIC,
        ParquetType::Timestamp(_, _) => postgres_types::Type::TIMESTAMP,
        ParquetType::TimestampTz(_, _) => postgres_types::Type::TIMESTAMPTZ,
        ParquetType::Real(_) => postgres_types::Type::FLOAT4,
        ParquetType::Text(_) => postgres_types::Type::TEXT,
        ParquetType::Uuid(_) => postgres_types::Type::UUID,
        ParquetType::Varchar(_, _) => postgres_types::Type::VARCHAR,
    }
}

pub fn build_definition<'a>(
    full_table: &'a str,
    types: &[ParquetType<'a>],
    post_sql: Option<String>,
    role: Option<String>,
) -> Result<LoadDefintion, anyhow::Error> {
    let (schema, table) = build_fqtn(full_table)?;
    let load_table = load_table_name(schema, table);
    let schema_ddl = format!("CREATE SCHEMA IF NOT EXISTS {};", schema);
    let drop_ddl = format!("DROP TABLE IF EXISTS {};", load_table);
    let fields_ddl: Vec<String> = types.iter().map(map_parquet_to_ddl).collect();
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

pub async fn execute_binary_copy<'a>(
    client: &mut tokio_postgres::Client,
    definition: &LoadDefintion,
    columns: &[ParquetType<'a>],
    stream: ParquetRecordBatchStream<ParquetObjectReader>,
) -> Result<(), anyhow::Error> {
    let pg_types = columns
        .iter()
        .map(map_parquet_to_pg_type)
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
    let pinned_writer = pin!(writer);
    let pinned_writer = Arc::new(Mutex::new(pinned_writer));
    let rename_ddl = definition.rename_and_move().clone();

    stream
        .try_for_each_concurrent(4, {
            |batch: RecordBatch| {
                let pinned_writer = Arc::clone(&pinned_writer);
                async move {
                    let num_rows = batch.num_rows();
                    let num_cols = batch.num_columns();
                    debug!("num_rows: {:?}, num_cols: {:?}", num_rows, num_cols);
                    let mut batch_buffer = Vec::<Vec<PgValue<'_>>>::with_capacity(num_rows);
                    // iterate over the rows and then the columns
                    let cols = batch
                        .columns()
                        .iter()
                        .zip(columns)
                        .map(downcast_array)
                        .collect::<Vec<_>>();
                    for row in 0..num_rows {
                        let vals: Vec<PgValue<'_>> = cols
                            .iter()
                            .map(|col| get_value_to_pg_type(col, row))
                            .collect::<Vec<PgValue<'_>>>();

                        batch_buffer.push(vals);
                    }

                    let mut writer_guard = pinned_writer.lock().await;
                    for row in batch_buffer.iter() {
                        let refs: Vec<&(dyn ToSql + Sync)> =
                            row.iter().map(|val| val as &(dyn ToSql + Sync)).collect();
                        let wrote = writer_guard.as_mut().write(&refs).await;
                        if wrote.is_err() {
                            println!("Error writing row: {:?}", wrote);
                        }
                    }

                    Ok(())
                }
            }
        })
        .await?;

    let mut writer_guard = pinned_writer.lock().await;
    writer_guard
        .as_mut()
        .finish()
        .await
        .with_context(|| "Could not finish copy")?;

    sink_tx.batch_execute(&rename_ddl).await?;
    if let Some(sql) = &definition.post_sql {
        sink_tx.batch_execute(sql).await?;
    }

    sink_tx
        .commit()
        .await
        .with_context(|| "Could not commit transaction")?;

    Ok(())
}

pub async fn load_postgres<'a>(
    pq_types: &[ParquetType<'a>],
    table: &str,
    post_sql: Option<String>,
    pg_url: &str,
    role: Option<String>,
    stream: ParquetRecordBatchStream<ParquetObjectReader>,
) -> Result<(), anyhow::Error> {
    let definition = build_definition(table, pq_types, post_sql, role)?;
    info!("PG definition: {:?}", definition);

    let mut client = create_connection(pg_url).await?;
    prepare_to_copy(&mut client, &definition).await?;

    execute_binary_copy(&mut client, &definition, pq_types, stream).await?;

    Ok(())
}

pub async fn load_postgres_cmd(
    args: &LoadArgs,
    pg_url: String,
    handle: tokio::runtime::Handle,
) -> Result<(), anyhow::Error> {
    let builder = builder_from_string(args.file.clone(), handle).await?;
    let file_md = builder.metadata().file_metadata().clone();
    let kv = get_kv_fields(&file_md);
    debug!("kv: {:?}", kv);
    info!("num rows: {:?}", file_md.num_rows());

    let fields = get_fields(&file_md)?;
    let mapped = fields
        .iter()
        .filter_map(|pq| map_parquet_to_abstract(pq, &kv))
        .collect::<Vec<ParquetType>>();

    let stream = builder.with_batch_size(args.batch_size).build()?;
    load_postgres(
        &mapped,
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

#[derive(Debug)]
pub enum PgValue<'a> {
    Bool(Option<bool>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Decimal128(Option<i128>, (), i8),
    Jsonb(Option<&'a str>),
    Text(Option<&'a str>),
    TimestampMicrosecond(Option<i64>, bool),
    TimestampNanosecond(Option<i64>, bool),
    FixedSizeBinaryArray(Option<&'a [u8]>, ()),
}

impl ToSql for PgValue<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            PgValue::Int32(i) => i.to_sql(ty, out),
            PgValue::Int64(i) => i.to_sql(ty, out),
            PgValue::Float32(f) => f.to_sql(ty, out),
            PgValue::Float64(f) => f.to_sql(ty, out),
            PgValue::Decimal128(i, _, scale) => i
                .map(|d| {
                    let big_int = BigInt::from_i128(d);
                    pg_bigdecimal::PgNumeric::new(
                        big_int
                            .map(|bi| BigDecimal::new(bi, scale.to_i64().expect("Scale failed"))),
                    )
                })
                .to_sql(ty, out),
            PgValue::Jsonb(s) => s
                .map(|j| serde_json::from_str::<Value>(j).expect("Failed to parse json"))
                .map(tokio_postgres::types::Json)
                .to_sql(ty, out),
            PgValue::Text(s) => s.to_sql(ty, out),
            PgValue::TimestampMicrosecond(ts, true) => ts
                .map(DateTime::<Utc>::from_timestamp_micros)
                .to_sql(ty, out),
            PgValue::TimestampMicrosecond(ts, false) => {
                ts.map(NaiveDateTime::from_timestamp_micros).to_sql(ty, out)
            }
            PgValue::TimestampNanosecond(ts, true) => ts
                .map(DateTime::<Utc>::from_timestamp_nanos)
                .to_sql(ty, out),
            PgValue::TimestampNanosecond(ts, false) => {
                ts.map(NaiveDateTime::from_timestamp_nanos).to_sql(ty, out)
            }
            PgValue::FixedSizeBinaryArray(b, _) => b
                .map(|x| uuid::Uuid::from_slice(x).unwrap())
                .to_sql(ty, out),
            PgValue::Bool(b) => b.to_sql(ty, out),
        }
    }

    to_sql_checked!();

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

fn get_value_to_pg_type<'a>(arrow_array: &ArrowArrayRef<'a>, index: usize) -> PgValue<'a> {
    match arrow_array {
        ArrowArrayRef::Int32(int_array) => {
            let v = if int_array.is_null(index) {
                None
            } else {
                Some(int_array.value(index))
            };
            PgValue::Int32(v)
        }
        ArrowArrayRef::Int64(int_array) => {
            let v = if int_array.is_null(index) {
                None
            } else {
                Some(int_array.value(index))
            };
            PgValue::Int64(v)
        }
        ArrowArrayRef::Float32(float_array) => {
            let v = if float_array.is_null(index) {
                None
            } else {
                Some(float_array.value(index))
            };
            PgValue::Float32(v)
        }
        ArrowArrayRef::Float64(float_array) => {
            let v = if float_array.is_null(index) {
                None
            } else {
                Some(float_array.value(index))
            };
            PgValue::Float64(v)
        }
        ArrowArrayRef::Decimal128(decimal_array, _precision, scale) => {
            let v = if decimal_array.is_null(index) {
                None
            } else {
                let value = decimal_array.value(index);
                Some(value)
            };
            PgValue::Decimal128(v, (), *scale)
        }
        ArrowArrayRef::Jsonb(string_array) => {
            let v = if string_array.is_null(index) {
                None
            } else {
                Some(string_array.value(index))
            };
            PgValue::Jsonb(v)
        }
        ArrowArrayRef::Utf8(string_array) => {
            let v = if string_array.is_null(index) {
                None
            } else {
                Some(string_array.value(index))
            };
            PgValue::Text(v)
        }
        ArrowArrayRef::TimestampMillisecond(ts_array, is_utc) => {
            let v = if ts_array.is_null(index) {
                None
            } else {
                Some(ts_array.value(index))
            };
            PgValue::TimestampMicrosecond(v, *is_utc)
        }
        ArrowArrayRef::TimestampMicrosecond(ts_array, is_utc) => {
            let v = if ts_array.is_null(index) {
                None
            } else {
                Some(ts_array.value(index))
            };
            PgValue::TimestampMicrosecond(v, *is_utc)
        }
        ArrowArrayRef::TimestampNanosecond(ts_array, is_utc) => {
            let v = if ts_array.is_null(index) {
                None
            } else {
                Some(ts_array.value(index))
            };
            PgValue::TimestampNanosecond(v, *is_utc)
        }
        ArrowArrayRef::FixedSizeBinaryArray(fixed_size_binary_array, _size) => {
            let v = if fixed_size_binary_array.is_null(index) {
                None
            } else {
                Some(fixed_size_binary_array.value(index))
            };
            PgValue::FixedSizeBinaryArray(v, ())
        }
        ArrowArrayRef::Boolean(bool_array) => {
            let v = if bool_array.is_null(index) {
                None
            } else {
                Some(bool_array.value(index))
            };
            PgValue::Bool(v)
        }
    }
}

#[cfg(test)]
mod tests {
    use parquet::{basic::TimeUnit, format::MilliSeconds};

    use crate::parquet_provider::builder_from_string;
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
            ParquetType::BigInt("id"),
            ParquetType::Text("name"),
            ParquetType::Timestamp("created_at", TimeUnit::MILLIS(MilliSeconds {})),
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
            .collect::<Vec<ParquetType>>();

        let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
        let stream = builder.with_batch_size(1024).build().unwrap();

        load_postgres(
            &mapped,
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
