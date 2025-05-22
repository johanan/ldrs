pub mod config;
pub mod mvr_config;

use crate::arrow_access::TypedColumnAccessor;
use crate::parquet_provider::builder_from_string;
use crate::pq::get_column_schema;
use crate::types::ColumnSchema;
use anyhow::Context;
use arrow::datatypes::ArrowNativeType;
use arrow_array::RecordBatch;
use bigdecimal::FromPrimitive;
use chrono::{DateTime, NaiveDateTime, Utc};
use config::{LoadArgs, PGFileLoad, PGFileLoadArgs, ProcessedPGFileLoad};
use futures::StreamExt;
use futures::TryStreamExt;
use native_tls::TlsConnector;
use pg_bigdecimal::{BigDecimal, BigInt};
use postgres_native_tls::MakeTlsConnector;
use postgres_types::{to_sql_checked, ToSql, Type};
use serde_json::Value;
use std::pin::pin;
use std::pin::Pin;
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

pub fn map_parquet_to_ddl(pq: &ColumnSchema) -> String {
    match pq {
        ColumnSchema::SmallInt(name) => format!("{} smallint", name),
        ColumnSchema::BigInt(name) => format!("{} bigint", name),
        ColumnSchema::Boolean(name) => format!("{} boolean", name),
        ColumnSchema::Double(name) => format!("{} double precision", name),
        ColumnSchema::Integer(name) => format!("{} integer", name),
        ColumnSchema::Jsonb(name) => format!("{} jsonb", name),
        ColumnSchema::Numeric(name, precision, scale) => {
            format!("{} numeric({}, {})", name, precision, scale)
        }
        ColumnSchema::Timestamp(name, ..) => format!("{} timestamp", name),
        ColumnSchema::TimestampTz(name, ..) => format!("{} timestamptz", name),
        ColumnSchema::Date(name) => format!("{} date", name),
        ColumnSchema::Real(name) => format!("{} real", name),
        ColumnSchema::Text(name) => format!("{} text", name),
        ColumnSchema::Uuid(name) => format!("{} uuid", name),
        ColumnSchema::Varchar(name, size) => format!("{} varchar({})", name, size),
    }
}

pub fn map_parquet_to_pg_type(pq: &ColumnSchema) -> postgres_types::Type {
    match pq {
        ColumnSchema::SmallInt(_) => postgres_types::Type::INT2,
        ColumnSchema::BigInt(_) => postgres_types::Type::INT8,
        ColumnSchema::Boolean(_) => postgres_types::Type::BOOL,
        ColumnSchema::Double(_) => postgres_types::Type::FLOAT8,
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

pub async fn process_record_batch<'a>(
    writer: &mut Pin<&mut BinaryCopyInWriter>,
    batch: &RecordBatch,
    parquet_types: &[ColumnSchema<'a>],
) -> Result<(), anyhow::Error> {
    let num_rows = batch.num_rows();
    let columns = batch.columns();

    // Create TypedColumnAccessor for each column
    let accessors: Vec<TypedColumnAccessor> =
        columns.iter().map(TypedColumnAccessor::new).collect();

    let mut batch_buffer = Vec::<Vec<PgTypedValue>>::with_capacity(num_rows);

    // Process each row
    for row_idx in 0..num_rows {
        // Create PgTypedValue for each column in this row
        let row_values: Vec<PgTypedValue> = accessors
            .iter()
            .zip(parquet_types)
            .map(|(accessor, parquet_type)| PgTypedValue::new(accessor, row_idx, parquet_type))
            .collect();

        batch_buffer.push(row_values);
    }

    for row in batch_buffer.iter() {
        let refs: Vec<&(dyn ToSql + Sync)> =
            row.iter().map(|val| val as &(dyn ToSql + Sync)).collect();
        writer
            .as_mut()
            .write(&refs)
            .await
            .with_context(|| "Failed to write row to PostgreSQL")?;
    }

    Ok(())
}

pub async fn execute_binary_copy<'a, S, E>(
    client: &mut tokio_postgres::Client,
    definition: &LoadDefintion,
    columns: &[ColumnSchema<'a>],
    stream: S,
) -> Result<(), anyhow::Error>
where
    S: futures::TryStream<Ok = RecordBatch, Error = E> + Send,
    E: std::error::Error + Send + Sync + 'static,
{
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
    let mut pinned_writer = pin!(writer);

    let mut stream = stream.into_stream();
    let mut stream = pin!(stream);

    while let Some(batch_result) = stream.next().await {
        let batch = match batch_result {
            Ok(b) => b,
            Err(e) => return Err(anyhow::anyhow!("Stream error: {}", e)),
        };

        process_record_batch(&mut pinned_writer, &batch, columns).await?;
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
    info!("num rows: {:?}", file_md.num_rows());

    let mapped = get_column_schema(&file_md)?;

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

pub async fn load_postgres_from_arrow<'a, S, E>(
    schema: Vec<ColumnSchema<'a>>,
    stream: S,
) -> Result<(), anyhow::Error>
where
    S: futures::TryStream<Ok = RecordBatch, Error = E> + Send,
    E: std::error::Error + Send + Sync + 'static,
{
    let pg_url = std::env::var("LDRS_PG_URL").with_context(|| "LDRS_PG_URL not set")?;
    let table = "public.users_arrow";
    let post_sql = None;
    let role = None;

    load_postgres(&schema, table, post_sql, &pg_url, role, stream).await?;
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
pub struct PgTypedValue<'a> {
    accessor: &'a TypedColumnAccessor<'a>,
    row_idx: usize,
    col_schema: &'a ColumnSchema<'a>,
}

impl<'a> PgTypedValue<'a> {
    pub fn new(
        accessor: &'a TypedColumnAccessor<'a>,
        row_idx: usize,
        col_schema: &'a ColumnSchema<'a>,
    ) -> Self {
        Self {
            accessor,
            row_idx,
            col_schema,
        }
    }
}

impl<'a> ToSql for PgTypedValue<'a> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // Use the ParquetType to determine the semantic meaning
        match self.accessor {
            TypedColumnAccessor::Int16(_) => self.accessor.Int16(self.row_idx).to_sql(ty, out),
            TypedColumnAccessor::Int32(_) => self.accessor.Int32(self.row_idx).to_sql(ty, out),
            TypedColumnAccessor::Int64(_) => self.accessor.Int64(self.row_idx).to_sql(ty, out),
            TypedColumnAccessor::Float32(_) => self.accessor.Float32(self.row_idx).to_sql(ty, out),
            TypedColumnAccessor::Float64(_) => self.accessor.Float64(self.row_idx).to_sql(ty, out),
            TypedColumnAccessor::Boolean(_) => self.accessor.Boolean(self.row_idx).to_sql(ty, out),
            TypedColumnAccessor::Decimal128(_, _, scale) => {
                self.accessor
                    .Decimal128(self.row_idx)
                    .map(|d| {
                        let big_int = BigInt::from_i128(d);
                        pg_bigdecimal::PgNumeric::new(big_int.map(|bi| {
                            BigDecimal::new(bi, (*scale).to_i64().expect("Scale failed"))
                        }))
                    })
                    .to_sql(ty, out)
            }
            TypedColumnAccessor::FixedSizeBinary(_, size) => match self.col_schema {
                ColumnSchema::Uuid(_) => self
                    .accessor
                    .FixedSizeBinary(self.row_idx)
                    .map(|bytes| uuid::Uuid::from_slice(bytes).expect("Failed to parse UUID"))
                    .to_sql(ty, out),
                _ => self.accessor.FixedSizeBinary(self.row_idx).to_sql(ty, out),
            },
            // timestamp types
            // Datetime and NaiveDateTime are different types so they have to be targeted separately
            TypedColumnAccessor::TimestampMillisecond(_, true) => self
                .accessor
                .TimestampMillisecond(self.row_idx)
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .to_sql(ty, out),
            TypedColumnAccessor::TimestampMillisecond(_, false) => self
                .accessor
                .TimestampMillisecond(self.row_idx)
                .and_then(NaiveDateTime::from_timestamp_millis)
                .to_sql(ty, out),
            TypedColumnAccessor::TimestampMicrosecond(_, true) => self
                .accessor
                .TimestampMicrosecond(self.row_idx)
                .and_then(DateTime::<Utc>::from_timestamp_micros)
                .to_sql(ty, out),
            TypedColumnAccessor::TimestampMicrosecond(_, false) => self
                .accessor
                .TimestampMicrosecond(self.row_idx)
                .and_then(NaiveDateTime::from_timestamp_micros)
                .to_sql(ty, out),
            TypedColumnAccessor::TimestampNanosecond(_, true) => self
                .accessor
                .TimestampNanosecond(self.row_idx)
                .map(DateTime::<Utc>::from_timestamp_nanos)
                .to_sql(ty, out),
            TypedColumnAccessor::TimestampNanosecond(_, false) => self
                .accessor
                .TimestampNanosecond(self.row_idx)
                .and_then(NaiveDateTime::from_timestamp_nanos)
                .to_sql(ty, out),
            TypedColumnAccessor::Date32(_) => self
                .accessor
                .Date32(self.row_idx)
                .map(|d| {
                    //chrono::NaiveDate::from_ymd(1970, 1, 1) + chrono::Duration::days(d.into())
                    chrono::NaiveDate::from_num_days_from_ce_opt(719163 + d).expect("Failed to parse date")
                })
                .to_sql(ty, out),
            
            // string types
            TypedColumnAccessor::Utf8(_) => match self.col_schema {
                ColumnSchema::Jsonb(_) => self
                    .accessor
                    .Utf8(self.row_idx)
                    .map(|j| serde_json::from_str::<Value>(j).expect("Failed to parse json"))
                    .map(tokio_postgres::types::Json)
                    .to_sql(ty, out),
                _ => self.accessor.Utf8(self.row_idx).to_sql(ty, out),
            },
        }
    }

    to_sql_checked!();

    fn accepts(_ty: &Type) -> bool {
        true
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
            ColumnSchema::BigInt("id"),
            ColumnSchema::Text("name"),
            ColumnSchema::Timestamp("created_at", TimeUnit::MILLIS(MilliSeconds {})),
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
