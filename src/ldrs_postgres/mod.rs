pub mod client;
pub mod config;
pub mod postgres_destination;
pub mod postgres_execution;
pub mod schema;
pub mod schema_change;

use crate::arrow_access::extracted_values::ColumnConverter;
use crate::arrow_access::extracted_values::ExtractedValue;
use crate::arrow_access::TypedColumnAccessor;
use crate::ldrs_schema::SchemaChange;
use crate::parquet_provider::builder_from_string;
use crate::pq::get_column_schema;
use crate::types::ColumnSchema;
use crate::types::ColumnType;
use anyhow::Context;
use arrow_array::RecordBatch;
use clap::Subcommand;
use client::create_connection;
use config::{LoadArgs, PGFileLoad, PGFileLoadArgs, ProcessedPGFileLoad};
use futures::StreamExt;
use futures::TryStreamExt;
use postgres_types::{to_sql_checked, ToSql, Type};
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

    let stream = stream.into_stream();
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
