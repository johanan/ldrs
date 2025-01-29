use arrow::datatypes::ArrowNativeType;
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayAccessor, ArrayRef, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow_schema::{DataType, TimeUnit};
use bigdecimal::FromPrimitive;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::TryStreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::basic::LogicalType;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::schema::types::Type::{GroupType, PrimitiveType};
use pg_bigdecimal::{BigDecimal, BigInt};
use postgres_types::to_sql_checked;
use postgres_types::{ToSql, Type};
use std::env;
use std::fs::File;
use std::pin::pin;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <file_path>", args[0]);
        std::process::exit(1);
    }

    let path = &args[1];
    let file = File::open(path).unwrap();

    let metadata = ParquetMetaDataReader::new()
        .with_page_indexes(false)
        .parse_and_finish(&file)
        .unwrap();

    let file_md = metadata.file_metadata();
    println!("num rows: {:?}", file_md.num_rows());
    println!("{:?}", file_md.version());
    let schema_name = file_md.schema().name();
    println!("schema_name: {:?}", schema_name);

    let root = file_md.schema();
    print!("{:?}", root);

    let f = match root {
        GroupType {
            basic_info: _,
            fields,
        } => Ok(fields),
        _ => Err("Root is not a group type"),
    };
    println!("{:?}", f);

    let fields = f.unwrap();
    let mapped = fields
        .iter()
        .filter_map(map_parquet_to_ddl)
        .collect::<Vec<(String, Type)>>();
    let mut ddl = String::from("CREATE TABLE if not exists public.test_load (");
    let fields_ddl = mapped
        .iter()
        .map(|t| t.0.clone())
        .collect::<Vec<String>>()
        .join(", ");
    ddl.push_str(&fields_ddl);
    ddl.push_str(");");
    println!("PG ddl: {:?}", ddl);

    let pg_url = "postgres://postgres:postgres@localhost:5432/postgres";

    let (mut client, connection) = tokio_postgres::connect(pg_url, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let stmt = client.prepare("SELECT $1::TEXT").await?;
    let rows = client.query(&stmt, &[&"hello world"]).await?;
    let value: &str = rows[0].get(0);
    println!("value: {}", value);

    let tx = client.transaction().await?;

    let _drop = tx
        .execute("drop table if exists public.test_load;", &[])
        .await?;

    let create = tx.execute(&ddl, &[]).await?;
    tx.commit().await?;

    println!("create: {:?}", create);

    let pg_types = mapped.into_iter().map(|t| t.1).collect::<Vec<Type>>();

    let sink_tx = client.transaction().await?;

    let sink = sink_tx
        .copy_in("COPY public.test_load FROM STDIN BINARY")
        .await
        .unwrap();
    let writer = BinaryCopyInWriter::new(sink, &pg_types);
    let pinned_writer = pin!(writer);
    let pinned_writer = Arc::new(Mutex::new(pinned_writer));

    let a_file = tokio::fs::File::from_std(file);
    let builder = ParquetRecordBatchStreamBuilder::new(a_file).await?;
    let stream = builder.with_batch_size(1024).build()?;
    stream
        .try_for_each_concurrent(4, {
            |batch: RecordBatch| {
                let pinned_writer = Arc::clone(&pinned_writer);
                let pg_types_clone = pg_types.clone();
                println!("pg_types_clone: {:?}", pg_types_clone);
                async move {
                    let num_rows = batch.num_rows();
                    let num_cols = batch.num_columns();
                    println!("num_rows: {:?}, num_cols: {:?}", num_rows, num_cols);
                    let mut batch_buffer = Vec::<Vec<PgValue<'_>>>::with_capacity(num_rows);
                    // iterate over the rows and then the columns
                    let cols = batch
                        .columns()
                        .iter()
                        .zip(pg_types_clone.iter())
                        .map(downcast_array)
                        .collect::<Vec<_>>();
                    for row in 0..num_rows {
                        let vals: Vec<PgValue<'_>> = cols
                            .iter()
                            .map(|col| get_value_to_pg_type(col, row))
                            .collect::<Vec<PgValue<'_>>>();

                        println!("vals: {:?}", vals);

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
    writer_guard.as_mut().finish().await.unwrap();
    sink_tx.commit().await.unwrap();

    Ok(())
}

#[derive(Debug)]
enum PgValue<'a> {
    Bool(Option<bool>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Decimal128(Option<i128>, u8, i8),
    Jsonb(Option<&'a str>),
    Text(Option<&'a str>),
    TimestampNanosecond(Option<i64>, bool),
    FixedSizeBinaryArray(Option<&'a [u8]>, i32),
}

impl<'a> ToSql for PgValue<'a> {
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
                    pg_bigdecimal::PgNumeric::new(big_int.map(|bi| {
                        BigDecimal::new(bi, scale.to_i64().expect("Should always be ok"))
                    }))
                })
                .to_sql(ty, out),
            PgValue::Jsonb(s) => s
                .map(|j| serde_json::Value::String(j.to_string()))
                .to_sql(ty, out),
            PgValue::Text(s) => s.to_sql(ty, out),
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

    fn accepts(ty: &Type) -> bool {
        true
    }
}

#[derive(Debug)]
enum ArrowArrayRef<'a> {
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    Decimal128(&'a Decimal128Array, u8, i8),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Jsonb(&'a StringArray),
    Utf8(&'a StringArray),
    Boolean(&'a BooleanArray),
    TimestampNanosecond(&'a TimestampNanosecondArray, bool),
    FixedSizeBinaryArray(&'a FixedSizeBinaryArray, i32),
    //TimestampMillisecond(&'a TimestampMillisecondArray),
    //TimestampSecond(&'a TimestampSecondArray),
}

fn downcast_array<'a>((array, pg_type): (&'a ArrayRef, &Type)) -> ArrowArrayRef<'a> {
    println!("array: {:?}", array);
    println!("pg_type: {:?}", pg_type);
    match array.data_type() {
        DataType::Int32 => ArrowArrayRef::Int32(array.as_primitive()),
        DataType::Int64 => ArrowArrayRef::Int64(array.as_primitive()),
        DataType::Float32 => ArrowArrayRef::Float32(array.as_primitive()),
        DataType::Float64 => ArrowArrayRef::Float64(array.as_primitive()),
        DataType::Timestamp(TimeUnit::Nanosecond, is_utc) => {
            ArrowArrayRef::TimestampNanosecond(array.as_primitive(), is_utc.is_some())
        }
        DataType::FixedSizeBinary(size) => {
            ArrowArrayRef::FixedSizeBinaryArray(array.as_fixed_size_binary(), *size)
        }
        DataType::Boolean => ArrowArrayRef::Boolean(array.as_boolean()),
        DataType::Decimal128(precision, scale) => {
            ArrowArrayRef::Decimal128(array.as_primitive(), *precision, *scale)
        }
        DataType::Utf8 => match pg_type {
            &postgres_types::Type::JSONB => ArrowArrayRef::Jsonb(array.as_string()),
            _ => ArrowArrayRef::Utf8(array.as_string()),
        },
        _ => unimplemented!(),
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
        ArrowArrayRef::Decimal128(decimal_array, precision, scale) => {
            let v = if decimal_array.is_null(index) {
                None
            } else {
                let value = decimal_array.value(index);
                Some(value)
            };
            PgValue::Decimal128(v, *precision, *scale)
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
        ArrowArrayRef::TimestampNanosecond(ts_array, is_utc) => {
            let v = if ts_array.is_null(index) {
                None
            } else {
                Some(ts_array.value(index))
            };
            PgValue::TimestampNanosecond(v, *is_utc)
        }
        ArrowArrayRef::FixedSizeBinaryArray(fixed_size_binary_array, size) => {
            let v = if fixed_size_binary_array.is_null(index) {
                None
            } else {
                Some(fixed_size_binary_array.value(index))
            };
            PgValue::FixedSizeBinaryArray(v, *size)
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

fn map_parquet_to_ddl(
    pq: &Arc<parquet::schema::types::Type>,
) -> Option<(String, postgres_types::Type)> {
    match pq.as_ref() {
        GroupType { .. } => None,
        PrimitiveType { basic_info, .. } => {
            let name = pq.name();
            let pg_type = match basic_info {
                bi if bi.logical_type().is_some() => {
                    // we have a logical type, use that
                    match bi.logical_type().unwrap() {
                        LogicalType::String => (String::from("TEXT"), postgres_types::Type::TEXT),
                        LogicalType::Timestamp {
                            is_adjusted_to_u_t_c,
                            ..
                        } => {
                            if is_adjusted_to_u_t_c {
                                (
                                    String::from("TIMESTAMPTZ"),
                                    postgres_types::Type::TIMESTAMPTZ,
                                )
                            } else {
                                (String::from("TIMESTAMP"), postgres_types::Type::TIMESTAMP)
                            }
                        }
                        LogicalType::Uuid => (String::from("UUID"), postgres_types::Type::UUID),
                        LogicalType::Json => (String::from("JSONB"), postgres_types::Type::JSONB),
                        LogicalType::Decimal { scale, precision } => (
                            format!("NUMERIC({},{})", precision, scale),
                            postgres_types::Type::NUMERIC,
                        ),
                        _ => (String::from("TEXT"), postgres_types::Type::TEXT),
                    }
                }
                _ => match pq.get_physical_type() {
                    parquet::basic::Type::FLOAT => {
                        (String::from("FLOAT4"), postgres_types::Type::FLOAT4)
                    }
                    parquet::basic::Type::DOUBLE => {
                        (String::from("FLOAT8"), postgres_types::Type::FLOAT8)
                    }
                    parquet::basic::Type::INT32 => {
                        (String::from("INT4"), postgres_types::Type::INT4)
                    }
                    parquet::basic::Type::INT64 => {
                        (String::from("INT8"), postgres_types::Type::INT8)
                    }
                    parquet::basic::Type::BOOLEAN => {
                        (String::from("BOOL"), postgres_types::Type::BOOL)
                    }
                    _ => (String::from("TEXT"), postgres_types::Type::TEXT),
                },
            };
            Some((format!("{} {}", name, pg_type.0), pg_type.1))
        }
    }
}
