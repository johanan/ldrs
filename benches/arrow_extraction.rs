//extern crate ldrs;

use arrow::array::{Decimal128Builder, GenericStringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_array::{Int32Array, TimestampMicrosecondArray, TimestampMillisecondArray};
use arrow_schema::TimeUnit;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use ldrs::arrow_access::TypedColumnAccessor;
use ldrs::ldrs_postgres::PgTypedValue;
use ldrs::types::ColumnSchema;
use parquet::format::{MicroSeconds, MilliSeconds};
use postgres_types::ToSql;
use postgres_types::Type;
use std::sync::Arc;
use tokio_util::bytes::BytesMut;

fn create_test_batch(size: usize) -> RecordBatch {
    // Create schema with additional columns
    let schema = Schema::new(vec![
        Field::new("int32_col", DataType::Int32, true),
        Field::new("string_col", DataType::Utf8, true),
        Field::new(
            "timestamp_ms_col",
            DataType::Timestamp(TimeUnit::Millisecond, Some(("UTC".into()))),
            true,
        ),
        Field::new(
            "timestamp_no_tz_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("decimal_col", DataType::Decimal128(10, 2), true),
    ]);

    // Create arrays
    let mut int_builder = Int32Array::builder(size);
    let mut str_builder = GenericStringBuilder::<i32>::new();
    let mut ts_ms_builder = TimestampMillisecondArray::builder(size).with_timezone("UTC");
    let mut ts_micro_builder = TimestampMicrosecondArray::builder(size);
    let mut decimal_builder =
        Decimal128Builder::with_capacity(size).with_data_type(DataType::Decimal128(10, 2));

    let now = chrono::Utc::now();
    let now_millis = now.timestamp_millis();
    let now_micros = now.timestamp_micros();

    for i in 0..size {
        if i % 10 == 0 {
            int_builder.append_null();
            str_builder.append_null();
            ts_ms_builder.append_null();
            ts_micro_builder.append_null();
            decimal_builder.append_null();
        } else {
            int_builder.append_value(i as i32);
            str_builder.append_value(format!("value_{}", i));

            // Timestamp values (current time + offset)
            ts_ms_builder.append_value(now_millis + (i as i64 * 1000));
            ts_micro_builder.append_value(now_micros + (i as i64 * 1000));

            // Decimal values (price-like data with 2 decimal places)
            let decimal_val = (i as i128) * 100 + 99; // e.g., 1.99, 2.99, etc.
            decimal_builder.append_value(decimal_val);
        }
    }

    let int_array = int_builder.finish();
    let str_array = str_builder.finish();
    let ts_ms_array = ts_ms_builder.finish();
    let ts_micro_array = ts_micro_builder.finish();
    let decimal_array = decimal_builder.finish();

    // Create record batch
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(int_array),
            Arc::new(str_array),
            Arc::new(ts_ms_array),
            Arc::new(ts_micro_array),
            Arc::new(decimal_array),
        ],
    )
    .unwrap()
}

enum UnionType<'a> {
    Int32(Option<i32>),
    Utf8(Option<&'a str>),
    Int64(Option<i64>),
    Decimal128(Option<i128>),
}

fn typed_accessor_approach(batch: &RecordBatch) -> Vec<Vec<UnionType<'_>>> {
    let columns = batch.columns();
    let num_rows = batch.num_rows();

    // Create TypedColumnAccessor for all columns
    let accessors: Vec<TypedColumnAccessor> =
        columns.iter().map(TypedColumnAccessor::new).collect();

    let mut results = Vec::with_capacity(num_rows);

    // Extract values from all columns using typed accessors
    for row in 0..num_rows {
        let mut row_values = Vec::with_capacity(columns.len());
        for accessor in &accessors {
            let value = match accessor {
                TypedColumnAccessor::Int32(arr) => UnionType::Int32(accessor.Int32(row)),
                TypedColumnAccessor::Utf8(arr) => UnionType::Utf8(accessor.Utf8(row)),
                TypedColumnAccessor::TimestampMillisecond(arr, _) => {
                    UnionType::Int64(accessor.TimestampMillisecond(row))
                }
                TypedColumnAccessor::TimestampMicrosecond(arr, _) => {
                    UnionType::Int64(accessor.TimestampMicrosecond(row))
                }
                TypedColumnAccessor::Decimal128(arr, _, _) => {
                    UnionType::Decimal128(accessor.Decimal128(row))
                }
                _ => continue,
            };
            row_values.push(value);
        }
        results.push(row_values);
    }

    results
}

fn new_pg_types(batch: &RecordBatch) -> BytesMut {
    let columns = batch.columns();
    let num_rows = batch.num_rows();
    let mut buffer = BytesMut::new();

    let accessors: Vec<TypedColumnAccessor> =
        columns.iter().map(TypedColumnAccessor::new).collect();

    let parquet_types = vec![
        ColumnSchema::Integer("int32_col"),
        ColumnSchema::Text("string_col"),
        ColumnSchema::TimestampTz(
            "timestamp_ms_col",
            parquet::basic::TimeUnit::MILLIS(MilliSeconds {}),
        ),
        ColumnSchema::Timestamp(
            "timestamp_no_tz_col",
            parquet::basic::TimeUnit::MICROS(MicroSeconds {}),
        ),
        ColumnSchema::Numeric("decimal_col", 10, 2),
    ];

    // Create PostgreSQL types
    let pg_types = [
        Type::INT4,
        Type::TEXT,
        Type::TIMESTAMPTZ,
        Type::TIMESTAMP,
        Type::NUMERIC,
    ];

    for row in 0..num_rows {
        for ((accessor, pg_type), parquet_type) in
            accessors.iter().zip(&pg_types).zip(&parquet_types)
        {
            let value = PgTypedValue::new(accessor, row, parquet_type);
            black_box(value.to_sql(pg_type, &mut buffer));
            buffer.clear();
        }
    }

    buffer
}

fn bench_arrow_extraction(c: &mut Criterion) {
    let mut group = c.benchmark_group("Arrow Value Extraction");

    for size in [100, 1024, 10_000].iter() {
        let batch = create_test_batch(*size);

        group.bench_with_input(
            BenchmarkId::new("TypedAccessor", size),
            &batch,
            |b, batch| {
                b.iter(|| typed_accessor_approach(black_box(batch)));
            },
        );

        group.bench_with_input(BenchmarkId::new("NewPgTypes", size), &batch, |b, batch| {
            b.iter(|| new_pg_types(black_box(batch)));
        });
    }

    group.finish();
}

criterion_group!(benches, bench_arrow_extraction);
criterion_main!(benches);
