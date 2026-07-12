use std::fs::File;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Decimal128Array, Decimal64Array, Int16Array, Int32Array, Int8Array, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};
use ldrs_arrow::{build_source_and_target_schema, ColumnSpec};
use ldrs_parquet::{columnspec_from_parquet, get_fields};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

/// Fresh per-test output dir alongside the other generated test outputs under tests/test_data.
fn out_dir(name: &str) -> String {
    let dir = format!(
        "{}/tests/test_data/schema_readback/{}/",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn readback_source_specs(batch: &RecordBatch, label: &str) -> anyhow::Result<Vec<ColumnSpec>> {
    let path = format!("{}data.parquet", out_dir(label));
    {
        let file = File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
    }
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?;
    let arrow_schema = builder.schema().clone();
    let fields = get_fields(builder.metadata().file_metadata())?;
    let source_cols: Vec<ColumnSpec> = fields.iter().filter_map(columnspec_from_parquet).collect();
    let (source, _target) = build_source_and_target_schema(&arrow_schema, source_cols, vec![])?;
    Ok(source)
}

fn one_col(name: &str, dtype: DataType, array: ArrayRef) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(name, dtype, true)]));
    RecordBatch::try_new(schema, vec![array]).unwrap()
}

#[test]
fn readback_tinyint_is_not_stringified() {
    let batch = one_col(
        "n2",
        DataType::Int8,
        Arc::new(Int8Array::from(vec![1i8, 2, 3])),
    );
    let source = readback_source_specs(&batch, "tinyint").unwrap();
    assert!(
        !matches!(source[0], ColumnSpec::Text { .. }),
        "NUMBER(2,0)/tinyint (INT32+INT_8) read back as Text; expected an integer type, got {:?}",
        source[0]
    );
}

#[test]
fn readback_smallint_is_not_stringified() {
    let batch = one_col(
        "n4",
        DataType::Int16,
        Arc::new(Int16Array::from(vec![1i16, 2, 3])),
    );
    let source = readback_source_specs(&batch, "smallint").unwrap();
    assert!(
        !matches!(source[0], ColumnSpec::Text { .. }),
        "NUMBER(4,0)/smallint (INT32+INT_16) read back as Text; expected an integer type, got {:?}",
        source[0]
    );
}

#[test]
fn readback_int32_is_integer() {
    let batch = one_col(
        "n9",
        DataType::Int32,
        Arc::new(Int32Array::from(vec![1i32, 2, 3])),
    );
    let source = readback_source_specs(&batch, "int32").unwrap();
    assert!(
        matches!(source[0], ColumnSpec::Integer { .. }),
        "NUMBER(9,0) (plain INT32) should be Integer, got {:?}",
        source[0]
    );
}

#[test]
fn readback_decimal38_is_numeric() {
    let array = Decimal128Array::from(vec![100i128, 200, 300])
        .with_precision_and_scale(38, 0)
        .unwrap();
    let batch = one_col("n38", DataType::Decimal128(38, 0), Arc::new(array));
    let source = readback_source_specs(&batch, "decimal38").unwrap();
    assert!(
        matches!(
            source[0],
            ColumnSpec::Numeric {
                precision: 38,
                scale: 0,
                ..
            }
        ),
        "NUMBER(38,0) (FLBA+DECIMAL) should be Numeric(38,0), got {:?}",
        source[0]
    );
}

#[test]
fn readback_decimal64_is_numeric() {
    let array = Decimal64Array::from(vec![100i64, 200, 300])
        .with_precision_and_scale(10, 0)
        .unwrap();
    let batch = one_col("n10", DataType::Decimal64(10, 0), Arc::new(array));
    let source = readback_source_specs(&batch, "decimal64")
        .expect("NUMBER(10-18,0) (Decimal64) should resolve, not fail the schema length check");
    assert!(
        matches!(
            source[0],
            ColumnSpec::Numeric {
                precision: 10,
                scale: 0,
                ..
            }
        ),
        "NUMBER(10,0) (Decimal64) should be Numeric(10,0), got {:?}",
        source[0]
    );
}
