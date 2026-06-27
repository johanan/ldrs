use std::fs::File;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use ldrs_parquet::{FileNamer, ParquetSink};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn test_batch(schema: &SchemaRef, start: i64, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (start..start + rows as i64).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("name_{}", i)).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

fn indexed_namer() -> FileNamer {
    Box::new(|i| Ok(format!("part_{:05}.parquet", i)))
}

fn constant_namer() -> FileNamer {
    Box::new(|_| Ok("constant.parquet".to_string()))
}

/// Fresh output directory under tests/test_data, cleaned per test.
fn test_dir(name: &str) -> String {
    let dir = format!(
        "{}/tests/test_data/sink_writes/{}/",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn read_rows(dir: &str, filename: &str) -> (usize, SchemaRef) {
    let file = File::open(format!("{}{}", dir, filename)).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();
    let mut rows = 0;
    let mut schema = None;
    for batch in reader {
        let batch = batch.unwrap();
        schema = Some(batch.schema());
        rows += batch.num_rows();
    }
    let schema = schema.unwrap_or_else(|| {
        let file = File::open(format!("{}{}", dir, filename)).unwrap();
        ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .schema()
            .clone()
    });
    (rows, schema)
}

#[tokio::test]
async fn sink_write_finish_single_file() {
    let schema = test_schema();
    let dir = test_dir("single");

    // The namer emits a stray leading slash; `join_relative` drops the empty leading
    // segment, so the file lands at the normalized path (not "%2F…" or "//…").
    let namer: FileNamer = Box::new(|i| Ok(format!("/part_{:05}.parquet", i)));
    let mut sink = ParquetSink::new(&dir, schema.clone(), None, None, namer, None).unwrap();
    sink.write_batch(&test_batch(&schema, 0, 3)).await.unwrap();
    sink.write_batch(&test_batch(&schema, 3, 2)).await.unwrap();
    let results = sink.finish().await.unwrap();

    assert_eq!(results.len(), 1, "no limits set: expected one file");
    // reading via the normalized, slash-free path proves the leading slash was dropped
    let (rows, read_schema) = read_rows(&dir, "part_00000.parquet");
    assert_eq!(rows, 5);
    assert_eq!(read_schema, schema);
}

#[tokio::test]
async fn sink_rotates_by_rows() {
    let schema = test_schema();
    let dir = test_dir("rotate_rows");

    let mut sink =
        ParquetSink::new(&dir, schema.clone(), Some(2), None, indexed_namer(), None).unwrap();
    for i in 0..3 {
        sink.write_batch(&test_batch(&schema, i * 2, 2))
            .await
            .unwrap();
    }
    let results = sink.finish().await.unwrap();

    assert_eq!(results.len(), 3, "each 2-row batch hits the limit");
    assert_eq!(results[0].0, "part_00000.parquet");
    assert_eq!(results[1].0, "part_00001.parquet");
    assert_eq!(results[2].0, "part_00002.parquet");
    for (filename, _) in &results {
        let (rows, _) = read_rows(&dir, filename);
        assert_eq!(rows, 2);
    }
}

#[tokio::test]
async fn sink_collision_check_rejects_constant_name_with_rotation() {
    let schema = test_schema();
    let dir = test_dir("collision");

    let err = ParquetSink::new(&dir, schema, Some(2), None, constant_namer(), None)
        .err()
        .expect("constant namer + rotation limit must fail at construct");
    assert!(
        err.to_string().contains("{{ index }}"),
        "error should tell the user the fix: {}",
        err
    );
}

#[tokio::test]
async fn sink_constant_name_without_rotation_is_fine() {
    let schema = test_schema();
    let dir = test_dir("constant_ok");

    let mut sink =
        ParquetSink::new(&dir, schema.clone(), None, None, constant_namer(), None).unwrap();
    sink.write_batch(&test_batch(&schema, 0, 4)).await.unwrap();
    let results = sink.finish().await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "constant.parquet");
}

#[tokio::test]
async fn sink_namer_error_fails_at_construct() {
    let schema = test_schema();
    let dir = test_dir("namer_err");

    let broken: FileNamer = Box::new(|_| Err(anyhow::anyhow!("render failed")));
    // even with no rotation limits, namer(0) runs at construct
    let result = ParquetSink::new(&dir, schema, None, None, broken, None);
    assert!(
        result.is_err(),
        "broken template must fail before any data moves"
    );
}

#[tokio::test]
async fn sink_skips_zero_row_batches() {
    let schema = test_schema();
    let dir = test_dir("zero_rows");

    let mut sink =
        ParquetSink::new(&dir, schema.clone(), None, None, indexed_namer(), None).unwrap();
    let empty = RecordBatch::new_empty(schema.clone());
    sink.write_batch(&empty).await.unwrap();
    sink.write_batch(&test_batch(&schema, 0, 2)).await.unwrap();
    sink.write_batch(&empty).await.unwrap();
    let results = sink.finish().await.unwrap();

    assert_eq!(results.len(), 1);
    let (rows, _) = read_rows(&dir, &results[0].0);
    assert_eq!(rows, 2, "empty batches contribute nothing");
}

#[tokio::test]
async fn sink_empty_stream_writes_no_files() {
    let schema = test_schema();
    let dir = test_dir("empty_stream");

    let mut sink =
        ParquetSink::new(&dir, schema.clone(), None, None, indexed_namer(), None).unwrap();
    sink.write_batch(&RecordBatch::new_empty(schema))
        .await
        .unwrap();
    let results = sink.finish().await.unwrap();

    assert!(results.is_empty());
    let entries: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
    assert!(entries.is_empty(), "no rows must mean no files");
}

#[tokio::test]
async fn sink_finish_empty_writes_schema_only_marker() {
    let schema = test_schema();
    let dir = test_dir("marker");

    let sink = ParquetSink::new(&dir, schema.clone(), None, None, indexed_namer(), None).unwrap();
    let results = sink.finish_empty().await.unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "part_00000.parquet");
    let (rows, read_schema) = read_rows(&dir, &results[0].0);
    assert_eq!(rows, 0);
    assert_eq!(read_schema, schema, "marker file carries the full schema");
}

#[tokio::test]
async fn sink_finish_empty_rejects_written_rows() {
    let schema = test_schema();
    let dir = test_dir("marker_misuse");

    let mut sink =
        ParquetSink::new(&dir, schema.clone(), None, None, indexed_namer(), None).unwrap();
    sink.write_batch(&test_batch(&schema, 0, 1)).await.unwrap();
    assert!(
        sink.finish_empty().await.is_err(),
        "finish_empty after rows is an orchestrator contract violation"
    );
}

#[tokio::test]
async fn sink_abort_deletes_completed_files() {
    let schema = test_schema();
    let dir = test_dir("abort");

    let mut sink =
        ParquetSink::new(&dir, schema.clone(), Some(2), None, indexed_namer(), None).unwrap();
    // first batch completes a file via rotation, second leaves a writer in flight
    sink.write_batch(&test_batch(&schema, 0, 2)).await.unwrap();
    sink.write_batch(&test_batch(&schema, 2, 1)).await.unwrap();
    assert!(
        std::path::Path::new(&format!("{}part_00000.parquet", dir)).exists(),
        "rotated file should exist before abort"
    );

    sink.abort().await;
    assert!(
        !std::path::Path::new(&format!("{}part_00000.parquet", dir)).exists(),
        "abort must delete completed files"
    );
}
