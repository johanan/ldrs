use std::sync::Arc;

use arrow_schema::{Field, Schema};
use deltalake::{
    arrow::array::RecordBatch,
    kernel::{DataType, PrimitiveType, StructField, StructType},
    open_table, DeltaOps,
};

pub async fn test_delta() -> Result<(), anyhow::Error> {
    /*let delta_ops = DeltaOps::try_from_uri("tmp/public.some_table").await?;
    let mut table = delta_ops
        .create()
        .with_table_name("public.some_table")
        .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
        .with_columns(
            StructType::new(vec![
                StructField::new("id", DataType::Primitive(PrimitiveType::Integer), true),
                StructField::new("name", DataType::Primitive(PrimitiveType::String), true),
            ])
            .fields()
            .cloned(),
        )
        .await?;*/

    let table = open_table("tmp/public.some_table").await?;
    println!("Version: {}", table.clone().version());
    println!("Files: {:?}", table.get_schema());
    /*let mut record_batch_writer = deltalake::writer::RecordBatchWriter::for_table(&table)?;
    record_batch_writer
        .write(RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", arrow_schema::DataType::Int32, true),
                Field::new("name", arrow_schema::DataType::Utf8, true),
            ])),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
            ],
        )?)
        .await
        .with_context(|| "Failed to write record batch")?;

    record_batch_writer.flush_and_commit(&mut table).await?;*/
    DeltaOps(table)
        .write(RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", arrow_schema::DataType::Int32, true),
                Field::new("name", arrow_schema::DataType::Utf8, true),
            ])),
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])),
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
            ],
        ))
        .with_save_mode(deltalake::protocol::SaveMode::Append)
        .await?;

    /*   let table = open_table("tmp/public.some_table").await?;
    let ctx = SessionContext::new();
    ctx.register_table("public.some_table", Arc::new(table.clone()))?;
    let df = ctx.read_table( Arc::new(table.clone()))?;
    df.show().await?;
    */

    /*  let table = open_table("tmp/public.some_table").await?;
    let (table, metrics) = DeltaOps(table).optimize().with_type(OptimizeType::Compact).await?;
    println!("{:?}", metrics);*/

    let table = open_table("tmp/public.some_table").await?;
    let (table, metrics) = DeltaOps(table)
        .vacuum()
        .with_retention_period(chrono::Duration::days(0))
        .with_enforce_retention_duration(false)
        .with_dry_run(false)
        .await?;
    println!("{:?}", metrics);

    Ok(())
}
