use std::path::Path;

use anyhow::Context;
use bigdecimal::ToPrimitive;
use clap::Args;
use deltalake::{
    arrow::array::RecordBatch,
    kernel::{DataType, StructField, StructType},
    open_table,
    table::DeltaTable,
    DeltaOps,
};
use futures::TryStreamExt;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStream};
use serde::Deserialize;
use tracing::info;

use crate::pq::{
    get_fields, get_file_metadata, get_kv_fields, map_parquet_to_abstract, ParquetType,
};

#[derive(Args, Deserialize, Debug)]
pub struct DeltaLoad {
    #[arg(short, long)]
    pub delta_root: String,
    #[arg(short, long)]
    pub file: String,
    #[arg(short, long)]
    pub table_name: String,
}

pub async fn delta_run(load_args: &DeltaLoad) -> Result<(), anyhow::Error> {
    let builder = get_file_metadata(load_args.file.clone()).await?;
    let file_md = builder.metadata().file_metadata().clone();
    let kv = get_kv_fields(&file_md);
    let root = Path::new(&load_args.delta_root);
    let table_path = Path::new(&load_args.table_name);
    let full_table_path = root.join(table_path);
    let uri = full_table_path.to_string_lossy().to_string();
    let stream = builder.with_batch_size(1024).build()?;

    // Check if table exists first
    let mut table = match open_table(&uri).await {
        Ok(table) => {
            info!("Table exists");
            table
        }
        Err(_) => {
            info!("Creating new table");
            let fields = get_fields(&file_md)?;
            let mapped = fields
                .iter()
                .filter_map(|pq| map_parquet_to_abstract(pq, &kv))
                .collect::<Vec<ParquetType>>();

            let schema = schema_from_parquet(mapped)?;
            info!("Schema: {:?}", schema);
            // Get fresh ops since we need to create
            let ops = DeltaOps::try_from_uri(&uri).await?;
            create_table_from_parquet(ops, table_path.to_string_lossy().to_string(), schema).await?
        }
    };

    info!("num rows: {:?}", file_md.num_rows());
    info!("delta table version: {:?}", table.version());

    overwrite_table_from_parquet(&mut table, stream).await?;

    Ok(())
}

fn map_parquet_to_delta(pq_col: &ParquetType) -> Result<StructField, anyhow::Error> {
    match *pq_col {
        ParquetType::Timestamp(name, _) => {
            Ok(StructField::new(name, DataType::TIMESTAMP_NTZ, true))
        }
        ParquetType::TimestampTz(name, _) => Ok(StructField::new(name, DataType::TIMESTAMP, true)),
        ParquetType::Uuid(name) => Ok(StructField::new(name, DataType::BINARY, true)
            .with_metadata([("paruqet_type", "UUID")])),
        ParquetType::Jsonb(name) => Ok(StructField::new(name, DataType::STRING, true)
            .with_metadata([("parquet_type", "JSONB")])),
        ParquetType::Numeric(name, precision, scale) => {
            let precision = precision
                .to_u8()
                .ok_or_else(|| anyhow::anyhow!("precision too large"))?;
            let scale = scale
                .to_u8()
                .ok_or_else(|| anyhow::anyhow!("scale too large"))?;
            let dec = DataType::decimal(precision, scale)
                .with_context(|| "Could not create decimal column type")?;
            Ok(StructField::new(name, dec, true))
        }
        ParquetType::Varchar(name, length) => {
            let length = length
                .to_u32()
                .ok_or_else(|| anyhow::anyhow!("length too large"))?;
            Ok(StructField::new(name, DataType::STRING, true)
                .with_metadata([("parquet_type", "VARCHAR"), ("length", &length.to_string())]))
        }
        ParquetType::Text(name) => Ok(StructField::new(name, DataType::STRING, true)),
        ParquetType::Integer(name) => Ok(StructField::new(name, DataType::INTEGER, true)),
        ParquetType::BigInt(name) => Ok(StructField::new(name, DataType::LONG, true)),
        ParquetType::Real(name) => Ok(StructField::new(name, DataType::FLOAT, true)),
        ParquetType::Double(name) => Ok(StructField::new(name, DataType::DOUBLE, true)),
        ParquetType::Boolean(name) => Ok(StructField::new(name, DataType::BOOLEAN, true)),
    }
}

pub fn schema_from_parquet(pq: Vec<ParquetType>) -> Result<StructType, anyhow::Error> {
    let fields = pq
        .iter()
        .map(map_parquet_to_delta)
        .collect::<Result<Vec<StructField>, anyhow::Error>>()?;
    Ok(StructType::new(fields))
}

pub async fn overwrite_table_from_parquet(
    table: &mut DeltaTable,
    stream: ParquetRecordBatchStream<ParquetObjectReader>,
) -> Result<(), anyhow::Error> {
    info!("schema: {:?}", stream.schema());
    let writer = DeltaOps(table.clone());
    let batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .with_context(|| "Failed to read parquet stream")?;
    let _table = writer
        .write(batches)
        .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
        .await
        .with_context(|| "Failed to write to table")?;
    Ok(())
}

pub async fn create_table_from_parquet(
    ops: DeltaOps,
    name: String,
    schema: StructType,
) -> Result<DeltaTable, anyhow::Error> {
    ops.create()
        .with_table_name(&name)
        .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
        .with_columns(schema.fields().cloned())
        .await
        .with_context(|| "Failed to create table")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use deltalake::datafusion::prelude::SessionContext;

    use super::*;

    #[tokio::test]
    async fn test_delta_lake_from_parquet() {
        let cd = std::env::current_dir().unwrap();
        let file = format!("{}/test_data/public.users.snappy.parquet", cd.display());
        let delta_root = format!("{}/test_data/delta", cd.display());
        let delta_load = DeltaLoad {
            delta_root: delta_root.clone(),
            file,
            table_name: "users".to_string(),
        };
        delta_run(&delta_load).await.unwrap();

        let table = deltalake::open_table(format!("{}/users", delta_root))
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("users", Arc::new(table.clone()))
            .unwrap();
        let df = ctx
            .sql("SELECT * from users")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let result = results.get(0).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.schema().fields.len(), 6);

        // it is overwrite so run it again
        delta_run(&delta_load).await.unwrap();
        let df = ctx
            .sql("SELECT * from users")
            .await
            .unwrap();
        let results = df.collect().await.unwrap();
        let result = results.get(0).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.schema().fields.len(), 6);
    }
}
