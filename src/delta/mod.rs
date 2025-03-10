mod schema;

use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::Context;
use arrow_schema::{Schema, SchemaRef};
use bigdecimal::ToPrimitive;
use clap::Args;
use deltalake::{
    arrow::array::RecordBatch,
    datafusion::{
        catalog::TableProvider, datasource::provider_as_source, logical_expr::LogicalPlanBuilder,
        physical_plan::memory::LazyBatchGenerator,
    },
    kernel::{DataType, StructField, StructType, Transaction},
    operations::{transaction::CommitProperties, write::WriteBuilder},
    table::DeltaTable,
    DeltaOps, DeltaResult,
};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStream};
use schema::{analyze_schema_conversions, convert_batch, map_convert_schema, ColumnConversion};
use serde::Deserialize;
use tracing::info;

use crate::pq::{
    get_fields, get_file_metadata, get_kv_fields, map_parquet_to_abstract, ParquetType,
};
use deltalake::azure::register_handlers;

#[derive(Args, Deserialize, Debug)]
pub struct DeltaLoad {
    #[arg(short, long)]
    pub delta_root: String,
    #[arg(short, long)]
    pub file: String,
    #[arg(short, long)]
    pub table_name: String,
    #[arg(short, long)]
    pub load_mode: deltalake::protocol::SaveMode,
}

pub async fn delta_run(load_args: &DeltaLoad) -> Result<(), anyhow::Error> {
    register_handlers(None);
    let builder = get_file_metadata(load_args.file.clone()).await?;
    let file_md = builder.metadata().file_metadata().clone();
    let kv = get_kv_fields(&file_md);
    let root = Path::new(&load_args.delta_root);
    let table_path = Path::new(&load_args.table_name);
    let full_table_path = root.join(table_path);
    let uri = full_table_path.to_string_lossy().to_string();
    let stream = builder.with_batch_size(1024).build()?;

    let options = HashMap::from([(String::from("timeout"), String::from("60s"))]);
    let ops = DeltaOps::try_from_uri_with_storage_options(&uri, options)
        .await
        .with_context(|| format!("Failed to create DeltaOps for {}", uri))?;

    // Check if table exists first
    let mut table = match ops.0.state {
        Some(_) => {
            info!("Table exists");
            ops.0
        }
        None => {
            info!("Creating new table");
            let fields = get_fields(&file_md)?;
            let mapped = fields
                .iter()
                .filter_map(|pq| map_parquet_to_abstract(pq, &kv))
                .collect::<Vec<ParquetType>>();

            let schema = schema_from_parquet(mapped)?;
            info!("Schema: {:?}", schema);
            create_table_from_parquet(ops, table_path.to_string_lossy().to_string(), schema).await?
        }
    };

    info!("num rows: {:?}", file_md.num_rows());
    info!("delta table version: {:?}", table.version());

    write(&mut table, stream, load_args.load_mode).await?;

    Ok(())
}

fn map_parquet_to_delta(pq_col: &ParquetType) -> Result<StructField, anyhow::Error> {
    match *pq_col {
        ParquetType::Timestamp(name, _) => {
            Ok(StructField::new(name, DataType::TIMESTAMP_NTZ, true))
        }
        ParquetType::TimestampTz(name, _) => Ok(StructField::new(name, DataType::TIMESTAMP, true)),
        ParquetType::Uuid(name) => Ok(StructField::new(name, DataType::STRING, true)
            .with_metadata([("parquet_type", "UUID")])),
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

pub async fn write(
    table: &mut DeltaTable,
    stream: ParquetRecordBatchStream<ParquetObjectReader>,
    mode: deltalake::protocol::SaveMode,
) -> Result<(), anyhow::Error> {
    let builder = WriteBuilder::new(table.log_store(), table.state.clone()).with_save_mode(mode);
    let table = to_lazy_table(stream)?;
    let plan = LogicalPlanBuilder::scan("source", provider_as_source(table), None)?.build()?;
    info!("plan: {:?}", plan);
    let txn = Transaction::new("ldrs v0.9.0", 1);
    let commit = CommitProperties::default().with_application_transaction(txn);
    builder
        .with_input_execution_plan(Arc::new(plan))
        .with_commit_properties(commit)
        .await?;
    Ok(())
}

pub fn to_lazy_table(
    stream: ParquetRecordBatchStream<ParquetObjectReader>,
) -> DeltaResult<Arc<dyn TableProvider>> {
    use deltalake::delta_datafusion::LazyTableProvider;
    use parking_lot::RwLock;
    use std::sync::{Arc, Mutex};

    let stream_mutex = Arc::new(Mutex::new(stream));

    #[derive(Debug)]
    struct ParquetStreamBatchGenerator {
        stream: Arc<Mutex<ParquetRecordBatchStream<ParquetObjectReader>>>,
        conversions: Vec<ColumnConversion>,
        target_schema: SchemaRef,
    }

    impl ParquetStreamBatchGenerator {
        fn new(stream: Arc<Mutex<ParquetRecordBatchStream<ParquetObjectReader>>>) -> Self {
            let schema = {
                let locked_stream = stream.lock().unwrap();
                locked_stream.schema().clone()
            };
            let conversions = analyze_schema_conversions(&schema);
            info!("conversions: {:?}", conversions);
            let target_schema = Arc::new(Schema::new(
                schema
                    .fields()
                    .iter()
                    .zip(conversions.iter())
                    .map(|(field, conversion)| map_convert_schema(field, conversion))
                    .collect::<Vec<_>>(),
            ));
            Self {
                stream,
                conversions,
                target_schema,
            }
        }
    }

    impl std::fmt::Display for ParquetStreamBatchGenerator {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ParquetStreamBatchGenerator")
        }
    }

    impl LazyBatchGenerator for ParquetStreamBatchGenerator {
        fn generate_next_batch(
            &mut self,
        ) -> deltalake::datafusion::error::Result<Option<RecordBatch>> {
            use futures::{executor::block_on, StreamExt};

            // Get the next batch from the stream
            let mut stream = self.stream.lock().map_err(|_| {
                deltalake::datafusion::error::DataFusionError::Execution(
                    "Failed to lock the ArrowArrayStreamReader".to_string(),
                )
            })?;

            let next_result = block_on(stream.next());
            info!("loading next batch ");

            match next_result {
                Some(Ok(batch)) => {
                    // Apply conversions to the batch
                    let converted_batch =
                        convert_batch(&batch, &self.conversions).map_err(|e| {
                            deltalake::datafusion::error::DataFusionError::Execution(format!(
                                "Failed to convert batch: {}",
                                e
                            ))
                        })?;

                    Ok(Some(converted_batch))
                }
                Some(Err(err)) => Err(deltalake::datafusion::error::DataFusionError::ArrowError(
                    err.into(),
                    None,
                )),
                None => Ok(None), // End of stream
            }
        }
    }

    let generator = ParquetStreamBatchGenerator::new(stream_mutex);
    let target_schema = generator.target_schema.clone();

    let parquet_generator: Arc<RwLock<dyn LazyBatchGenerator>> = Arc::new(RwLock::new(generator));

    // Create the LazyTableProvider with our generator
    Ok(Arc::new(LazyTableProvider::try_new(
        target_schema,
        vec![parquet_generator],
    )?))
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
    use super::*;
    use deltalake::datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_delta_lake_from_parquet() {
        let cd = std::env::current_dir().unwrap();
        let file = format!("{}/test_data/public.users.snappy.parquet", cd.display());
        let delta_root = format!("{}/test_data/delta", cd.display());
        let delta_load = DeltaLoad {
            delta_root: delta_root.clone(),
            file: file.clone(),
            table_name: "users".to_string(),
            load_mode: deltalake::protocol::SaveMode::Overwrite,
        };
        delta_run(&delta_load).await.unwrap();

        let table = deltalake::open_table(format!("{}/users", delta_root.clone()))
            .await
            .unwrap();
        let ctx = SessionContext::new();
        let dataframe = ctx.read_table(Arc::new(table.clone())).unwrap();
        let results = dataframe.collect().await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2);
        assert_eq!(results[0].schema().fields.len(), 6);

        // it is overwrite so run it again
        delta_run(&delta_load).await.unwrap();
        let dataframe = ctx.read_table(Arc::new(table.clone())).unwrap();
        let results = dataframe.collect().await.unwrap();
        assert_eq!(results.len(), 1);

        // now run an append
        let delta_load = DeltaLoad {
            delta_root: delta_root.clone(),
            file,
            table_name: "users".to_string(),
            load_mode: deltalake::protocol::SaveMode::Append,
        };
        delta_run(&delta_load).await.unwrap();
        let table = deltalake::open_table(format!("{}/users", delta_root))
            .await
            .unwrap();

        let dataframe = ctx.read_table(Arc::new(table.clone())).unwrap();
        let results = dataframe.collect().await.unwrap();

        assert_eq!(results.len(), 2);
    }
}
