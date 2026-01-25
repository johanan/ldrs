use std::{path::Path, pin::Pin};

use arrow_array::RecordBatch;
use futures::Stream;
use parquet::arrow::{
    arrow_reader::ArrowReaderBuilder,
    async_reader::{AsyncReader, ParquetObjectReader},
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use url::Url;

use crate::{parquet_provider::builder_from_url, pq::get_fields, types::ColumnSchema};

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct FileSource {
    pub name: String,
    pub filename: Option<String>,
}

pub fn get_full_path(source: &FileSource) -> String {
    match &source.filename {
        Some(filename) => filename.clone(),
        None => source.name.clone(),
    }
}

pub fn is_object_store_url(url: &Url) -> bool {
    matches!(
        url.scheme(),
        "file" | "az" | "adl" | "azure" | "abfs" | "abfss" | "https"
    )
}

fn find_source<'a>(sources: &'a [(String, Url)], key: Option<&str>) -> Option<&'a (String, Url)> {
    let target = match key {
        Some(k) => format!("src_{}", k),
        None => "src".to_string(),
    };

    sources.iter().find(|(scheme, _)| scheme == &target)
}

pub struct ObjectStoreSource<'a> {
    columns: Vec<Vec<ColumnSchema<'a>>>,
    stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, anyhow::Error>> + Send>>,
}

pub struct ParquetSource {
    builder: ArrowReaderBuilder<AsyncReader<ParquetObjectReader>>,
    schema: arrow_schema::SchemaRef,
    metadata: parquet::file::metadata::FileMetaData,
}

async fn create_parquet_source(url: Url, runtime: Handle) -> Result<ParquetSource, anyhow::Error> {
    let builder = builder_from_url(url, runtime).await?;
    let schema = builder.schema().clone();
    let file_md = builder.metadata().file_metadata().clone();

    Ok(ParquetSource {
        builder,
        schema,
        metadata: file_md,
    })
}

fn get_file_extension(url: &Url) -> Option<&str> {
    let path = Path::new(url.path());
    path.extension().and_then(osstr_to_string)
}

pub fn find_connection<'a>(
    connections: &'a [(String, Url)],
    key: Option<&str>,
) -> Result<&'a Url, anyhow::Error> {
    find_source(connections, key)
        .map(|(_, url)| url)
        .ok_or_else(|| anyhow::anyhow!("No source found"))
}

fn osstr_to_string(os_str: &std::ffi::OsStr) -> Option<&str> {
    os_str.to_str()
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::TryStreamExt;
    use tokio_stream::StreamExt;

    use crate::{
        storage::base_or_relative_path,
        test_utils::{create_runtime, drop_runtime},
    };

    use super::*;

    #[test]
    fn test_azure_filename() {
        let azure_url =
            Url::parse("https://myaccount.blob.core.windows.net/mycontainer/myblob.parquet")
                .unwrap();
        let conns = vec![(String::from("src_azure"), azure_url.clone())];
        let azure_url = find_connection(&conns, Some("azure")).unwrap();
        assert_eq!(azure_url, azure_url);
        let extension = get_file_extension(azure_url).unwrap();
        assert_eq!(extension, "parquet");
    }

    #[tokio::test]
    async fn test_parquet_source() {
        let cd = std::env::current_dir().unwrap();
        let path = format!("{}/test_data/public.users.snappy.parquet", cd.display());
        let file_url = base_or_relative_path(&path).unwrap();
        let rt = create_runtime();
        let conns = vec![(String::from("src_file"), file_url.clone())];

        let src_url = find_connection(&conns, Some("file")).unwrap();
        let extension = get_file_extension(src_url).unwrap();
        assert_eq!(extension, "parquet");
        let pq_source = create_parquet_source(src_url.clone(), rt.handle().clone())
            .await
            .unwrap();
        // turn into a objectstoresourc
        let fields = get_fields(&pq_source.metadata).unwrap();
        let mapped = fields
            .iter()
            .filter_map(|pq| ColumnSchema::try_from(pq).ok())
            .collect::<Vec<_>>();
        let stream = pq_source.builder.with_batch_size(1024).build().unwrap();
        // let object_store_source = ObjectStoreSource {
        //     columns: vec![mapped],
        //     stream: Box::pin(stream.map_err(|e| anyhow::Error::from(e))),
        // };
        // read the batches now
        let mut stream = stream.into_stream();
        let mut stream = pin!(stream);
        while let Some(batch_result) = stream.next().await {
            match batch_result {
                Ok(batch) => {
                    println!("Batch: {:?}", batch);
                }
                Err(e) => {
                    // Handle the error here
                }
            }
        }
        drop_runtime(rt);
    }
}
