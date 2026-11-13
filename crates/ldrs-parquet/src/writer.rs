use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;

pub fn default_writer_props() -> WriterProperties {
    WriterPropertiesBuilder::default()
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
        .set_compression(parquet::basic::Compression::SNAPPY)
        .set_encoding(parquet::basic::Encoding::PLAIN)
        .build()
}

pub fn with_bloom_filters(
    props: WriterProperties,
    bloom_filters: Vec<Vec<String>>,
) -> WriterProperties {
    let mut builder = props.into_builder();
    for filter in bloom_filters {
        builder = builder.set_column_bloom_filter_enabled(ColumnPath::from(filter), true);
    }
    builder.build()
}
