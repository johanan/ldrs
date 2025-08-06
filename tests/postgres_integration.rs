use std::path::Path;

use ldrs::{
    ldrs_postgres::{build_ddl, create_connection, map_parquet_to_ddl},
    lua_logic::{LuaFunctionLoader, StorageData, UrlData},
    parquet_provider::builder_from_string,
    path_pattern::{self, build_module_path_from_pattern},
    storage::StorageProvider,
    types::{
        parquet_types::{get_fields, ParquetSchema},
        ColumnSchema, TableSchema,
    },
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TestLuaResult {
    pub sql: Vec<String>,
    pub schema: TableSchema,
}

//#[tokio::test]
async fn test_postgres_integration() {
    let file = "tests/test_data/public.string_values/public.strings.snappy.parquet";
    let storage = StorageProvider::try_from_string(&file).unwrap();
    let pattern_string = "{_}/{_}/{_}/{_}/tests/test_data/{schema}.{table}/*";
    let pattern = path_pattern::PathPattern::new(pattern_string).unwrap();
    let (_, file_path) = storage.get_store_and_path().unwrap();
    let file_path = file_path.to_string();
    let extracted = pattern.parse_path(&file_path).unwrap();
    let segments_value = path_pattern::extracted_segments_to_value(&extracted);
    println!("segments_value: {:?}", segments_value);
    println!("extracted: {:?}", extracted);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let builder = builder_from_string(file.to_string(), rt.handle().clone())
        .await
        .unwrap();
    print!("schema {:?}\n", builder.schema());

    let module_paths = build_module_path_from_pattern(&pattern, "tests/lua_mod");
    println!("module_paths: {:?}", module_paths);
    let mut loader = LuaFunctionLoader::new().unwrap();
    let lua_schema: ParquetSchema<'_> = builder.metadata().file_metadata().try_into().unwrap();
    println!("lua_schema: {:?}", lua_schema);

    loader.setup_execution_context(Some(lua_schema)).unwrap();

    let url_data: UrlData = storage.get_url().into();
    let storage_data: StorageData = storage.into();
    let context = serde_json::json!({});

    let lua_output = loader
        .call_process::<TestLuaResult>(
            &module_paths,
            &url_data,
            &storage_data,
            &segments_value,
            Some(builder.schema()),
            &context,
        )
        .unwrap();
    println!("lua_output: {:?}", lua_output);
    let lua_output_schema_columns = lua_output
        .schema
        .columns
        .iter()
        .map(|column| column.to_column_schema(&column.name))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    println!("lua_output_schema_columns: {:?}", lua_output_schema_columns);

    let default_schema = get_fields(builder.metadata().file_metadata())
        .unwrap()
        .iter()
        .map(|c| ColumnSchema::try_from(c))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    println!("default_schema: {:?}", default_schema);

    let pg_url = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable";
    let mut conn = create_connection(pg_url).await.unwrap();

    let tx = conn.transaction().await.unwrap();
    for statement in lua_output.sql {
        tx.execute(&statement, &[]).await.unwrap();
    }

    // Merge default schema with lua overrides
    use std::collections::HashMap;
    let lua_overrides: HashMap<&str, &ColumnSchema> = lua_output_schema_columns
        .iter()
        .map(|col| (col.name(), col))
        .collect();

    let merged_columns: Vec<&ColumnSchema> = default_schema
        .iter()
        .map(|default_col| {
            lua_overrides
                .get(default_col.name())
                .map(|&override_col| override_col)
                .unwrap_or(default_col)
        })
        .collect();

    println!("merged_columns: {:?}", merged_columns);

    let ddl_columns = merged_columns
        .into_iter()
        .map(map_parquet_to_ddl)
        .collect::<Vec<String>>();

    let ddl_statement = build_ddl(lua_output.schema.table_name, &ddl_columns);
    println!("{}", ddl_statement);

    tx.execute(&ddl_statement, &[]).await.unwrap();

    tx.commit().await.unwrap();

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}
