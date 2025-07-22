use std::path::Path;

use ldrs::{
    ldrs_snowflake::SnowflakeLoadMode,
    lua_logic::{
        build_module_path_from_pattern, nom_pattern, StorageData, UrlData, LuaFunctionLoader, LuaResult, ExecResult, PreExecResult,
    },
    parquet_provider::builder_from_string,
    storage::StorageProvider,
    types::parquet_types::{ParquetSchema},
};

#[tokio::test]
async fn test_snowflake_lua_integration() {
    let file = "tests/test_data/public.string_values/public.strings.snappy.parquet";
    let storage = StorageProvider::try_from_string(&file).unwrap();
    let pattern_string = "{_}/{_}/{_}/{_}/tests/test_data/{schema}.{table}/*";
    let pattern = nom_pattern::PathPattern::new(pattern_string).unwrap();
    let (_, file_path) = storage.get_store_and_path().unwrap();
    let file_path = file_path.to_string();
    let extracted = pattern.parse_path(&file_path).unwrap();
    let segments_value = nom_pattern::extracted_segments_to_value(&extracted);
    println!("segments_value: {:?}", segments_value);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();

    let builder = builder_from_string(file.to_string(), rt.handle().clone())
        .await
        .unwrap();

    // Use snowflake-specific module path
    let module_paths = build_module_path_from_pattern(&pattern, "tests/lua_mod_snowflake");
    println!("module_paths: {:?}", module_paths);
    
    let mut loader = LuaFunctionLoader::new().unwrap();
    let lua_schema: ParquetSchema<'_> = builder.metadata().file_metadata().try_into().unwrap();
    
    loader.setup_execution_context(Some(lua_schema)).unwrap();

    // Test pre_exec phase
    let pre_exec_func = loader
        .find_and_load_function(&module_paths, "pre_exec")
        .unwrap()
        .unwrap();
    
    let url_data: UrlData = storage.get_url().into();
    let storage_data: StorageData = storage.into();
    let context = serde_json::json!({"destination": "snowflake"});

    let pre_exec_result: PreExecResult = loader
        .call_pre_exec(
            pre_exec_func,
            &url_data,
            &storage_data,
            &segments_value,
            builder.schema(),
            &context,
        )
        .unwrap();
    
    println!("pre_exec_result: {:?}", pre_exec_result);
    
    // Verify pre_exec returns expected SQL
    assert!(!pre_exec_result.sql.is_empty());
    assert!(pre_exec_result.sql[0].contains("CREATE"));

    // Test exec phase
    let exec_func = loader
        .find_and_load_function(&module_paths, "exec")
        .unwrap()
        .unwrap();

    let exec_result: ExecResult<SnowflakeLoadMode> = loader
        .call_exec(
            exec_func,
            &url_data,
            &storage_data,
            &segments_value,
            builder.schema(),
            &pre_exec_result.context, // Pass context from pre_exec
        )
        .unwrap();
    
    println!("exec_result: {:?}", exec_result);
    
    // Verify exec returns expected SQL 
    assert!(!exec_result.sql.is_empty());
    assert!(exec_result.sql.iter().any(|sql| sql.contains("COPY")));

    // Test post_exec phase
    let post_exec_func = loader
        .find_and_load_function(&module_paths, "post_exec")
        .unwrap()
        .unwrap();

    let post_exec_result: LuaResult = loader
        .call_post_exec(
            post_exec_func,
            &url_data,
            &storage_data,
            &segments_value,
            builder.schema(),
            &exec_result.context, // Pass context from exec
        )
        .unwrap();
    
    println!("post_exec_result: {:?}", post_exec_result);
    
    // Verify post_exec returns expected SQL
    assert!(!post_exec_result.sql.is_empty());
    assert!(post_exec_result.sql.iter().any(|sql| sql.contains("DROP")));

    // Test transaction SQL generation
    let all_sql = [
        pre_exec_result.sql,
        exec_result.sql, 
        post_exec_result.sql,
    ].concat();
    
    println!("All SQL statements:");
    for (i, sql) in all_sql.iter().enumerate() {
        println!("{}: {}", i + 1, sql);
    }
    
    assert!(all_sql.len() >= 3, "Should have at least 3 SQL statements");

    tokio::runtime::Handle::current().spawn_blocking(move || drop(rt));
}