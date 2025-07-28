function process(url_data, storage_data, segments, schema, context)
    print("Executing pre-execution logic")
    local table_name = segments["table"]
    local schema_name = segments["schema"]

    -- Access parquet schema via global value
    if parquet_schema then
        print("Parquet schema available: " .. parquet_schema.name)
        print("Number of columns: " .. #parquet_schema.columns)
        print("First column name: " .. parquet_schema.columns[1].name)
    else
        print("No parquet schema available")
    end

    return {
        sql = { "CREATE SCHEMA IF NOT EXISTS " .. schema_name .. "ldrs" },
        schema = {
            table_name = schema_name .. "ldrs." .. table_name .. "_TEST",
            columns = {
                { name = "varchar_value", column_type = "VARCHAR(255)", length = 255 }
            }
        },
    }
end
