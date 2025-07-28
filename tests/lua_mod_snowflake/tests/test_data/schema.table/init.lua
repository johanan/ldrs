function process(url_data, storage_data, segments, schema, context)
    print("Executing Snowflake pre-execution logic")
    local table_name = segments["table"]
    local schema_name = segments["schema"]

    return {
        pre_sql = {
            "CREATE SCHEMA IF NOT EXISTS " .. schema_name .. "_TEMP",
            "CREATE OR REPLACE TABLE " .. schema_name .. "_TEMP." .. table_name .. "_STAGE (" ..
            "varchar_value VARCHAR(255), " ..
            "created_at TIMESTAMP_NTZ" ..
            ")"
        },
        strategy = {
            Sql = { "COPY INTO table FROM @stage", "ANALYZE TABLE table" }
        },
        post_sql = {
            "DROP TABLE IF EXISTS " .. schema_name .. "_TEMP." .. table_name .. "_STAGE",
            "DROP SCHEMA IF EXISTS " .. schema_name .. "_TEMP"
        },
    }
end
