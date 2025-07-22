function pre_exec(url_data, storage_data, segments, schema, context)
    print("Executing Snowflake pre-execution logic")
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
        sql = {
            "CREATE SCHEMA IF NOT EXISTS " .. schema_name .. "_TEMP",
            "CREATE OR REPLACE TABLE " .. schema_name .. "_TEMP." .. table_name .. "_STAGE (" ..
            "varchar_value VARCHAR(255), " ..
            "created_at TIMESTAMP_NTZ" ..
            ")"
        },
        context = {
            temp_schema = schema_name .. "_TEMP",
            temp_table = schema_name .. "_TEMP." .. table_name .. "_STAGE",
            target_table = schema_name .. "." .. table_name
        }
    }
end

function exec(url_data, storage_data, segments, schema, context)
    local temp_table = context["temp_table"]
    local target_table = context["target_table"]
    
    -- Generate Snowflake COPY command (static for test)
    local copy_sql = "COPY INTO " .. temp_table .. 
                     " FROM '" .. url_data.full_url .. "' " ..
                     "FILE_FORMAT = (TYPE = 'PARQUET')"
    
    return {
        sql = {
            copy_sql,
            "BEGIN",
            "DROP TABLE IF EXISTS " .. target_table .. "_BACKUP",
            "ALTER TABLE " .. target_table .. " RENAME TO " .. target_table .. "_BACKUP",
            "ALTER TABLE " .. temp_table .. " RENAME TO " .. target_table,
            "COMMIT"
        },
        load_mode = "Copy",
        context = {
            final_table = target_table,
            backup_table = target_table .. "_BACKUP"
        }
    }
end

function post_exec(url_data, storage_data, segments, schema, context)
    local backup_table = context["backup_table"]
    local temp_schema = context["temp_schema"]
    
    return {
        sql = {
            "DROP TABLE IF EXISTS " .. backup_table,
            "DROP SCHEMA IF EXISTS " .. temp_schema
        },
        context = {}
    }
end