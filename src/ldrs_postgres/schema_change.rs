use crate::{ldrs_schema::SchemaChange, types::ColumnSchema};

impl<'a> SchemaChange<'a> {
    pub fn to_postgres_final_column_ddl(&self) -> Vec<String> {
        self.final_schema
            .iter()
            .map(|col| map_parquet_to_ddl(col))
            .collect()
    }
}

pub fn map_parquet_to_ddl(pq: &ColumnSchema) -> String {
    match pq {
        ColumnSchema::SmallInt(name) => format!("{} smallint", name),
        ColumnSchema::BigInt(name) => format!("{} bigint", name),
        ColumnSchema::Boolean(name) => format!("{} boolean", name),
        ColumnSchema::Double(name, _) => format!("{} double precision", name),
        ColumnSchema::Integer(name) => format!("{} integer", name),
        ColumnSchema::Jsonb(name) => format!("{} jsonb", name),
        ColumnSchema::Numeric(name, precision, scale) => {
            format!("{} numeric({}, {})", name, precision, scale)
        }
        ColumnSchema::Timestamp(name, ..) => format!("{} timestamp", name),
        ColumnSchema::TimestampTz(name, ..) => format!("{} timestamptz", name),
        ColumnSchema::Date(name) => format!("{} date", name),
        ColumnSchema::Real(name) => format!("{} real", name),
        ColumnSchema::Text(name) => format!("{} text", name),
        ColumnSchema::Uuid(name) => format!("{} uuid", name),
        ColumnSchema::Varchar(name, size) => format!("{} varchar({})", name, size),
        ColumnSchema::Custom(name, type_name) => format!("{} {}", name, type_name),
        ColumnSchema::Bytea(name) => format!("{} bytea", name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_parquet_to_ddl() {
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::SmallInt("id")),
            "id smallint"
        );
        assert_eq!(map_parquet_to_ddl(&ColumnSchema::BigInt("id")), "id bigint");
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::Boolean("is_active")),
            "is_active boolean"
        );
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::Double("price", None)),
            "price double precision"
        );
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::Integer("age")),
            "age integer"
        );
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::Jsonb("data")),
            "data jsonb"
        );
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::Numeric("amount", 10, 2)),
            "amount numeric(10, 2)"
        );
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::Timestamp(
                "created_at",
                crate::types::TimeUnit::Millis
            )),
            "created_at timestamp"
        );
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::TimestampTz(
                "updated_at",
                crate::types::TimeUnit::Micros
            )),
            "updated_at timestamptz"
        );
        assert_eq!(map_parquet_to_ddl(&ColumnSchema::Date("dob")), "dob date");
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::Real("score")),
            "score real"
        );
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::Text("description")),
            "description text"
        );
        assert_eq!(map_parquet_to_ddl(&ColumnSchema::Uuid("id")), "id uuid");
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::Varchar("name", 100)),
            "name varchar(100)"
        );
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::Custom("custom", "custom_type".to_string())),
            "custom custom_type"
        );
        assert_eq!(
            map_parquet_to_ddl(&ColumnSchema::Bytea("data")),
            "data bytea"
        );
    }

    #[test]
    fn test_final_column_schema() {
        let current = vec![];
        let next = vec![
            ColumnSchema::Integer("id"),
            ColumnSchema::Text("name"),
            ColumnSchema::Varchar("email", 255),
        ];
        let schema_change = SchemaChange::build_from_columns(&current, &next);
        let ddl = schema_change.to_postgres_final_column_ddl();
        assert_eq!(ddl[0], "id integer");
        assert_eq!(ddl[1], "name text");
        assert_eq!(ddl[2], "email varchar(255)");
    }
}
