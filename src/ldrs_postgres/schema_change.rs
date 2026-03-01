use crate::{ldrs_schema::SchemaChange, types::ColumnSpec};

impl<'a> SchemaChange<'a> {
    pub fn to_postgres_final_column_ddl(&self) -> Vec<String> {
        self.final_schema
            .iter()
            .map(|col| map_colspec_to_pg_ddl(col))
            .collect()
    }
}

pub fn map_colspec_to_pg_ddl(pq: &ColumnSpec) -> String {
    match pq {
        ColumnSpec::SmallInt { name } => format!("{} smallint", name),
        ColumnSpec::BigInt { name } => format!("{} bigint", name),
        ColumnSpec::Boolean { name } => format!("{} boolean", name),
        ColumnSpec::Double { name } => format!("{} double precision", name),
        ColumnSpec::Integer { name } => format!("{} integer", name),
        ColumnSpec::Jsonb { name } => format!("{} jsonb", name),
        ColumnSpec::Numeric {
            name,
            precision,
            scale,
        } => format!("{} numeric({}, {})", name, precision, scale),
        ColumnSpec::Timestamp { name, .. } => format!("{} timestamp", name),
        ColumnSpec::TimestampTz { name, .. } => format!("{} timestamptz", name),
        ColumnSpec::Date { name } => format!("{} date", name),
        ColumnSpec::Real { name } => format!("{} real", name),
        ColumnSpec::Text { name } => format!("{} text", name),
        ColumnSpec::Uuid { name } => format!("{} uuid", name),
        ColumnSpec::Varchar { name, length } => format!("{} varchar({})", name, length),
        ColumnSpec::Custom { name, ddl_type } => format!("{} {}", name, ddl_type),
        ColumnSpec::Bytea { name } => format!("{} bytea", name),
        ColumnSpec::FixedSizeBinary { name, .. } => format!("{} bytea", name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_parquet_to_ddl() {
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::SmallInt { name: "id".into() }),
            "id smallint"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::BigInt { name: "id".into() }),
            "id bigint"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Boolean {
                name: "is_active".into()
            }),
            "is_active boolean"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Double {
                name: "price".into()
            }),
            "price double precision"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Integer { name: "age".into() }),
            "age integer"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Jsonb {
                name: "data".into()
            }),
            "data jsonb"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Numeric {
                name: "amount".into(),
                precision: 10,
                scale: 2,
            }),
            "amount numeric(10, 2)"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Timestamp {
                name: "created_at".into(),
                time_unit: crate::types::TimeUnit::Millis,
            }),
            "created_at timestamp"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::TimestampTz {
                name: "updated_at".into(),
                time_unit: crate::types::TimeUnit::Micros,
            }),
            "updated_at timestamptz"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Date { name: "dob".into() }),
            "dob date"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Real {
                name: "score".into()
            }),
            "score real"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Text {
                name: "description".into()
            }),
            "description text"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Uuid { name: "id".into() }),
            "id uuid"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Varchar {
                name: "name".into(),
                length: 100
            }),
            "name varchar(100)"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Custom {
                name: "custom".into(),
                ddl_type: "custom_type".into(),
            }),
            "custom custom_type"
        );
        assert_eq!(
            map_colspec_to_pg_ddl(&ColumnSpec::Bytea {
                name: "data".into()
            }),
            "data bytea"
        );
    }

    #[test]
    fn test_final_column_schema() {
        let current = vec![];
        let next = vec![
            ColumnSpec::Integer { name: "id".into() },
            ColumnSpec::Text {
                name: "name".into(),
            },
            ColumnSpec::Varchar {
                name: "email".into(),
                length: 255,
            },
        ];
        let schema_change = SchemaChange::build_from_columns(&current, &next);
        let ddl = schema_change.to_postgres_final_column_ddl();
        assert_eq!(ddl[0], "id integer");
        assert_eq!(ddl[1], "name text");
        assert_eq!(ddl[2], "email varchar(255)");
    }
}
