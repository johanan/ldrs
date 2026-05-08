use ldrs_postgres::map_colspec_to_pg_ddl;

use crate::ldrs_schema::SchemaChange;

impl<'a> SchemaChange<'a> {
    pub fn to_postgres_final_column_ddl(&self) -> Vec<String> {
        self.final_schema
            .iter()
            .map(|col| map_colspec_to_pg_ddl(col))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use ldrs_arrow::ColumnSpec;

    use super::*;

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
