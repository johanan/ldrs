use crate::types::ColumnSchema;

pub enum SchemaMode {
    Strict,
    Additive,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ColumnChange<'a> {
    Modified(&'a ColumnSchema<'a>),
    Removed,
}

pub struct SchemaChange<'a> {
    pub columns: Vec<&'a ColumnSchema<'a>>,
    pub additions: Vec<&'a ColumnSchema<'a>>,
    pub changes: Vec<Option<ColumnChange<'a>>>,
    pub final_schema: Vec<&'a ColumnSchema<'a>>,
}

impl<'a> SchemaChange<'a> {
    pub fn build_from_columns(
        current: &'a [ColumnSchema<'a>],
        next: &'a [ColumnSchema<'a>],
    ) -> Self {
        let current_ref = current.iter().collect::<Vec<&ColumnSchema<'_>>>();
        let final_schema = next.iter().collect::<Vec<&ColumnSchema<'_>>>();
        let mut additions = next.iter().collect::<Vec<&ColumnSchema<'_>>>();

        additions.retain(|item| !current_ref.contains(item));

        // now go through each item in current_ref and see if it exists in final_ref
        // I think we can use swap_remove for possible_changes
        let mut possible_changes = next.iter().collect::<Vec<&ColumnSchema<'_>>>();
        let mut changes = Vec::new();
        for &item in current_ref.iter() {
            if let Some(index) = possible_changes
                .iter()
                .position(|x| x.name().eq_ignore_ascii_case(&item.name()))
            {
                let matched = possible_changes.swap_remove(index);
                // now check if the items are equal
                // there is an edge case around case difference, but we can ignore it for now
                if matched != item {
                    changes.push(Some(ColumnChange::Modified(item)));
                } else {
                    changes.push(None);
                }
            } else {
                changes.push(Some(ColumnChange::Removed));
            }
        }
        Self {
            columns: current_ref,
            additions,
            changes,
            final_schema,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_from_columns_with_no_changes() {
        let current = vec![ColumnSchema::Integer("id"), ColumnSchema::Text("name")];
        let next = vec![ColumnSchema::Integer("id"), ColumnSchema::Text("name")];
        let schema_change = SchemaChange::build_from_columns(&current, &next);
        assert_eq!(schema_change.columns.len(), 2);
        assert_eq!(schema_change.additions.len(), 0);
        assert_eq!(schema_change.changes.len(), 2);
        assert_eq!(schema_change.changes, vec![None, None]);
        assert_eq!(schema_change.final_schema.len(), 2);
    }

    #[test]
    fn test_build_from_columns_by_only_adding() {
        let current = vec![ColumnSchema::Integer("id"), ColumnSchema::Text("name")];
        let next = vec![
            ColumnSchema::Integer("id"),
            ColumnSchema::Text("name"),
            ColumnSchema::Varchar("email", 255),
        ];
        let schema_change = SchemaChange::build_from_columns(&current, &next);
        assert_eq!(schema_change.columns.len(), 2);
        assert_eq!(schema_change.additions.len(), 1);
        assert_eq!(schema_change.changes.len(), 2);
        assert_eq!(schema_change.changes, vec![None, None]);
        assert_eq!(schema_change.final_schema.len(), 3);
    }

    #[test]
    fn test_build_from_columns() {
        let current = vec![ColumnSchema::Integer("id"), ColumnSchema::Text("name")];
        let next = vec![
            ColumnSchema::Integer("id"),
            ColumnSchema::Varchar("email", 255),
        ];
        let schema_change = SchemaChange::build_from_columns(&current, &next);
        assert_eq!(schema_change.columns.len(), 2);
        assert_eq!(schema_change.additions.len(), 1);
        assert_eq!(
            schema_change.additions,
            vec![&ColumnSchema::Varchar("email", 255)]
        );
        assert_eq!(schema_change.changes.len(), 2);
        assert_eq!(
            schema_change.changes,
            vec![None, Some(ColumnChange::Removed)]
        );
        assert_eq!(schema_change.final_schema.len(), 2);
    }
}
