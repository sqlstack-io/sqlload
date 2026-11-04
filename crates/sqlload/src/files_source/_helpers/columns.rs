use arrow_schema::SchemaRef;

use crate::column_types::main::portable_type::portable_type;
use crate::column_types::models::ColumnType;
use crate::files_source::models::FileColumn;

/// `columns` plus `schema`'s new columns, and a problem for each column whose type differs from an earlier file's.
pub(crate) fn merge_schema(
    mut columns: Vec<FileColumn>,
    schema: &SchemaRef,
    key: &str,
) -> (Vec<FileColumn>, Vec<String>) {
    let mut problems: Vec<String> = Vec::new();
    for field in schema.fields() {
        let column_type = portable_type(field);
        match columns.iter().find(|column| column.name == *field.name()) {
            None => columns.push(FileColumn {
                name: field.name().clone(),
                column_type,
            }),
            Some(existing) if existing.column_type == column_type => {}
            Some(existing) => problems.push(format!(
                "column {} is {} in newer files but {} in {key}; pin a type that holds both",
                field.name(),
                describe(&existing.column_type),
                describe(&column_type)
            )),
        }
    }
    (columns, problems)
}

fn describe(column_type: &Result<ColumnType, String>) -> String {
    match column_type {
        Ok(column_type) => column_type.to_string(),
        Err(_) => "unsupported".to_owned(),
    }
}
