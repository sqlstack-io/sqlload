use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{PgType, SourceColumn, SourceTable};

/// A source table of text `columns`.
pub(crate) fn table(schema: &str, name: &str, columns: &[&str]) -> SourceTable {
    SourceTable {
        schema: schema.to_owned(),
        name: name.to_owned(),
        columns: columns
            .iter()
            .map(|column| SourceColumn {
                name: (*column).to_owned(),
                postgres_type: "Utf8".to_owned(),
                pg_type: PgType::Other,
                column_type: Ok(ColumnType::Text),
                nullable: true,
                source_spelling: None,
            })
            .collect(),
        primary_key: Vec::new(),
        indexed_columns: Vec::new(),
        estimated_rows: -1,
        estimated_bytes: -1,
        comment: None,
    }
}
