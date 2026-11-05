use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;

/// `name` in double quotes, exactly as written.
pub(crate) fn exact_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A fully qualified table name.
pub(crate) fn qualified<A: SqlAdapter + ?Sized>(adapter: &A, table: &TableName) -> String {
    let schema_and_table = format!(
        "{}.{}",
        adapter.render_identifier(&table.schema),
        adapter.render_identifier(&table.table)
    );
    match &table.database {
        Some(database) => format!("{}.{schema_and_table}", adapter.render_identifier(database)),
        None => schema_and_table,
    }
}

/// `text` as a standard SQL string literal.
pub(crate) fn string_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The same table with `suffix` added to its name.
pub(crate) fn with_suffix(table: &TableName, suffix: &str) -> TableName {
    TableName {
        table: format!("{}{suffix}", table.table),
        ..table.clone()
    }
}

/// The type a staging column has: what the loaded files or batches carry (JSON and UUIDs as text).
pub(crate) fn staging_type<A: SqlAdapter + ?Sized>(
    adapter: &A,
    column_type: &ColumnType,
) -> String {
    match column_type {
        ColumnType::Json | ColumnType::Uuid => {
            adapter.render_loader_logical_type(&ColumnType::Text).sql
        }
        other => adapter.render_loader_logical_type(other).sql,
    }
}

/// The expression turning staging column `name` into the table's type: JSON and UUIDs cast from their text.
pub(crate) fn source_expression_cast<A: SqlAdapter + ?Sized>(
    adapter: &A,
    name: &str,
    column_type: &ColumnType,
) -> String {
    let column = adapter.render_identifier(name);
    match column_type {
        ColumnType::Json | ColumnType::Uuid => format!(
            "CAST({column} AS {})",
            adapter.render_loader_logical_type(column_type).sql
        ),
        _ => column,
    }
}
