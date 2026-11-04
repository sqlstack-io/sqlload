use crate::column_types::main::warehouse_type::warehouse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::write_sql::models::TableName;

/// Whether `name` is lower snake_case, which Snowflake stores upper-cased like an unquoted name.
fn is_plain(name: &str) -> bool {
    name.chars()
        .next()
        .is_some_and(|first| first.is_ascii_lowercase() || first == '_')
        && name.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}

/// `name` quoted for `warehouse`; plain snake_case names follow Snowflake's upper-case convention.
pub(crate) fn quote(name: &str, warehouse: Warehouse) -> String {
    let stored = match warehouse {
        Warehouse::Snowflake if is_plain(name) => name.to_ascii_uppercase(),
        _ => name.to_owned(),
    };
    format!("\"{}\"", stored.replace('"', "\"\""))
}

/// A fully qualified table name.
pub(crate) fn qualified(table: &TableName, warehouse: Warehouse) -> String {
    let schema_and_table = format!(
        "{}.{}",
        quote(&table.schema, warehouse),
        quote(&table.table, warehouse)
    );
    match &table.database {
        Some(database) => format!("{}.{schema_and_table}", quote(database, warehouse)),
        None => schema_and_table,
    }
}

/// The same table with `suffix` added to its name.
pub(crate) fn with_suffix(table: &TableName, suffix: &str) -> TableName {
    TableName {
        table: format!("{}{suffix}", table.table),
        ..table.clone()
    }
}

/// `text` as a SQL string literal.
pub(crate) fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The type a staging column has: what the loaded files or batches carry (JSON and UUIDs as text).
pub(crate) fn staging_type(column_type: &ColumnType, warehouse: Warehouse) -> String {
    match column_type {
        ColumnType::Json | ColumnType::Uuid => warehouse_type(&ColumnType::Text, warehouse).sql,
        other => warehouse_type(other, warehouse).sql,
    }
}

/// The expression turning staging column `name` into the table's type.
pub(crate) fn from_staging(name: &str, column_type: &ColumnType, warehouse: Warehouse) -> String {
    let column = quote(name, warehouse);
    match (column_type, warehouse) {
        (ColumnType::Json, Warehouse::Snowflake) => format!("PARSE_JSON({column})"),
        (ColumnType::Json | ColumnType::Uuid, _) => {
            format!(
                "CAST({column} AS {})",
                warehouse_type(column_type, warehouse).sql
            )
        }
        _ => column,
    }
}

/// A live column name, quoted exactly as the warehouse reports it.
pub(crate) fn quote_live(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
