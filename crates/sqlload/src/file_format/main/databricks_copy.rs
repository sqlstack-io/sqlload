//! The Databricks `COPY INTO` statement that loads staged Parquet files into a table.

use crate::config::models::ColumnConfig;

/// `COPY INTO table FROM (SELECT ... FROM 'location') FILEFORMAT = PARQUET FILES = (...)`: each envelope column cast to its type, JSON text parsed for VARIANT, MAP, STRUCT and ARRAY columns. Databricks skips files it loaded before, so a retried load adds no rows.
pub fn databricks_copy(
    table: &str,
    columns: &[ColumnConfig],
    location: &str,
    files: &[&str],
) -> String {
    let select: Vec<String> = columns.iter().map(column_expression).collect();
    let names: Vec<String> = files.iter().map(|file| literal(file)).collect();
    format!(
        "COPY INTO {table} FROM (SELECT {} FROM {}) FILEFORMAT = PARQUET FILES = ({})",
        select.join(", "),
        literal(location),
        names.join(", ")
    )
}

/// The column read from its Parquet text: VARIANT parsed, MAP, STRUCT and ARRAY read as JSON, every other type cast.
fn column_expression(column: &ColumnConfig) -> String {
    let name = format!("`{}`", column.name.replace('`', "``"));
    let sql_type = column.sql_type.trim();
    let upper = sql_type.to_ascii_uppercase();
    match upper.split(['(', '<', ' ']).next().unwrap_or_default() {
        "VARIANT" => format!("parse_json({name}) AS {name}"),
        "MAP" | "STRUCT" | "ARRAY" => format!("from_json({name}, {}) AS {name}", literal(sql_type)),
        _ => format!("CAST({name} AS {sql_type}) AS {name}"),
    }
}

fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
}
