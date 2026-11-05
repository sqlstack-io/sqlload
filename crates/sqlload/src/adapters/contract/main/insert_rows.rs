//! Batched multi-row `INSERT` statements.

use crate::column_types::models::Warehouse;
use crate::schema_plan::models::DesiredColumn;
use crate::adapters::contract::_helpers::identifiers::{literal, qualified, quote, staging_type};
use crate::adapters::contract::constants::{MAX_INSERT_ROWS, MAX_INSERT_SQL_BYTES};
use crate::adapters::contract::models::TableName;

/// `INSERT` statements adding `rows` (each value as text, in `columns`' order) to staging table `table`, each at most 16,384 rows and about 900 KiB of SQL; the values are cast to the columns' types, because `VALUES` infers its own. A row too long for a statement still gets one of its own, which Snowflake refuses with its own message.
pub fn insert_rows(
    table: &TableName,
    columns: &[DesiredColumn],
    rows: &[Vec<Option<String>>],
    warehouse: Warehouse,
) -> Vec<String> {
    let names: Vec<String> = columns
        .iter()
        .map(|column| quote(&column.name, warehouse))
        .collect();
    let casts: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            format!(
                "CAST(column{} AS {})",
                index + 1,
                staging_type(&column.column_type, warehouse)
            )
        })
        .collect();
    let opening = format!(
        "INSERT INTO {} ({}) SELECT {} FROM VALUES ",
        qualified(table, warehouse),
        names.join(", "),
        casts.join(", ")
    );
    let mut statements: Vec<String> = Vec::new();
    let mut current: Vec<String> = Vec::new();
    let mut bytes = opening.len();
    for row in rows {
        let values: Vec<String> = row
            .iter()
            .map(|value| match value {
                Some(text) => literal(text, warehouse),
                None => "NULL".to_owned(),
            })
            .collect();
        let tuple = format!("({})", values.join(", "));
        let over = current.len() == MAX_INSERT_ROWS || bytes + tuple.len() > MAX_INSERT_SQL_BYTES;
        if over && !current.is_empty() {
            statements.push(format!("{opening}{}", current.join(", ")));
            current.clear();
            bytes = opening.len();
        }
        bytes += tuple.len() + 2;
        current.push(tuple);
    }
    if !current.is_empty() {
        statements.push(format!("{opening}{}", current.join(", ")));
    }
    statements
}
