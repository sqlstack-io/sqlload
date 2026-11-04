//! `merge` and Snowflake's `replace`, ported from sqlbuild's adapters (`render_merge`, `ATOMIC_SWAP` / `render_swap`).

use crate::write_sql::_helpers::identifiers::{qualified, quote, with_suffix};
use crate::write_sql::constants::SWAP_SUFFIX;
use crate::write_sql::models::{TableName, WriteStatementsRequest};

/// The table a `replace` builds before swapping it in, named after the load's staging table.
pub(crate) fn swap_table_name(staging: &TableName) -> TableName {
    with_suffix(staging, SWAP_SUFFIX)
}

/// One `MERGE`: key matches update every other column, the rest are inserted (sqlbuild `render_merge`).
pub(crate) fn merge_statement(
    request: &WriteStatementsRequest<'_>,
    unique_key: &[String],
    source: &str,
    values: &[String],
) -> String {
    let warehouse = request.warehouse;
    let target = qualified(request.table, warehouse);
    let names: Vec<String> = request
        .columns
        .iter()
        .map(|column| quote(&column.name, warehouse))
        .collect();
    let keys: Vec<String> = unique_key.iter().map(|key| quote(key, warehouse)).collect();
    let selected: Vec<String> = values
        .iter()
        .zip(&names)
        .map(|(value, name)| format!("{value} AS {name}"))
        .collect();
    let join: Vec<String> = keys
        .iter()
        .map(|key| format!("__target.{key} = __source.{key}"))
        .collect();
    let updates: Vec<String> = names
        .iter()
        .filter(|name| !keys.contains(name))
        .map(|name| format!("{name} = __source.{name}"))
        .collect();
    let inserted: Vec<String> = names
        .iter()
        .map(|name| format!("__source.{name}"))
        .collect();
    let matched = match updates.is_empty() {
        true => String::new(),
        false => format!("WHEN MATCHED THEN UPDATE SET {} ", updates.join(", ")),
    };
    format!(
        "MERGE INTO {target} AS __target USING (SELECT {} FROM {source}) AS __source ON {} {matched}WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
        selected.join(", "),
        join.join(" AND "),
        names.join(", "),
        inserted.join(", ")
    )
}

/// Snowflake `replace`: build a copy beside the table, then `SWAP WITH` it (sqlbuild `ATOMIC_SWAP`); `LIKE ... COPY GRANTS` keeps grants and constraints.
pub(crate) fn swap_statements(
    request: &WriteStatementsRequest<'_>,
    staging: &TableName,
    insert_from: &str,
) -> Vec<String> {
    let warehouse = request.warehouse;
    let target = qualified(request.table, warehouse);
    let swap = qualified(&swap_table_name(staging), warehouse);
    vec![
        format!("CREATE OR REPLACE TABLE {swap} LIKE {target} COPY GRANTS"),
        format!("INSERT INTO {swap} {insert_from}"),
        format!("ALTER TABLE {target} SWAP WITH {swap}"),
        format!("DROP TABLE IF EXISTS {swap}"),
    ]
}
