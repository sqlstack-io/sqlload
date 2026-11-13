//! The live rows a merge replaces whose ids were not derived from their key.

use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;

/// Under a merge, a query of the `row_id` of each row of `request.table` that shares its `unique_key` with a row of `staging` but holds another `row_id`: rows whose id was not derived from their key (as dlt's `delete-insert` merge gives them, random), so the rows hanging off them are found by the ids they hold. Keys compare as the merge compares them (equal, so an empty key matches nothing); `None` without a merge.
pub(in crate::adapters) fn rekeyed_row_ids<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    staging: &TableName,
    row_id: &str,
) -> Option<String> {
    let WriteMode::Merge { unique_key, .. } = request.mode else {
        return None;
    };
    let id = adapter.render_identifier(row_id);
    let keys: Vec<(String, String)> = unique_key
        .iter()
        .map(|key| {
            (
                adapter.render_identifier(key),
                staged_key(adapter, request, key),
            )
        })
        .collect();
    let staged: Vec<String> = keys
        .iter()
        .map(|(name, value)| format!("{value} AS {name}"))
        .collect();
    let on: Vec<String> = keys
        .iter()
        .map(|(name, _)| format!("__live.{name} = __staged.{name}"))
        .collect();
    Some(format!(
        "SELECT __live.{id} FROM {} AS __live INNER JOIN (SELECT {}, {id} FROM {}) AS __staged ON {} WHERE __live.{id} <> __staged.{id}",
        adapter.render_qualified_name(request.table),
        staged.join(", "),
        adapter.render_qualified_name(staging),
        on.join(" AND ")
    ))
}

/// The staged value of the key column `key`, cast as the merge casts it; the bare column when the request doesn't list it.
fn staged_key<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    key: &str,
) -> String {
    match request
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(key))
    {
        Some(column) => adapter.render_source_expression_cast(&column.name, &column.column_type),
        None => adapter.render_identifier(key),
    }
}
