//! A merge's staged rows kept to one per key.

use crate::adapters::contract::models::{WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;

/// With `dedupe` on `request.mode`, `(source, values)` as a subquery over `source` that keeps, of the rows sharing a non-NULL `unique_key`, the one ranked first, and the column names it selects; `values` are `request.columns`' expressions, in order. Keys and the sort column are ranked as converted to the table's types, as the merge compares them. `None` without `dedupe`.
pub(in crate::adapters) fn ranked_source<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    (source, values): (&str, &[String]),
) -> Option<(String, Vec<String>)> {
    let WriteMode::Merge {
        unique_key,
        dedupe: Some(dedupe),
    } = request.mode
    else {
        return None;
    };
    let names: Vec<String> = request
        .columns
        .iter()
        .map(|column| adapter.render_identifier(&column.name))
        .collect();
    let value_of = |name: &str| {
        request
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(name))
            .and_then(|index| values.get(index).cloned())
            .unwrap_or_else(|| adapter.render_identifier(name))
    };
    let partition: Vec<String> = unique_key
        .iter()
        .map(|key| partition_key(adapter, request, (key, value_of(key))))
        .collect();
    let order: Vec<String> = dedupe
        .sort
        .iter()
        .map(|(column, highest)| {
            let direction = match highest {
                true => "DESC",
                false => "ASC",
            };
            format!("{} {direction} NULLS LAST", value_of(column))
        })
        .chain([format!(
            "{} DESC",
            adapter.render_identifier(&dedupe.ordinal)
        )])
        .collect();
    let selected: Vec<String> = values
        .iter()
        .zip(&names)
        .map(|(value, name)| format!("{value} AS {name}"))
        .collect();
    let kept: Vec<String> = std::iter::once("__sqlload_rank = 1".to_owned())
        .chain(
            unique_key
                .iter()
                .map(|key| format!("{} IS NULL", adapter.render_identifier(key))),
        )
        .collect();
    Some((
        format!(
            "(SELECT {} FROM (SELECT {}, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}) AS __sqlload_rank FROM {source}) AS __sqlload_ranked WHERE {}) AS __sqlload_kept",
            names.join(", "),
            selected.join(", "),
            partition.join(", "),
            order.join(", "),
            kept.join(" OR ")
        ),
        names,
    ))
}

/// Key column `key`'s `value` as the window partitions by it, in the warehouse's terms for the key's type.
fn partition_key<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    (key, value): (&str, String),
) -> String {
    match request
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(key))
    {
        Some(column) => adapter.render_partition_key(&value, &column.column_type),
        None => value,
    }
}
