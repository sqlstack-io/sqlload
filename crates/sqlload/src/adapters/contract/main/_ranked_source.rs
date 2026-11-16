//! A merge's staged rows kept to one per key.

use crate::adapters::contract::_helpers::promote::same_value;
use crate::adapters::contract::models::{WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;

/// With `dedupe` on `request.mode`, `(source, values)` as a subquery over `source` that keeps, of the rows sharing a `unique_key` (NULLs included, which the window groups together as the merge matches them), the one ranked first, and with `newest_by` only if the table holds no newer row of its key (with `older`, only if it does); and the column names it selects. `values` are `request.columns`' expressions, in order. Keys and the `newest_by` columns are compared as converted to the table's types, as the merge compares them. `None` without `dedupe`.
pub(in crate::adapters) fn ranked_source<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    (source, values): (&str, &[String]),
    older: bool,
) -> Option<(String, Vec<String>)> {
    let WriteMode::Merge {
        unique_key,
        dedupe: Some(dedupe),
        ..
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
        .newest_by
        .iter()
        .map(|column| format!("{} DESC NULLS LAST", value_of(column)))
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
    let ranked = format!(
        "(SELECT {}, ROW_NUMBER() OVER (PARTITION BY {} ORDER BY {}) AS __sqlload_rank FROM {source}) AS __sqlload_ranked",
        selected.join(", "),
        partition.join(", "),
        order.join(", ")
    );
    if dedupe.newest_by.is_empty() {
        let kept = format!(
            "(SELECT {} FROM {ranked} WHERE __sqlload_rank = 1) AS __sqlload_kept",
            names.join(", ")
        );
        return Some((kept, names));
    }
    let matched = request.mode.matched_keys();
    let keys: Vec<(String, bool)> = unique_key
        .iter()
        .map(|key| {
            let nullable = !matched.contains(&(key.as_str(), false));
            (adapter.render_identifier(key), nullable)
        })
        .collect();
    let newer = newer_condition(
        adapter,
        &dedupe.newest_by,
        ("__sqlload_newer", "__sqlload_ranked"),
    );
    let kept = adapter.render_newest_kept(request.table, (&ranked, &names), (&keys, &newer), older);
    Some((kept, names))
}

/// Whether the row aliased `newer` is strictly newer than the row aliased `older` by `columns`, compared in order: the first column decides, and the next breaks a tie. A NULL loses to any value, and two NULLs tie. Written out rather than as a tuple comparison, which not every warehouse has.
fn newer_condition<A: SqlAdapter + ?Sized>(
    adapter: &A,
    columns: &[String],
    (newer, older): (&str, &str),
) -> String {
    let mut ties: Vec<String> = Vec::new();
    let mut alternatives: Vec<String> = Vec::new();
    for column in columns {
        let name = adapter.render_identifier(column);
        let (left, right) = (format!("{newer}.{name}"), format!("{older}.{name}"));
        let wins = format!("({left} IS NOT NULL AND ({right} IS NULL OR {left} > {right}))");
        alternatives.push(
            ties.iter()
                .cloned()
                .chain([wins])
                .collect::<Vec<_>>()
                .join(" AND "),
        );
        ties.push(same_value((left, right), true));
    }
    format!("({})", alternatives.join(" OR "))
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
