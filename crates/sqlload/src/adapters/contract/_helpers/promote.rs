//! `merge` and `replace`, ported from sqlbuild's adapters (`render_merge`, staged promotion and `render_swap`).

use crate::adapters::contract::_helpers::identifiers::with_suffix;
use crate::adapters::contract::constants::SWAP_SUFFIX;
use crate::adapters::contract::main::_ranked_source::ranked_source;
use crate::adapters::contract::models::{TableName, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;

/// One `MERGE`: key matches update every other column, the rest are inserted (sqlbuild `render_merge`); the staged rows are first kept to one per key when the mode dedupes.
pub(crate) fn merge_statement<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    unique_key: &[String],
    (source, values): (&str, &[String]),
) -> String {
    let target = adapter.render_qualified_name(request.table);
    let ranked = ranked_source(adapter, request, (source, values));
    let (source, values) = ranked
        .as_ref()
        .map_or((source, values), |(source, names)| (source.as_str(), names.as_slice()));
    let names: Vec<String> = request
        .columns
        .iter()
        .map(|column| adapter.render_identifier(&column.name))
        .collect();
    let keys: Vec<String> = unique_key
        .iter()
        .map(|key| adapter.render_identifier(key))
        .collect();
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

/// The old table dropped and the build table renamed in its place in one transaction (a table with an index cannot be renamed on DuckDB), then `indexes` re-created.
pub(crate) fn drop_and_rename<A: SqlAdapter + ?Sized>(
    adapter: &A,
    target: &TableName,
    build: &TableName,
    indexes: &[String],
) -> Vec<String> {
    let (old, new) = (
        adapter.render_qualified_name(target),
        adapter.render_qualified_name(build),
    );
    let mut statements = vec![
        "BEGIN".to_owned(),
        format!("DROP TABLE IF EXISTS {old}"),
        format!(
            "ALTER TABLE {new} RENAME TO {}",
            adapter.render_identifier(&target.table)
        ),
    ];
    statements.extend_from_slice(indexes);
    statements.push("COMMIT".to_owned());
    statements
}

/// `replace` from rows that arrived as text (streaming, `INSERT`): build a copy beside the table, then `SWAP WITH` it (sqlbuild `ATOMIC_SWAP`); `LIKE ... COPY GRANTS` keeps grants and constraints.
pub(crate) fn swap_statements<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    staging: &TableName,
    insert_from: &str,
) -> Vec<String> {
    let target = adapter.render_qualified_name(request.table);
    let swap = adapter.render_qualified_name(&with_suffix(staging, SWAP_SUFFIX));
    vec![
        format!("CREATE OR REPLACE TABLE {swap} LIKE {target} COPY GRANTS"),
        format!("INSERT INTO {swap} {insert_from}"),
        format!("ALTER TABLE {target} SWAP WITH {swap}"),
        format!("DROP TABLE IF EXISTS {swap}"),
    ]
}
