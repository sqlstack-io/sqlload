//! BigQuery's writes: each one statement, which commits on its own.

use crate::adapters::bigquery::_helpers::dialect::layout::{
    partition_column, partition_filter, pruned,
};
use crate::adapters::contract::main::_keyed_merge::keyed_merge;
use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;

/// The statements applying `request.mode` from `staging`, each atomic on its own. A `delete_insert` is one `MERGE` that inserts every staged row and deletes the table's rows in the range, so it keeps `DELETE` then `INSERT`'s meaning (rows with a NULL cursor included) in one commit, and by a `replace_key` [one that also deletes the rows of the staged key values](keyed_merge); a `replace` is the same `MERGE` over every row, since BigQuery has no `INSERT OVERWRITE`.
pub(crate) fn write_statements<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    staging: &TableName,
) -> Vec<String> {
    if let Some(indexes) = request.built {
        return adapter.render_swap(request.table, staging, indexes, request.layout);
    }
    let target = adapter.render_qualified_name(request.table);
    let source = adapter.render_qualified_name(staging);
    let names: Vec<String> = request
        .columns
        .iter()
        .map(|column| adapter.render_identifier(&column.name))
        .collect();
    let values: Vec<String> = request
        .columns
        .iter()
        .map(|column| {
            let value = adapter.render_source_expression_cast(&column.name, &column.column_type);
            match request.hashed.contains(&column.name) {
                true => format!("TO_HEX(SHA256({value}))"),
                false => value,
            }
        })
        .collect();
    match request.mode {
        WriteMode::Recreate => {
            adapter.render_replace_table_from_relation(request, &source, &values)
        }
        WriteMode::Merge { unique_key, .. } => {
            let merge = adapter.render_merge(request, unique_key, &source, &values);
            vec![prune(request, unique_key, (&source, &values), merge)]
        }
        WriteMode::Append => vec![format!(
            "INSERT INTO {target} ({}) SELECT {} FROM {source}",
            names.join(", "),
            values.join(", ")
        )],
        WriteMode::MarkMissing(mark) => vec![adapter.render_mark(request, mark, staging)],
        WriteMode::Changes(change) => adapter
            .render_changes(request, change, staging)
            .into_iter()
            .filter(|statement| !matches!(statement.as_str(), "BEGIN" | "COMMIT"))
            .collect(),
        WriteMode::Replace => vec![insert_and_delete(
            (&target, &source),
            (&names, &values),
            None,
        )],
        WriteMode::DeleteInsert {
            cursor_column,
            replace_key,
            ..
        } if !replace_key.is_empty() => {
            let merge = keyed_merge(
                adapter,
                request,
                (&source, &values),
                (
                    replace_key,
                    cursor_column
                        .as_ref()
                        .map(|_| range_condition(adapter, request)),
                ),
            );
            vec![prune(request, replace_key, (&source, &values), merge)]
        }
        WriteMode::DeleteInsert { .. } => vec![insert_and_delete(
            (&target, &source),
            (&names, &values),
            range_condition(adapter, request),
        )],
    }
}

/// `merge` kept to the partitions its staged rows fall in, where the table is partitioned by a column among `keys`; as it is otherwise.
fn prune(
    request: &WriteStatementsRequest<'_>,
    keys: &[String],
    (source, values): (&str, &[String]),
    merge: String,
) -> String {
    let staged = partition_column(request.layout).and_then(|partitioned| {
        request
            .columns
            .iter()
            .zip(values)
            .find(|(column, _)| column.name == partitioned)
            .map(|(_, value)| value.as_str())
    });
    match staged.and_then(|staged| partition_filter(request.layout, keys, (staged, source))) {
        Some(filter) => pruned(filter, merge),
        None => merge,
    }
}

/// One `MERGE` inserting every row of `source` into `target` and deleting the target rows it did not insert that meet `condition` (every one when `None`).
fn insert_and_delete(
    (target, source): (&str, &str),
    (names, values): (&[String], &[String]),
    condition: Option<String>,
) -> String {
    let selected: Vec<String> = values
        .iter()
        .zip(names)
        .map(|(value, name)| format!("{value} AS {name}"))
        .collect();
    let inserted: Vec<String> = names
        .iter()
        .map(|name| format!("__source.{name}"))
        .collect();
    let deleted = match condition {
        Some(condition) => format!("WHEN NOT MATCHED BY SOURCE AND {condition} THEN DELETE"),
        None => "WHEN NOT MATCHED BY SOURCE THEN DELETE".to_owned(),
    };
    format!(
        "MERGE INTO {target} AS __target USING (SELECT {} FROM {source}) AS __source ON FALSE WHEN NOT MATCHED THEN INSERT ({}) VALUES ({}) {deleted}",
        selected.join(", "),
        names.join(", "),
        inserted.join(", ")
    )
}

/// The table rows a `delete_insert` replaces by its cursor, on `__target`; `None` for every row, and without a cursor.
fn range_condition<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
) -> Option<String> {
    let WriteMode::DeleteInsert {
        cursor_column: Some(cursor_column),
        from,
        to,
        values,
        ..
    } = request.mode
    else {
        return None;
    };
    let cursor_type = request
        .columns
        .iter()
        .find(|column| column.name == *cursor_column)
        .map_or(ColumnType::Text, |column| column.column_type.clone());
    let sql_type = adapter.render_loader_logical_type(&cursor_type).sql;
    let cast = |text: &str| {
        format!(
            "CAST({} AS {sql_type})",
            adapter.render_loader_value_literal(text)
        )
    };
    let column = format!("__target.{}", adapter.render_identifier(cursor_column));
    if !values.is_empty() {
        let listed: Vec<String> = values.iter().map(|value| cast(value)).collect();
        return Some(format!("{column} IN ({})", listed.join(", ")));
    }
    let bounds: Vec<String> = [(from, ">="), (to, "<")]
        .into_iter()
        .filter_map(|(bound, operator)| {
            let bound = bound.as_ref()?;
            Some(format!("{column} {operator} {}", cast(bound)))
        })
        .collect();
    (!bounds.is_empty()).then(|| bounds.join(" AND "))
}
