use crate::adapters::contract::_helpers::promote::key_join;
use crate::adapters::contract::models::{
    MarkWrite, TableName, TombstoneWrite, WriteMode, WriteStatementsRequest,
};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::load_naming::constants::SQLLOAD_LOADED_AT;
use crate::schema_plan::constants::CDC_DELETED_AT_COLUMN;

/// Atomic statements applying `request.mode` from `staging`, which holds every column, the `_sqlload_*` metadata included; the caller drops `staging` and its swap table.
pub(crate) fn write_statements<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    staging: &TableName,
) -> Vec<String> {
    if let Some(indexes) = request.built {
        return adapter.render_swap(request.table, staging, indexes);
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
                true => format!("SHA2({value}, 256)"),
                false => value,
            }
        })
        .collect();
    let insert_from = format!(
        "({}) SELECT {} FROM {source}",
        names.join(", "),
        values.join(", ")
    );
    let insert = format!("INSERT INTO {target} {insert_from}");
    let delete = match request.mode {
        WriteMode::Recreate => {
            return adapter.render_replace_table_from_relation(request, &source, &values);
        }
        WriteMode::Merge { unique_key, .. } => {
            return vec![adapter.render_merge(request, unique_key, &source, &values)];
        }
        WriteMode::Replace => return adapter.render_replace(request, staging, &insert_from),
        WriteMode::Changes(change) => return adapter.render_changes(request, change, staging),
        WriteMode::MarkMissing(mark) => {
            return vec![adapter.render_mark(request, mark, staging)];
        }
        WriteMode::Append => Vec::new(),
        WriteMode::DeleteInsert {
            cursor_column,
            replace_key,
            ..
        } => {
            let spared = |table: &str| {
                request.spared_load.map(|(column, load_id)| {
                    format!(
                        "{table}{} IS DISTINCT FROM {}",
                        adapter.render_identifier(column),
                        adapter.render_loader_value_literal(load_id)
                    )
                })
            };
            let range = match (cursor_column, adapter.render_replaced_rows(request)) {
                (None, _) => None,
                (Some(_), condition) => {
                    let conditions: Vec<String> = condition.into_iter().chain(spared("")).collect();
                    Some(match conditions.is_empty() {
                        true => format!("DELETE FROM {target}"),
                        false => format!("DELETE FROM {target} WHERE {}", conditions.join(" AND ")),
                    })
                }
            };
            let keyed = (!replace_key.is_empty()).then(|| {
                let (keys, join) = replaced_keys(adapter, request, &values, replace_key);
                let spared = spared("__target.")
                    .map(|condition| format!(" AND {condition}"))
                    .unwrap_or_default();
                format!(
                    "DELETE FROM {target} AS __target USING (SELECT DISTINCT {keys} FROM {source}) AS __source WHERE {join}{spared}"
                )
            });
            range.into_iter().chain(keyed).collect()
        }
    };
    let mut statements = vec!["BEGIN".to_owned()];
    statements.extend(delete);
    statements.push(insert);
    statements.push("COMMIT".to_owned());
    statements
}

/// The one statement of a key scan (`mark`): the table's rows [it marks](marked_rows) get `_sqlload_deleted_at` when it soft-deletes, else are removed.
pub(crate) fn mark_statement<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    mark: &MarkWrite,
    staging: &TableName,
) -> String {
    let (table, condition) = marked_rows(adapter, request, mark, staging);
    match mark.soft {
        true => format!(
            "UPDATE {table} SET {} = {} WHERE {condition}",
            adapter.render_identifier(CDC_DELETED_AT_COLUMN),
            timestamp(adapter, &mark.before)
        ),
        false => format!("DELETE FROM {table} WHERE {condition}"),
    }
}

/// Two queries, each returning one count as text: the table's rows a key scan (`mark`) [would mark](marked_rows), then those it may mark from (the rows not marked yet when it soft-deletes, else every row).
pub(crate) fn marked_counts<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    mark: &MarkWrite,
    staging: &TableName,
) -> [String; 2] {
    let (table, condition) = marked_rows(adapter, request, mark, staging);
    let count = adapter.render_text_cast("COUNT(*)");
    let unmarked = match mark.soft {
        true => format!(
            " WHERE {} IS NULL",
            adapter.render_identifier(CDC_DELETED_AT_COLUMN)
        ),
        false => String::new(),
    };
    [
        format!("SELECT {count} FROM {table} WHERE {condition}"),
        format!(
            "SELECT {count} FROM {}{unmarked}",
            adapter.render_qualified_name(request.table)
        ),
    ]
}

/// The table aliased `__target`, and the condition choosing the rows of it a key scan (`mark`) marks deleted: loaded before the scan's read started, not marked yet when soft-deleting, and holding a key none of `staging`'s rows holds.
fn marked_rows<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    mark: &MarkWrite,
    staging: &TableName,
) -> (String, String) {
    let source = adapter.render_qualified_name(staging);
    let values: Vec<String> = request
        .columns
        .iter()
        .map(|column| adapter.render_source_expression_cast(&column.name, &column.column_type))
        .collect();
    let (keys, join) = replaced_keys(adapter, request, &values, &mark.unique_key);
    let column = |name: &str| format!("__target.{}", adapter.render_identifier(name));
    let mut conditions = vec![format!(
        "{} < {}",
        column(SQLLOAD_LOADED_AT),
        timestamp(adapter, &mark.before)
    )];
    if mark.soft {
        conditions.push(format!("{} IS NULL", column(CDC_DELETED_AT_COLUMN)));
    }
    conditions.push(format!(
        "NOT EXISTS (SELECT 1 FROM (SELECT DISTINCT {keys} FROM {source}) AS __source WHERE {join})"
    ));
    (
        format!(
            "{} AS __target",
            adapter.render_qualified_name(request.table)
        ),
        conditions.join(" AND "),
    )
}

/// A query counting, as text, the rows of `table` its tombstones (`tombstones`) mark, then the statement marking them: rows not marked yet when soft-deleting, whose key a tombstone holds, deleted at or after the row's cursor (or of a row without one). A soft mark sets the latest tombstone of the row's key through a grouped join rather than a correlated subquery, which Snowflake can't always evaluate, on exactly the rows the count counts. Run again, both find nothing more.
pub(crate) fn tombstone_statements<A: SqlAdapter + ?Sized>(
    adapter: &A,
    table: &TableName,
    tombstones: &TombstoneWrite,
) -> [String; 2] {
    let name = |alias: &str, column: &str| format!("{alias}.{}", adapter.render_identifier(column));
    let keys = |row: &str, other: &str| -> String {
        tombstones
            .unique_key
            .iter()
            .map(|key| {
                let nullable = !tombstones.not_null_keys.contains(key);
                adapter.render_key_equality((&name(row, key), &name(other, key)), nullable)
            })
            .collect::<Vec<String>>()
            .join(" AND ")
    };
    let cursor_of = |row: &str| -> String {
        let cursors: Vec<String> = tombstones
            .cursor
            .iter()
            .map(|column| name(row, column))
            .collect();
        match cursors.as_slice() {
            [one] => one.clone(),
            _ => format!("COALESCE({})", cursors.join(", ")),
        }
    };
    let applies = |row: &str| -> String {
        let cursor = cursor_of(row);
        let deleted_at = name("__tombstone", &tombstones.deleted_at);
        format!(
            "{} AND ({cursor} IS NULL OR {deleted_at} >= {cursor})",
            keys(row, "__tombstone")
        )
    };
    let tombstone_table = adapter.render_qualified_name(&tombstones.tombstones);
    let unmarked = |row: &str| format!("{} IS NULL", name(row, CDC_DELETED_AT_COLUMN));
    let mut conditions: Vec<String> = Vec::new();
    if tombstones.soft {
        conditions.push(unmarked("__target"));
    }
    conditions.push(format!(
        "EXISTS (SELECT 1 FROM {tombstone_table} AS __tombstone WHERE {})",
        applies("__target")
    ));
    let qualified = adapter.render_qualified_name(table);
    let target = format!("{qualified} AS __target");
    let condition = conditions.join(" AND ");
    let mark = match tombstones.soft {
        true => {
            let row_keys: Vec<String> = tombstones
                .unique_key
                .iter()
                .map(|key| name("__row", key))
                .collect();
            let selected: Vec<String> = tombstones
                .unique_key
                .iter()
                .map(|key| {
                    format!(
                        "{} AS {}",
                        name("__row", key),
                        adapter.render_identifier(key)
                    )
                })
                .collect();
            let column = adapter.render_identifier(CDC_DELETED_AT_COLUMN);
            let deleted_at = name("__tombstone", &tombstones.deleted_at);
            let marks = |latest: &str| {
                format!(
                    "(SELECT {}, CAST(MAX({deleted_at}) AS {}) AS {column}{latest} FROM {qualified} AS __row JOIN {tombstone_table} AS __tombstone ON {} WHERE {} GROUP BY {}) AS __marks",
                    selected.join(", "),
                    adapter
                        .render_loader_logical_type(&ColumnType::TimestampTz)
                        .sql,
                    applies("__row"),
                    unmarked("__row"),
                    row_keys.join(", "),
                )
            };
            let cursor = cursor_of("__target");
            let matched = format!(
                "{} AND ({cursor} IS NULL OR __marks.__sqlload_latest >= {cursor})",
                unmarked("__target")
            );
            adapter.render_soft_tombstones(
                (&target, &column),
                (
                    (
                        &marks(""),
                        &marks(&format!(", MAX({deleted_at}) AS __sqlload_latest")),
                    ),
                    &keys("__target", "__marks"),
                ),
                (&condition, &matched),
            )
        }
        false => format!("DELETE FROM {target} WHERE {condition}"),
    };
    let count = adapter.render_text_cast("COUNT(*)");
    [
        format!("SELECT {count} FROM {target} WHERE {condition}"),
        mark,
    ]
}

/// RFC 3339 `text` as a timestamp with time zone.
fn timestamp<A: SqlAdapter + ?Sized>(adapter: &A, text: &str) -> String {
    format!(
        "CAST({} AS {})",
        adapter.render_loader_value_literal(text),
        adapter
            .render_loader_logical_type(&ColumnType::TimestampTz)
            .sql
    )
}

/// The staged values of the `replace_key` columns as a `SELECT` lists them (`value AS name`, from `values`, the insert's expressions in `request.columns`' order, so they convert and hash as it does; the bare column when the request doesn't list one), and the join matching them on `__source` to the table's rows on `__target`, NULL-safely where a column's staged values may be NULL.
pub(crate) fn replaced_keys<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    values: &[String],
    replace_key: &[String],
) -> (String, String) {
    let keys: Vec<String> = replace_key
        .iter()
        .map(|key| {
            let name = adapter.render_identifier(key);
            let value = staged_value(request, values, key).unwrap_or(&name);
            format!("{value} AS {name}")
        })
        .collect();
    let join = key_join(adapter, request.mode, replace_key, ("__target", "__source"));
    (keys.join(", "), join)
}

/// The expression of `values` (in `request.columns`' order) for column `key`; `None` when the request doesn't list it.
fn staged_value<'a>(
    request: &WriteStatementsRequest<'_>,
    values: &'a [String],
    key: &str,
) -> Option<&'a String> {
    let index = request
        .columns
        .iter()
        .position(|column| column.name == key)?;
    values.get(index)
}

/// Under `delete_insert` with a cursor, the condition choosing the rows of `request.table` its commit replaces by it: those whose cursor is one of its values, or within its window; `None` when that is every row (no bounds), and without a cursor or a `delete_insert` (the rows a `replace_key` replaces are [`replaced_keys`]'s).
pub(crate) fn replaced_rows<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
) -> Option<String> {
    let column_type = |name: &str| {
        request
            .columns
            .iter()
            .find(|column| column.name == name)
            .map_or(ColumnType::Text, |column| column.column_type.clone())
    };
    let compared = |name: &str| {
        let column = adapter.render_identifier(name);
        match column_type(name) {
            ColumnType::Text | ColumnType::Varchar(_) => adapter.render_binary_text(&column),
            _ => column,
        }
    };
    match request.mode {
        WriteMode::DeleteInsert {
            cursor_column: Some(cursor_column),
            values,
            ..
        } if !values.is_empty() => {
            let sql_type = adapter
                .render_loader_logical_type(&column_type(cursor_column))
                .sql;
            let listed: Vec<String> = values
                .iter()
                .map(|value| {
                    format!(
                        "CAST({} AS {sql_type})",
                        adapter.render_loader_value_literal(value)
                    )
                })
                .collect();
            Some(format!(
                "{} IN ({})",
                compared(cursor_column),
                listed.join(", ")
            ))
        }
        WriteMode::DeleteInsert {
            cursor_column: Some(cursor_column),
            from,
            to,
            ..
        } => {
            let sql_type = adapter
                .render_loader_logical_type(&column_type(cursor_column))
                .sql;
            let column = compared(cursor_column);
            let bounds: Vec<String> = [(from, ">="), (to, "<")]
                .into_iter()
                .filter_map(|(bound, operator)| {
                    let bound = bound.as_ref()?;
                    Some(format!(
                        "{column} {operator} CAST({} AS {sql_type})",
                        adapter.render_loader_value_literal(bound)
                    ))
                })
                .collect();
            (!bounds.is_empty()).then(|| bounds.join(" AND "))
        }
        _ => None,
    }
}

/// `staging` as a merge reads it: its name, and `request.columns`' expressions over it, each converted to its column's type.
pub(crate) fn staged_values<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    staging: &TableName,
) -> (String, Vec<String>) {
    let values: Vec<String> = request
        .columns
        .iter()
        .map(|column| adapter.render_source_expression_cast(&column.name, &column.column_type))
        .collect();
    (adapter.render_qualified_name(staging), values)
}
