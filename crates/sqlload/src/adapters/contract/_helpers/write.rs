use crate::adapters::contract::_helpers::promote::key_join;
use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;

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
        WriteMode::Append => Vec::new(),
        WriteMode::DeleteInsert {
            cursor_column,
            replace_key,
            ..
        } => {
            let range = match (cursor_column, adapter.render_replaced_rows(request)) {
                (None, _) => None,
                (Some(_), Some(condition)) => {
                    Some(format!("DELETE FROM {target} WHERE {condition}"))
                }
                (Some(_), None) => Some(format!("DELETE FROM {target}")),
            };
            let keyed = (!replace_key.is_empty()).then(|| {
                let (keys, join) = replaced_keys(adapter, request, &values, replace_key);
                format!(
                    "DELETE FROM {target} AS __target USING (SELECT DISTINCT {keys} FROM {source}) AS __source WHERE {join}"
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
