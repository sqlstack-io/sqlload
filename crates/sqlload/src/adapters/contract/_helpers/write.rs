use crate::adapters::contract::_helpers::changes::change_statements;
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
    let column_type = |name: &str| {
        request
            .columns
            .iter()
            .find(|column| column.name == name)
            .map_or(ColumnType::Text, |column| column.column_type.clone())
    };
    let delete = match request.mode {
        WriteMode::Recreate => {
            return adapter.render_replace_table_from_relation(request, &source, &values);
        }
        WriteMode::Merge { unique_key } => {
            return vec![adapter.render_merge(request, unique_key, &source, &values)];
        }
        WriteMode::Replace => return adapter.render_replace(request, staging, &insert_from),
        WriteMode::Changes(change) => return change_statements(adapter, request, change, staging),
        WriteMode::Append => None,
        WriteMode::DeleteInsert {
            cursor_column,
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
                "DELETE FROM {target} WHERE {} IN ({})",
                adapter.render_identifier(cursor_column),
                listed.join(", ")
            ))
        }
        WriteMode::DeleteInsert {
            cursor_column,
            from,
            to,
            ..
        } => {
            let sql_type = adapter
                .render_loader_logical_type(&column_type(cursor_column))
                .sql;
            let column = adapter.render_identifier(cursor_column);
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
            Some(match bounds.is_empty() {
                true => format!("DELETE FROM {target}"),
                false => format!("DELETE FROM {target} WHERE {}", bounds.join(" AND ")),
            })
        }
    };
    let mut statements = vec!["BEGIN".to_owned()];
    statements.extend(delete);
    statements.push(insert);
    statements.push("COMMIT".to_owned());
    statements
}
