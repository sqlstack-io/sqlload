use crate::column_types::main::warehouse_type::warehouse_type;
use crate::column_types::models::ColumnType;
use crate::adapters::contract::_helpers::identifiers::{from_staging, literal, qualified, quote};
use crate::adapters::contract::_helpers::promote::{merge_statement, promote_statements, swap_statements};
use crate::adapters::contract::_helpers::recreate::recreate_statements;
use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};

/// Atomic statements applying `request.mode` from `staging`, which holds every column, the `_sqlload_*` metadata included; the caller drops `staging` and its swap table.
pub(crate) fn write_statements(
    request: &WriteStatementsRequest<'_>,
    staging: &TableName,
) -> Vec<String> {
    let warehouse = request.warehouse;
    if let Some(indexes) = request.built {
        return promote_statements(request.table, staging, indexes, warehouse);
    }
    let target = qualified(request.table, warehouse);
    let source = qualified(staging, warehouse);
    let names: Vec<String> = request
        .columns
        .iter()
        .map(|column| quote(&column.name, warehouse))
        .collect();
    let values: Vec<String> = request
        .columns
        .iter()
        .map(|column| {
            let value = from_staging(&column.name, &column.column_type, warehouse);
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
        WriteMode::Recreate => return recreate_statements(request, &source, &values),
        WriteMode::Merge { unique_key } => {
            return vec![merge_statement(request, unique_key, &source, &values)];
        }
        WriteMode::Replace => return swap_statements(request, staging, &insert_from),
        WriteMode::Append => None,
        WriteMode::DeleteInsert {
            cursor_column,
            values,
            ..
        } if !values.is_empty() => {
            let sql_type = warehouse_type(&column_type(cursor_column), warehouse).sql;
            let listed: Vec<String> = values
                .iter()
                .map(|value| format!("CAST({} AS {sql_type})", literal(value, warehouse)))
                .collect();
            Some(format!(
                "DELETE FROM {target} WHERE {} IN ({})",
                quote(cursor_column, warehouse),
                listed.join(", ")
            ))
        }
        WriteMode::DeleteInsert {
            cursor_column,
            from,
            to,
            ..
        } => {
            let sql_type = warehouse_type(&column_type(cursor_column), warehouse).sql;
            let column = quote(cursor_column, warehouse);
            let bounds: Vec<String> = [(from, ">="), (to, "<")]
                .into_iter()
                .filter_map(|(bound, operator)| {
                    let bound = bound.as_ref()?;
                    Some(format!(
                        "{column} {operator} CAST({} AS {sql_type})",
                        literal(bound, warehouse)
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
