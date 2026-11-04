//! The statements that move staged rows into the table.

use crate::column_types::main::warehouse_type::warehouse_type;
use crate::column_types::models::ColumnType;
use crate::schema_plan::constants::{LOAD_ID_COLUMN, LOADED_AT_COLUMN};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::write_sql::_helpers::identifiers::{from_staging, literal, qualified, quote};
use crate::write_sql::models::{TableName, WriteMode, WriteStatementsRequest};

/// One transaction that applies `request.mode` from `staging`, then drops `staging`.
pub fn write_statements(request: &WriteStatementsRequest<'_>, staging: &TableName) -> Vec<String> {
    let warehouse = request.warehouse;
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
        .map(|column| value_expression(request, column))
        .collect();
    let insert = format!(
        "INSERT INTO {target} ({}) SELECT {} FROM {source}",
        names.join(", "),
        values.join(", ")
    );
    let column_type = |name: &str| {
        request
            .columns
            .iter()
            .find(|column| column.name == name)
            .map_or(ColumnType::Text, |column| column.column_type.clone())
    };
    let delete = match request.mode {
        WriteMode::Append => None,
        WriteMode::Replace => Some(format!("DELETE FROM {target}")),
        WriteMode::Merge { unique_key } => {
            let key: Vec<String> = unique_key
                .iter()
                .map(|name| quote(name, warehouse))
                .collect();
            let key = key.join(", ");
            Some(format!(
                "DELETE FROM {target} WHERE ({key}) IN (SELECT {key} FROM {source})"
            ))
        }
        WriteMode::DeleteInsert {
            cursor_column,
            from,
        } => Some(match from {
            None => format!("DELETE FROM {target}"),
            Some(from) => format!(
                "DELETE FROM {target} WHERE {} >= CAST({} AS {})",
                quote(cursor_column, warehouse),
                literal(from),
                warehouse_type(&column_type(cursor_column), warehouse).sql
            ),
        }),
    };
    let mut statements = vec!["BEGIN".to_owned()];
    statements.extend(delete);
    statements.push(insert);
    statements.push("COMMIT".to_owned());
    statements.push(format!("DROP TABLE IF EXISTS {source}"));
    statements
}

fn value_expression(request: &WriteStatementsRequest<'_>, column: &DesiredColumn) -> String {
    let warehouse = request.warehouse;
    match (column.origin, column.name.as_str()) {
        (ColumnOrigin::Metadata, LOAD_ID_COLUMN) => literal(request.load_id),
        (ColumnOrigin::Metadata, LOADED_AT_COLUMN) => format!(
            "CAST({} AS {})",
            literal(request.loaded_at),
            warehouse_type(&ColumnType::TimestampTz, warehouse).sql
        ),
        _ => from_staging(&column.name, &column.column_type, warehouse),
    }
}
