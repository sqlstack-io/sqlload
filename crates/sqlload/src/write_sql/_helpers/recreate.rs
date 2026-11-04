use crate::column_types::main::warehouse_type::warehouse_type;
use crate::column_types::models::Warehouse;
use crate::write_sql::_helpers::identifiers::{qualified, quote};
use crate::write_sql::models::WriteStatementsRequest;

/// Rebuild the table from `source` in one statement, so readers never see it empty; then drop `source`.
pub(crate) fn recreate_statements(
    request: &WriteStatementsRequest<'_>,
    source: &str,
    values: &[String],
) -> Vec<String> {
    let warehouse = request.warehouse;
    let target = qualified(request.table, warehouse);
    let typed: Vec<(String, String)> = request
        .columns
        .iter()
        .map(|column| {
            (
                quote(&column.name, warehouse),
                warehouse_type(&column.column_type, warehouse).sql,
            )
        })
        .collect();
    let create = match warehouse {
        Warehouse::Snowflake => {
            let definitions: Vec<String> = typed
                .iter()
                .map(|(name, sql_type)| format!("{name} {sql_type}"))
                .collect();
            format!(
                "CREATE OR REPLACE TABLE {target} ({}) COPY GRANTS AS SELECT {} FROM {source}",
                definitions.join(", "),
                values.join(", ")
            )
        }
        _ => {
            let select: Vec<String> = typed
                .iter()
                .zip(values)
                .map(|((name, sql_type), value)| format!("CAST({value} AS {sql_type}) AS {name}"))
                .collect();
            format!(
                "CREATE OR REPLACE TABLE {target} AS SELECT {} FROM {source}",
                select.join(", ")
            )
        }
    };
    vec![create, format!("DROP TABLE IF EXISTS {source}")]
}
