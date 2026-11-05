use crate::adapters::contract::_helpers::identifiers::exact_identifier;
use crate::schema_plan::models::LiveColumn;

/// Rewrite the table with some columns cast to wider types, keeping column order and grants.
pub(crate) fn rebuild_statement(
    target: &str,
    live: &[LiveColumn],
    rebuilt: &[(&str, &str)],
) -> String {
    let select: Vec<String> = live
        .iter()
        .map(|column| rebuilt_column(column, rebuilt))
        .collect();
    format!(
        "CREATE OR REPLACE TABLE {target} COPY GRANTS AS SELECT {} FROM {target}",
        select.join(", ")
    )
}

/// A select-list entry keeping `column`, cast when it is one of `rebuilt`.
fn rebuilt_column(column: &LiveColumn, rebuilt: &[(&str, &str)]) -> String {
    let name = exact_identifier(&column.name);
    match rebuilt.iter().find(|(rebuilt, _)| *rebuilt == column.name) {
        Some((_, sql_type)) => format!("CAST({name} AS {sql_type}) AS {name}"),
        None => name,
    }
}
