use crate::adapters::contract::types::SqlAdapter;
use crate::schema_plan::models::LiveColumn;

/// Rewrite the table with some columns cast to wider types, keeping column order (and grants, where the warehouse can).
pub(crate) fn rebuild_statement<A: SqlAdapter + ?Sized>(
    adapter: &A,
    target: &str,
    (live, rebuilt): (&[LiveColumn], &[(&str, &str)]),
) -> String {
    let select: Vec<String> = live
        .iter()
        .map(|column| rebuilt_column(adapter, column, rebuilt))
        .collect();
    adapter.render_rebuild_table(target, &select.join(", "))
}

/// A select-list entry keeping `column`, cast when it is one of `rebuilt`.
fn rebuilt_column<A: SqlAdapter + ?Sized>(
    adapter: &A,
    column: &LiveColumn,
    rebuilt: &[(&str, &str)],
) -> String {
    let name = adapter.render_exact_identifier(&column.name);
    match rebuilt.iter().find(|(rebuilt, _)| *rebuilt == column.name) {
        Some((_, sql_type)) => format!("CAST({name} AS {sql_type}) AS {name}"),
        None => name,
    }
}
