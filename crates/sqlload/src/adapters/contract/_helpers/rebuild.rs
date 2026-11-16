use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::LiveColumn;

/// Rewrite the table with some columns cast to wider types, keeping column order (and grants, where the warehouse can). Each column is declared with its type, unless a kept column has a native type, such as BigQuery's STRUCT or RANGE, known only by a name that can't declare it again; such a table is rebuilt with the types its query gives.
pub(crate) fn rebuild_statement<A: SqlAdapter + ?Sized>(
    adapter: &A,
    target: &str,
    (live, rebuilt): (&[LiveColumn], &[(&str, &str)]),
) -> String {
    let kept = |column: &&LiveColumn| !rebuilt.iter().any(|(name, _)| *name == column.name);
    let typed = !live
        .iter()
        .filter(kept)
        .any(|column| matches!(column.column_type, ColumnType::Native(_)));
    let columns: Vec<String> = match typed {
        false => Vec::new(),
        true => live
            .iter()
            .map(|column| declared_column(adapter, column, rebuilt))
            .collect(),
    };
    let select: Vec<String> = live
        .iter()
        .map(|column| rebuilt_column(adapter, column, (rebuilt, typed)))
        .collect();
    adapter.render_rebuild_table(target, (&select.join(", "), &columns.join(", ")))
}

/// `column` as the rebuilt table declares it, `name TYPE`, with its type from `rebuilt` when it is one of them.
fn declared_column<A: SqlAdapter + ?Sized>(
    adapter: &A,
    column: &LiveColumn,
    rebuilt: &[(&str, &str)],
) -> String {
    let sql_type = match rebuilt.iter().find(|(name, _)| *name == column.name) {
        Some((_, sql_type)) => (*sql_type).to_owned(),
        None => adapter.render_loader_logical_type(&column.column_type).sql,
    };
    format!(
        "{} {sql_type}",
        adapter.render_exact_identifier(&column.name)
    )
}

/// A select-list entry keeping `column`, cast when it is one of `rebuilt`, or as the dialect needs when the rebuild declares each column's type.
fn rebuilt_column<A: SqlAdapter + ?Sized>(
    adapter: &A,
    column: &LiveColumn,
    (rebuilt, typed): (&[(&str, &str)], bool),
) -> String {
    let name = adapter.render_exact_identifier(&column.name);
    match rebuilt.iter().find(|(rebuilt, _)| *rebuilt == column.name) {
        Some((_, sql_type)) => format!("CAST({name} AS {sql_type}) AS {name}"),
        None if typed => adapter.render_typed_rebuild_column(&name, &column.column_type),
        None => name,
    }
}
