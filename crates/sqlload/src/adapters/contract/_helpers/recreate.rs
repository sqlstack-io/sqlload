use crate::adapters::contract::models::WriteStatementsRequest;
use crate::adapters::contract::types::SqlAdapter;

/// Rebuild the table from `source` in one statement, each value cast to its column's type, so readers never see it empty.
pub(crate) fn recreate_statements<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    source: &str,
    values: &[String],
) -> Vec<String> {
    let target = adapter.render_qualified_name(request.table);
    let select: Vec<String> = request
        .columns
        .iter()
        .zip(values)
        .map(|(column, value)| {
            format!(
                "CAST({value} AS {}) AS {}",
                adapter.render_loader_logical_type(&column.column_type).sql,
                adapter.render_identifier(&column.name)
            )
        })
        .collect();
    vec![format!(
        "CREATE OR REPLACE TABLE {target} AS SELECT {} FROM {source}",
        select.join(", ")
    )]
}
