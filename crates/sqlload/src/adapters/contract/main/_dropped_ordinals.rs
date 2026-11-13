//! The staged rows a deduping merge drops.

use crate::adapters::contract::main::_ranked_source::ranked_source;
use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// With `dedupe` on `request.mode`, a query of the `dedupe.ordinal`, as text, of each row of `staging` the merge drops (a repeated key's rows other than the one ranked first, as `ranked_source` ranks them for the merge itself); `None` without `dedupe`.
pub(in crate::adapters) fn dropped_ordinals<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    staging: &TableName,
) -> Option<String> {
    let WriteMode::Merge {
        dedupe: Some(dedupe),
        ..
    } = request.mode
    else {
        return None;
    };
    let mut columns = request.columns.to_vec();
    if !columns
        .iter()
        .any(|column| column.name.eq_ignore_ascii_case(&dedupe.ordinal))
    {
        columns.push(DesiredColumn {
            name: dedupe.ordinal.clone(),
            column_type: ColumnType::Int,
            origin: ColumnOrigin::Metadata,
        });
    }
    let values: Vec<String> = columns
        .iter()
        .map(|column| adapter.render_source_expression_cast(&column.name, &column.column_type))
        .collect();
    let widened = WriteStatementsRequest {
        table: request.table,
        columns: &columns,
        mode: request.mode,
        hashed: request.hashed,
        built: request.built,
    };
    let source = adapter.render_qualified_name(staging);
    let (kept, _) = ranked_source(adapter, &widened, (&source, &values))?;
    let ordinal = adapter.render_identifier(&dedupe.ordinal);
    Some(format!(
        "SELECT {} FROM {source} WHERE {ordinal} NOT IN (SELECT {ordinal} FROM {kept})",
        adapter.render_text_cast(&ordinal)
    ))
}
