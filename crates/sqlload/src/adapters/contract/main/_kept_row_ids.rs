//! The rows of a merge's staging whose child rows it replaces.

use crate::adapters::contract::_helpers::write::staged_values;
use crate::adapters::contract::main::_ranked_source::ranked_source;
use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;

/// A query of the ids (`row_id`) of the rows of `staging` the merge of `request` writes, whose child tables' rows it replaces: every staged row's, or under `newest_by` only those of the rows it keeps, so a row the table holds a newer version of keeps its child rows.
pub(in crate::adapters) fn kept_row_ids<A: SqlAdapter + ?Sized>(
    adapter: &A,
    request: &WriteStatementsRequest<'_>,
    (staging, row_id): (&TableName, &str),
) -> String {
    let (source, values) = staged_values(adapter, request, staging);
    let newest = matches!(request.mode, WriteMode::Merge { dedupe: Some(dedupe), .. } if !dedupe.newest_by.is_empty());
    let kept = match newest {
        true => ranked_source(adapter, request, (&source, &values), false).map(|(kept, _)| kept),
        false => None,
    };
    format!(
        "SELECT {} FROM {}",
        adapter.render_identifier(row_id),
        kept.unwrap_or(source)
    )
}
