//! The keys a merge's `newest_by` keeps out of the table.

use crate::adapters::contract::_helpers::write::staged_values;
use crate::adapters::contract::main::_ranked_source::ranked_source;
use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;

/// Under `newest_by`, a query counting, as text, the keys of `staging` whose newest row the merge of `request` skips, as the table holds a newer one; `None` without `newest_by`.
pub(in crate::adapters) fn older_rows<A: SqlAdapter + ?Sized>(
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
    if dedupe.newest_by.is_empty() {
        return None;
    }
    let (source, values) = staged_values(adapter, request, staging);
    let (older, _) = ranked_source(adapter, request, (&source, &values), true)?;
    Some(format!(
        "SELECT {} FROM {older}",
        adapter.render_text_cast("COUNT(*)")
    ))
}
