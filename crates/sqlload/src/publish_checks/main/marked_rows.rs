//! How many rows a key scan's commit marks deleted, within its limit.

use crate::adapters::contract::models::{MarkWrite, WriteMode, WriteStatementsRequest};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::loader::errors::LoaderError;
use crate::number_text::main::thousands::thousands;
use crate::publish_checks::_helpers::failures::single_count;
use crate::publish_checks::constants::PERCENT;
use crate::publish_checks::models::CheckedCommit;

/// The rows of `commit`'s table the key scan `mark` (its staged rows every key the source holds) marks deleted; fails the commit, marking none, when they are more than `mark.max_drop` percent of the rows it may mark from, as from a source read that came back empty or narrowed.
pub async fn marked_rows(commit: &CheckedCommit<'_>, mark: &MarkWrite) -> Result<u64, LoaderError> {
    let mode = WriteMode::MarkMissing(mark.clone());
    let request = WriteStatementsRequest {
        table: commit.table,
        columns: commit.columns,
        mode: &mode,
        hashed: &[],
        built: None,
    };
    let [marked, unmarked] =
        sql_adapter(commit.warehouse).render_marked_counts(&request, mark, commit.staging);
    let marked = single_count(commit, marked).await?;
    let unmarked = single_count(commit, unmarked).await?;
    if marked.saturating_mul(PERCENT) > unmarked.saturating_mul(u64::from(mark.max_drop)) {
        return Err(LoaderError::Invalid(format!(
            "checks.max_row_drop failed: the key scan would mark {} of the table's {} rows deleted, more than {}%, so it marks none. Fix: check that the source still lists every row (an empty view or a changed permission reads fewer keys), or raise checks.max_row_drop",
            thousands(marked),
            thousands(unmarked),
            mark.max_drop,
        )));
    }
    Ok(marked)
}
