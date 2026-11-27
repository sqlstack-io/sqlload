//! How many keys a commit's `newest_by` keeps out of the table.

use crate::adapters::contract::models::{WriteMode, WriteStatementsRequest};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::loader::errors::LoaderError;
use crate::loader::models::TableLayout;
use crate::publish_checks::_helpers::failures::single_count;
use crate::publish_checks::models::CheckedCommit;

/// The keys of `commit`'s staged rows whose newest row a merge by `mode` skips, as the table holds a newer one by `newest_by`; 0 without `newest_by`, or when the table doesn't exist yet.
pub async fn older_rows(commit: &CheckedCommit<'_>, mode: &WriteMode) -> Result<u64, LoaderError> {
    let request = WriteStatementsRequest {
        table: commit.table,
        columns: commit.columns,
        mode,
        hashed: &[],
        layout: &TableLayout::default(),
        built: None,
        spared_load: None,
    };
    let query = sql_adapter(commit.warehouse).render_older_rows(&request, commit.staging);
    match query.filter(|_| commit.exists) {
        Some(query) => single_count(commit, query).await,
        None => Ok(0),
    }
}
