//! The key values a commit's staged rows hold.

use crate::adapters::contract::models::CheckQuery;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::loader::errors::LoaderError;
use crate::publish_checks::models::CheckedCommit;

/// Up to `limit` distinct values of `key` that `commit`'s staged rows hold, in order, each as its columns' text (`NULL` for a NULL) joined by `, `: the values a `delete_insert` by `replace_key` replaces.
pub async fn staged_keys(
    commit: &CheckedCommit<'_>,
    (key, limit): (&[String], u64),
) -> Result<Vec<String>, LoaderError> {
    let query = sql_adapter(commit.warehouse)
        .render_check_query(commit.staging, CheckQuery::KeyValues(key, limit));
    text_values(commit.destination, query).await
}
