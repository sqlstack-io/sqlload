//! The files a commit's key values already come from.

use crate::adapters::contract::models::CheckQuery;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::loader::errors::LoaderError;
use crate::publish_checks::models::CheckedCommit;

/// The distinct values of `file_column` among the rows of `commit`'s table whose `key` values its staged rows hold, other than `file`, in order: the files a `delete_insert` by `replace_key` would delete rows of.
pub async fn key_sources(
    commit: &CheckedCommit<'_>,
    (key, file_column, file): (&[String], &str, &str),
) -> Result<Vec<String>, LoaderError> {
    let query = sql_adapter(commit.warehouse).render_check_query(
        commit.table,
        CheckQuery::KeySources {
            key,
            staged: commit.staging,
            file_column,
            file,
        },
    );
    text_values(commit.destination, query).await
}
