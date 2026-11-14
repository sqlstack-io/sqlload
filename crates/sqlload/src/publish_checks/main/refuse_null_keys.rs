//! Refuse a merge batch holding a NULL in its key.

use crate::loader::errors::LoaderError;
use crate::publish_checks::_helpers::failures::null_keys;
use crate::publish_checks::models::CheckedCommit;

/// Fail `commit` when a staged row holds a NULL in a column of `unique_key`, naming the columns and example keys, so a merge under `null_keys = "fail"` never writes a NULL key; `file` says the rows are one source file.
pub async fn refuse_null_keys(
    commit: &CheckedCommit<'_>,
    unique_key: &[String],
    file: bool,
) -> Result<(), LoaderError> {
    let subject = match file {
        true => "the file holds",
        false => "the loaded rows hold",
    };
    match null_keys(commit, unique_key).await? {
        None => Ok(()),
        Some(detail) => Err(LoaderError::Invalid(format!(
            "{subject} a NULL in its unique_key: {detail}; null_keys = \"fail\" refuses a NULL in a key column, so fill it in the source, or set null_keys = \"match\" to match it with a NULL"
        ))),
    }
}
