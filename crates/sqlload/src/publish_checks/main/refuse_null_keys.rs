//! Refuse a merge or `delete_insert` batch holding a NULL in its key.

use crate::loader::errors::LoaderError;
use crate::publish_checks::_helpers::failures::null_keys;
use crate::publish_checks::models::CheckedCommit;

/// Fail `commit` when a staged row holds a NULL in a column of `unique_key` (the key setting `setting` names: `unique_key`, or `replace_key`), naming the columns and example keys, so a merge or `delete_insert` under `null_keys = "fail"` never writes a NULL key; `file` says the rows are one source file.
pub async fn refuse_null_keys(
    commit: &CheckedCommit<'_>,
    (unique_key, setting): (&[String], &str),
    file: bool,
) -> Result<(), LoaderError> {
    let subject = match file {
        true => "the file holds",
        false => "the loaded rows hold",
    };
    match null_keys(commit, unique_key).await? {
        None => Ok(()),
        Some(detail) => Err(LoaderError::Invalid(format!(
            "{subject} a NULL in its {setting}: {detail}; null_keys = \"fail\" refuses a NULL in a key column, so fill it in the source, or set null_keys = \"match\" to match it with a NULL"
        ))),
    }
}
