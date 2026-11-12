//! Refuse a merge batch that repeats its key.

use crate::files_source::constants::SOURCE_ROW_COLUMN;
use crate::loader::errors::LoaderError;
use crate::publish_checks::_helpers::failures::repeated_keys;
use crate::publish_checks::models::CheckedCommit;

/// Fail `commit` when its staged rows hold one value of `unique_key` more than once, naming the key's columns and examples, so a merge under `on_duplicate_keys = "fail"` never inserts a key twice or fails on the warehouse's own error; `file` says the rows are one source file. Rows with a NULL in the key are not compared.
pub async fn refuse_repeated_keys(
    commit: &CheckedCommit<'_>,
    unique_key: &[String],
    file: bool,
) -> Result<(), LoaderError> {
    let subject = match file {
        true => "the file repeats",
        false => "the loaded rows repeat",
    };
    match repeated_keys(commit, unique_key).await? {
        None => Ok(()),
        Some((_, detail)) => Err(LoaderError::Invalid(format!(
            "{subject} its unique_key: {detail}; this merge keeps no row of a repeated key (on_duplicate_keys = \"fail\", or a files source that does not write {SOURCE_ROW_COLUMN}), so remove the repeats"
        ))),
    }
}
