//! Refuse a merge batch that repeats its key.

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
            "{subject} its unique_key: {detail}; on_duplicate_keys = \"fail\" refuses a key held more than once, so remove the repeats, or drop that setting to keep the last row of each key"
        ))),
    }
}
