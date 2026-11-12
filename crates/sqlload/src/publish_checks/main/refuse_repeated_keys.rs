//! Refuse a merge batch that repeats its key.

use crate::loader::errors::LoaderError;
use crate::publish_checks::_helpers::failures::repeated_keys;
use crate::publish_checks::models::CheckedCommit;

/// Fail `commit` when its staged rows hold one value of `unique_key` more than once, naming the key's columns and examples, so a merge never inserts a key twice or fails on the warehouse's own error; rows with a NULL in the key are not compared.
pub async fn refuse_repeated_keys(
    commit: &CheckedCommit<'_>,
    unique_key: &[String],
) -> Result<(), LoaderError> {
    match repeated_keys(commit, unique_key).await? {
        None => Ok(()),
        Some((_, detail)) => Err(LoaderError::Invalid(format!(
            "the file repeats its unique_key: {detail}; a merge refuses a file holding a key more than once, so remove the repeats or load it with append"
        ))),
    }
}
