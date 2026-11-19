//! Refuse a merge or `delete_insert` batch holding a NULL in its key.

use crate::adapters::contract::models::CheckQuery;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::files_source::constants::{SOURCE_FILE_COLUMN, VERSION_SEPARATOR};
use crate::loader::errors::LoaderError;
use crate::publish_checks::_helpers::failures::null_keys;
use crate::publish_checks::models::CheckedCommit;

/// Fail `commit` when a staged row holds a NULL in a column of `unique_key` (the key setting `setting` names: `unique_key`, or `replace_key`), naming the columns and example keys, so a merge or `delete_insert` under `null_keys = "fail"` never writes a NULL key. `files` are the source files of the commit (`name|version`, in order), if any: with one, the rows are that file's; with more, the error names the first holding such a row, when the table writes `_sqlload_source_file`.
pub async fn refuse_null_keys(
    commit: &CheckedCommit<'_>,
    (unique_key, setting): (&[String], &str),
    files: &[String],
) -> Result<(), LoaderError> {
    let subject = match files.len() {
        0 => "the loaded rows hold",
        _ => "the file holds",
    };
    let Some(detail) = null_keys(commit, unique_key).await? else {
        return Ok(());
    };
    let names_files = commit
        .columns
        .iter()
        .any(|column| column.name == SOURCE_FILE_COLUMN);
    let named = match files.len() {
        _ if !names_files => String::new(),
        0 | 1 => String::new(),
        _ => {
            let query = sql_adapter(commit.warehouse).render_check_query(
                commit.staging,
                CheckQuery::NullKeyFiles(unique_key, SOURCE_FILE_COLUMN),
            );
            let holding = text_values(commit.destination, query).await?;
            let mut named = String::new();
            for file in files {
                let name = file
                    .rsplit_once(VERSION_SEPARATOR)
                    .map_or(file.as_str(), |(name, _)| name);
                if holding.contains(&name.to_owned()) {
                    named = format!("{name}: ");
                    break;
                }
            }
            named
        }
    };
    Err(LoaderError::Invalid(format!(
        "{named}{subject} a NULL in its {setting}: {detail}; null_keys = \"fail\" refuses a NULL in a key column, so fill it in the source, or set null_keys = \"match\" to match it with a NULL"
    )))
}
