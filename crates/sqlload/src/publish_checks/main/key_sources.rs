//! Which files hold which key values, among a commit's staged rows or its table's.

use crate::adapters::contract::models::{CheckQuery, TableName};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::loader::errors::LoaderError;
use crate::publish_checks::models::CheckedCommit;

/// The distinct `(file, key values)` pairs of `relation`'s rows (`file_column`, and the `key` values as `KeyValues` gives them), only those whose values a row of `held_by` holds when set: with `relation` the staged rows, which file of a commit holds which values; with the table and `held_by` the staged rows, the files a `delete_insert` by `replace_key` would delete rows of.
pub async fn key_sources(
    commit: &CheckedCommit<'_>,
    (relation, held_by): (&TableName, Option<&TableName>),
    (key, file_column): (&[String], &str),
) -> Result<Vec<(String, String)>, LoaderError> {
    let query = sql_adapter(commit.warehouse).render_check_query(
        relation,
        CheckQuery::KeyFiles {
            key,
            file_column,
            held_by,
        },
    );
    let texts = text_values(commit.destination, query).await?;
    texts
        .iter()
        .map(|text| {
            let (length, rest) = text.split_once(':').unwrap_or_default();
            let length: usize = length.parse().map_err(|_| {
                LoaderError::Invalid(format!("the key's file is unreadable: {text}"))
            })?;
            let split = rest
                .char_indices()
                .nth(length)
                .map_or(rest.len(), |(at, _)| at);
            let (file, values) = rest.split_at(split);
            Ok((file.to_owned(), values.to_owned()))
        })
        .collect()
}
