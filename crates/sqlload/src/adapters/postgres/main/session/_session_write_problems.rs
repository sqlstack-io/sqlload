//! What stops or endangers a write into a table loaded through a caller's session.

use crate::adapters::contract::models::{TableName, WriteMode};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::postgres::_helpers::null_keys::{index_problem, version_problem};
use crate::adapters::postgres::_helpers::write_checks::{
    as_text, key_index_text_query, nullable_keys, parsed, reference_outcome, references_text_query,
    row_text, text_key_indexes, text_references,
};
use crate::adapters::postgres::constants::SERVER_VERSION_SQL;
use crate::adapters::postgres::models::PostgresSql;
use crate::adapters::session::main::_query_rows::query_rows;
use crate::adapters::session::models::SessionDestination;
use crate::loader::errors::LoaderError;

/// The same checks as SQLLoad's own Postgres session runs before a write (see `write_problems`), with the same queries run as text on the caller's session.
pub(in crate::adapters) async fn session_write_problems(
    destination: &SessionDestination,
    table: &TableName,
    (mode, exists): (&WriteMode, bool),
) -> Result<(Vec<String>, Vec<String>), LoaderError> {
    let stored = TableName {
        database: None,
        schema: PostgresSql.stored_name(&table.schema),
        table: PostgresSql.stored_name(&table.table),
    };
    match mode {
        WriteMode::Merge { unique_key, .. } => {
            merge_problems(destination, &stored, (mode, unique_key, exists)).await
        }
        WriteMode::Replace | WriteMode::DeleteInsert { .. } if exists => {
            let place = (stored.schema.as_str(), stored.table.as_str());
            let query = references_text_query(place).map_err(LoaderError::Invalid)?;
            let rows = text_references(&query_rows(destination, query).await?)
                .map_err(LoaderError::Invalid)?;
            Ok(reference_outcome(&rows, place, mode))
        }
        _ => Ok((Vec::new(), Vec::new())),
    }
}

/// What stops a merge on `unique_key` into `stored` (which exists when `exists`): no unique index `ON CONFLICT` can use, or a key that may be NULL on a server before Postgres 15.
async fn merge_problems(
    destination: &SessionDestination,
    stored: &TableName,
    (mode, unique_key, exists): (&WriteMode, &[String], bool),
) -> Result<(Vec<String>, Vec<String>), LoaderError> {
    let indexes = match exists {
        true => {
            let query = key_index_text_query(stored, unique_key).map_err(LoaderError::Invalid)?;
            Some(
                text_key_indexes(&first_row(destination, query).await?)
                    .map_err(LoaderError::Invalid)?,
            )
        }
        false => None,
    };
    let nullable = nullable_keys(mode, indexes.as_ref());
    if !nullable.is_empty() {
        let row = first_row(destination, as_text(SERVER_VERSION_SQL, 2)).await?;
        let number: i32 = parsed(&row, 0).map_err(LoaderError::Invalid)?;
        if let Some(problem) = version_problem(stored, &nullable, (number, &row_text(&row, 1))) {
            return Ok((vec![problem], Vec::new()));
        }
    }
    Ok((
        indexes
            .and_then(|indexes| index_problem(stored, (unique_key, &nullable), &indexes))
            .into_iter()
            .collect(),
        Vec::new(),
    ))
}

/// The first row of `query` on the session.
async fn first_row(
    destination: &SessionDestination,
    query: String,
) -> Result<Vec<Option<String>>, LoaderError> {
    query_rows(destination, query)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| LoaderError::Invalid("a check before the write returned no row".to_owned()))
}
