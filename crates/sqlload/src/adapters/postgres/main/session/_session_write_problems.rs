//! What stops or endangers a write into a table loaded through a caller's session.

use crate::adapters::contract::models::{TableName, WriteMode};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::postgres::_helpers::null_keys::{index_problem, version_problem};
use crate::adapters::postgres::_helpers::write_checks::{
    as_text, bound, nullable_keys, reference_outcome, sorted_key, text_array,
};
use crate::adapters::postgres::constants::{KEY_INDEX_SQL, REFERENCES_SQL, SERVER_VERSION_SQL};
use crate::adapters::postgres::models::{KeyIndexes, PostgresSql, ReferenceRow};
use crate::adapters::session::main::_query_rows::query_rows;
use crate::adapters::session::models::SessionDestination;
use crate::loader::errors::LoaderError;

/// The same checks as SQLLoad's own Postgres session runs before a write (see `write_problems`), with the same queries run as text on the caller's session.
pub(in crate::adapters) async fn session_write_problems(
    destination: &SessionDestination,
    table: &TableName,
    (mode, exists): (&WriteMode, bool),
) -> Result<(Vec<String>, Vec<String>), LoaderError> {
    let schema = PostgresSql.stored_name(&table.schema);
    let name = PostgresSql.stored_name(&table.table);
    let stored = TableName {
        database: None,
        schema: schema.clone(),
        table: name.clone(),
    };
    match mode {
        WriteMode::Merge { unique_key, .. } => {
            let indexes = match exists {
                true => Some(key_indexes(destination, &stored, unique_key).await?),
                false => None,
            };
            let nullable = nullable_keys(mode, indexes.as_ref());
            if !nullable.is_empty() {
                let row = first_row(destination, as_text(SERVER_VERSION_SQL, 2)).await?;
                let number: i32 = number(&row, 0)?;
                let version = text(&row, 1);
                if let Some(problem) = version_problem(&stored, &nullable, (number, &version)) {
                    return Ok((vec![problem], Vec::new()));
                }
            }
            Ok((
                indexes
                    .and_then(|indexes| index_problem(&stored, (unique_key, &nullable), &indexes))
                    .into_iter()
                    .collect(),
                Vec::new(),
            ))
        }
        WriteMode::Replace | WriteMode::DeleteInsert { .. } if exists => {
            let query = bound(REFERENCES_SQL, &[literal(&schema), literal(&name)])
                .map_err(LoaderError::Invalid)?;
            let rows = query_rows(destination, as_text(&query, 5))
                .await?
                .iter()
                .map(|row| {
                    Ok(ReferenceRow {
                        referencing: text(row, 0),
                        constraint: text(row, 1),
                        action: text(row, 2),
                        deferred: text(row, 3) == "true",
                        own: text(row, 4) == "true",
                    })
                })
                .collect::<Result<Vec<ReferenceRow>, LoaderError>>()?;
            Ok(reference_outcome(&rows, (&schema, &name), mode))
        }
        _ => Ok((Vec::new(), Vec::new())),
    }
}

/// The unique indexes `INSERT ... ON CONFLICT` on `unique_key` could use on `table`, and which of the key columns the table declares NOT NULL.
async fn key_indexes(
    destination: &SessionDestination,
    table: &TableName,
    unique_key: &[String],
) -> Result<KeyIndexes, LoaderError> {
    let key: Vec<String> = sorted_key(unique_key)
        .iter()
        .map(|column| literal(column))
        .collect();
    let query = bound(
        KEY_INDEX_SQL,
        &[
            literal(&table.schema),
            literal(&table.table),
            format!("ARRAY[{}]", key.join(", ")),
        ],
    )
    .map_err(LoaderError::Invalid)?;
    let row = first_row(destination, as_text(&query, 5)).await?;
    Ok(KeyIndexes {
        count: number(&row, 0)?,
        nulls_equal: number(&row, 1)?,
        others: text(&row, 2),
        drops: text(&row, 3),
        not_null: text_array(&text(&row, 4)).map_err(LoaderError::Invalid)?,
    })
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

/// The text of `row`'s value at `index`, empty for NULL.
fn text(row: &[Option<String>], index: usize) -> String {
    row.get(index).cloned().flatten().unwrap_or_default()
}

/// `row`'s value at `index` as a number.
fn number<T: std::str::FromStr>(row: &[Option<String>], index: usize) -> Result<T, LoaderError> {
    let value = text(row, index);
    value.parse::<T>().map_err(|_| {
        LoaderError::Invalid(format!(
            "a check before the write gave {value:?} for a number"
        ))
    })
}

/// `text` as a Postgres string literal.
fn literal(text: &str) -> String {
    PostgresSql.render_loader_value_literal(text)
}
