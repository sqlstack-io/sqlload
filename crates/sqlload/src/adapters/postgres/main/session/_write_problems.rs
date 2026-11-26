//! What stops or endangers a write into a table.

use tokio_postgres::Client;

use crate::adapters::contract::models::{TableName, WriteMode};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::postgres::_helpers::null_keys::{index_problem, version_problem};
use crate::adapters::postgres::_helpers::session::{query_error, session};
use crate::adapters::postgres::_helpers::write_checks::{
    nullable_keys, reference_outcome, sorted_key,
};
use crate::adapters::postgres::constants::{KEY_INDEX_SQL, REFERENCES_SQL, SERVER_VERSION_SQL};
use crate::adapters::postgres::models::{
    KeyIndexes, PostgresDestination, PostgresSql, ReferenceRow,
};
use crate::loader::errors::LoaderError;

/// Why `mode` cannot run against `table` (`exists` when it does), and what it may fail on: `merge` needs a unique index `ON CONFLICT` can use on exactly its key, which treats NULLs as equal (Postgres 15 and later) when a key column may hold NULL (the source says it may, and an existing table does not declare it NOT NULL), so a row with a NULL key matches the one the table holds; `replace` and `delete_insert` delete rows, which a foreign key with `ON DELETE CASCADE`, `SET NULL` or `SET DEFAULT` would carry into its own table (refused), and one checked at once (any `RESTRICT`) would refuse while its rows reference them (warned); a table's reference to itself only matters to `delete_insert`, since `replace` deletes every row.
pub(in crate::adapters) async fn write_problems(
    destination: &PostgresDestination,
    table: &TableName,
    (mode, exists): (&WriteMode, bool),
) -> Result<(Vec<String>, Vec<String>), LoaderError> {
    let held = session(destination).await?;
    let Some((client, _)) = held.as_ref() else {
        return Ok((Vec::new(), Vec::new()));
    };
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
                true => Some(key_indexes(client, &stored, unique_key).await?),
                false => None,
            };
            let nullable = nullable_keys(mode, indexes.as_ref());
            if !nullable.is_empty() {
                let row = client
                    .query_one(SERVER_VERSION_SQL, &[])
                    .await
                    .map_err(query_error)?;
                let number: i32 = row.try_get(0).map_err(query_error)?;
                let version: String = row.try_get(1).map_err(query_error)?;
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
            let rows = reference_rows(client, (&schema, &name)).await?;
            Ok(reference_outcome(&rows, (&schema, &name), mode))
        }
        _ => Ok((Vec::new(), Vec::new())),
    }
}

/// The unique indexes `INSERT ... ON CONFLICT` on `unique_key` could use on `table`, and which of the key columns the table declares NOT NULL.
async fn key_indexes(
    client: &Client,
    table: &TableName,
    unique_key: &[String],
) -> Result<KeyIndexes, LoaderError> {
    let key = sorted_key(unique_key);
    let row = client
        .query_one(KEY_INDEX_SQL, &[&table.schema, &table.table, &key])
        .await
        .map_err(query_error)?;
    Ok(KeyIndexes {
        count: row.try_get(0).map_err(query_error)?,
        nulls_equal: row.try_get(1).map_err(query_error)?,
        others: row.try_get(2).map_err(query_error)?,
        drops: row.try_get(3).map_err(query_error)?,
        not_null: row.try_get(4).map_err(query_error)?,
    })
}

/// The foreign keys referencing `schema.name`.
async fn reference_rows(
    client: &Client,
    (schema, name): (&str, &str),
) -> Result<Vec<ReferenceRow>, LoaderError> {
    client
        .query(REFERENCES_SQL, &[&schema, &name])
        .await
        .map_err(query_error)?
        .iter()
        .map(|row| {
            Ok(ReferenceRow {
                referencing: row.try_get(0)?,
                constraint: row.try_get(1)?,
                action: row.try_get(2)?,
                deferred: row.try_get(3)?,
                own: row.try_get(4)?,
            })
        })
        .collect::<Result<Vec<ReferenceRow>, tokio_postgres::Error>>()
        .map_err(query_error)
}
