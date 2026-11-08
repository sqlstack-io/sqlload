//! Whether `merge` can upsert into a table on its key.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::postgres::_helpers::session::{query_error, session};
use crate::adapters::postgres::constants::KEY_INDEX_SQL;
use crate::adapters::postgres::models::{PostgresDestination, PostgresSql};
use crate::loader::errors::LoaderError;

/// Why `INSERT ... ON CONFLICT` on `unique_key` cannot run against the existing `table`: it has no unique index on exactly those columns.
pub(in crate::adapters) async fn key_problem(
    destination: &PostgresDestination,
    table: &TableName,
    unique_key: &[String],
) -> Result<Option<String>, LoaderError> {
    let held = session(destination).await?;
    let Some((client, _)) = held.as_ref() else {
        return Ok(None);
    };
    let mut key: Vec<String> = unique_key
        .iter()
        .map(|name| PostgresSql.stored_name(name))
        .collect();
    key.sort();
    let schema = PostgresSql.stored_name(&table.schema);
    let name = PostgresSql.stored_name(&table.table);
    let found: i64 = client
        .query_one(KEY_INDEX_SQL, &[&schema, &name, &key])
        .await
        .and_then(|row| row.try_get(0))
        .map_err(query_error)?;
    Ok((found == 0).then(|| {
        format!(
            "merge upserts with INSERT ... ON CONFLICT ({}), which needs a unique index or constraint on exactly those columns, and {}.{} has none; add one, for example CREATE UNIQUE INDEX ON {} ({})",
            unique_key.join(", "),
            table.schema,
            table.table,
            PostgresSql.render_qualified_name(table),
            key.iter()
                .map(|column| PostgresSql.render_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }))
}
