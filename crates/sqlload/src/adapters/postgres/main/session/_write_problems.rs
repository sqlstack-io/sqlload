//! What stops or endangers a write into an existing table.

use tokio_postgres::Client;

use crate::adapters::contract::models::{TableName, WriteMode};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::postgres::_helpers::session::{query_error, session};
use crate::adapters::postgres::constants::{KEY_INDEX_SQL, REFERENCES_SQL};
use crate::adapters::postgres::models::{PostgresDestination, PostgresSql};
use crate::loader::errors::LoaderError;

/// Why `mode` cannot run against the existing `table`, and what it may fail on: `merge` needs a unique index `ON CONFLICT` can use on exactly its key; `replace` and `delete_insert` delete rows, which a foreign key with `ON DELETE CASCADE`, `SET NULL` or `SET DEFAULT` would carry into its own table (refused), and one checked at once (any `RESTRICT`) would refuse while its rows reference them (warned); a table's reference to itself only matters to `delete_insert`, since `replace` deletes every row.
pub(in crate::adapters) async fn write_problems(
    destination: &PostgresDestination,
    table: &TableName,
    mode: &WriteMode,
) -> Result<(Vec<String>, Vec<String>), LoaderError> {
    let held = session(destination).await?;
    let Some((client, _)) = held.as_ref() else {
        return Ok((Vec::new(), Vec::new()));
    };
    let schema = PostgresSql.stored_name(&table.schema);
    let name = PostgresSql.stored_name(&table.table);
    match mode {
        WriteMode::Merge { unique_key } => Ok((
            key_problem(client, (&schema, &name), unique_key)
                .await?
                .into_iter()
                .collect(),
            Vec::new(),
        )),
        WriteMode::Replace | WriteMode::DeleteInsert { .. } => {
            reference_problems(client, (&schema, &name), mode).await
        }
        _ => Ok((Vec::new(), Vec::new())),
    }
}

/// Why `INSERT ... ON CONFLICT` on `unique_key` cannot run against `schema.name`: it has no unique index on exactly those columns that the statement can use.
async fn key_problem(
    client: &Client,
    (schema, name): (&str, &str),
    unique_key: &[String],
) -> Result<Option<String>, LoaderError> {
    let mut key: Vec<String> = unique_key
        .iter()
        .map(|column| PostgresSql.stored_name(column))
        .collect();
    key.sort();
    let found: i64 = client
        .query_one(KEY_INDEX_SQL, &[&schema, &name, &key])
        .await
        .and_then(|row| row.try_get(0))
        .map_err(query_error)?;
    let columns: Vec<String> = key
        .iter()
        .map(|column| PostgresSql.render_identifier(column))
        .collect();
    Ok((found == 0).then(|| {
        format!(
            "merge upserts with INSERT ... ON CONFLICT ({}), which needs a valid, non-deferrable unique index or constraint on exactly those columns, and {schema}.{name} has none; add one, for example CREATE UNIQUE INDEX ON {} ({})",
            unique_key.join(", "),
            PostgresSql.render_qualified_name(&TableName {
                database: None,
                schema: schema.to_owned(),
                table: name.to_owned(),
            }),
            columns.join(", ")
        )
    }))
}

/// The foreign keys referencing `schema.name` that a write deleting its rows would carry into their tables (problems), or that would refuse it while their rows reference the rows it deletes (warnings); `replace` skips the table's own.
async fn reference_problems(
    client: &Client,
    (schema, name): (&str, &str),
    mode: &WriteMode,
) -> Result<(Vec<String>, Vec<String>), LoaderError> {
    let strategy = match mode {
        WriteMode::Replace => "replace",
        _ => "delete_insert",
    };
    let rows = client
        .query(REFERENCES_SQL, &[&schema, &name])
        .await
        .map_err(query_error)?;
    let (mut problems, mut warnings) = (Vec::new(), Vec::new());
    for row in rows {
        let referencing: String = row.try_get(0).map_err(query_error)?;
        let constraint: String = row.try_get(1).map_err(query_error)?;
        let action: String = row.try_get(2).map_err(query_error)?;
        let deferred: bool = row.try_get(3).map_err(query_error)?;
        let own: bool = row.try_get(4).map_err(query_error)?;
        let carried = match action.as_str() {
            "c" => Some("ON DELETE CASCADE deletes"),
            "n" => Some("ON DELETE SET NULL changes"),
            "d" => Some("ON DELETE SET DEFAULT changes"),
            _ => None,
        };
        match (carried, deferred, action.as_str(), own && matches!(mode, WriteMode::Replace)) {
            (_, _, _, true) => {}
            (Some(effect), _, _, _) => problems.push(format!(
                "{referencing} references {schema}.{name} ({constraint}), and its {effect} its rows when {strategy} deletes the rows it replaces; SQLLoad refuses to change another table, so change the constraint's ON DELETE action, or load with merge"
            )),
            (None, true, "a", _) => {}
            (None, _, _, _) => warnings.push(format!(
                "{referencing} references {schema}.{name} ({constraint}), so {strategy} fails while any of its rows reference a row it deletes"
            )),
        }
    }
    Ok((problems, warnings))
}
