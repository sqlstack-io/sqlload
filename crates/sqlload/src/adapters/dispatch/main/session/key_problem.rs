//! Whether a destination can `merge` into an existing table on its key.

use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::postgres::main::session::_key_problem::key_problem as postgres_key_problem;
use crate::loader::errors::LoaderError;

/// Why `merge` on `unique_key` cannot run against the existing `table`; `None` where it can, or where the warehouse's `MERGE` needs nothing of the table.
pub async fn key_problem(
    destination: &Destination,
    table: &TableName,
    unique_key: &[String],
) -> Result<Option<String>, LoaderError> {
    match destination {
        Destination::Postgres(postgres) => postgres_key_problem(postgres, table, unique_key).await,
        _ => Ok(None),
    }
}
