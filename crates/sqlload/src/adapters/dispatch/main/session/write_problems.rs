//! What stops or endangers a write into an existing table.

use crate::adapters::contract::models::{TableName, WriteMode};
use crate::adapters::dispatch::models::Destination;
use crate::adapters::postgres::main::session::_write_problems::write_problems as postgres_write_problems;
use crate::loader::errors::LoaderError;

/// Why `mode` cannot run against the existing `table` (problems), and what it may fail on (warnings); none where the warehouse's writes need nothing of the table.
pub async fn write_problems(
    destination: &Destination,
    table: &TableName,
    mode: &WriteMode,
) -> Result<(Vec<String>, Vec<String>), LoaderError> {
    match destination {
        Destination::Postgres(postgres) => postgres_write_problems(postgres, table, mode).await,
        _ => Ok((Vec::new(), Vec::new())),
    }
}
