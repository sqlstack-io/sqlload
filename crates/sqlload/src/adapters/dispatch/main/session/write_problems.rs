//! What stops or endangers a write into a table.

use crate::adapters::contract::models::{TableName, WriteMode};
use crate::adapters::dispatch::models::Destination;
use crate::adapters::postgres::main::session::_write_problems::write_problems as postgres_write_problems;
use crate::loader::errors::LoaderError;

/// Why `mode` cannot run against `table` (`exists` when it does) (problems), and what it may fail on (warnings); none where the warehouse's writes need nothing of the table or its server.
pub async fn write_problems(
    destination: &Destination,
    table: &TableName,
    (mode, exists): (&WriteMode, bool),
) -> Result<(Vec<String>, Vec<String>), LoaderError> {
    match destination {
        Destination::Postgres(postgres) => {
            postgres_write_problems(postgres, table, (mode, exists)).await
        }
        _ => Ok((Vec::new(), Vec::new())),
    }
}
