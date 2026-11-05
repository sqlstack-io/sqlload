//! What a rebuilt table must carry over.

use crate::adapters::contract::models::{TableName, TableShape};
use crate::adapters::dispatch::models::Destination;
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::main::_table_shape::table_shape as duckdb_table_shape;
use crate::loader::errors::LoaderError;

/// What a rebuild of `table` must carry over itself: nothing where the warehouse copies a table's shape (Snowflake's `LIKE`).
pub async fn table_shape(
    destination: &Destination,
    table: &TableName,
) -> Result<Option<TableShape>, LoaderError> {
    #[cfg(not(feature = "duckdb"))]
    let _ = table;
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) => Ok(duckdb_table_shape(duckdb, table).await?),
        _ => Ok(None),
    }
}
