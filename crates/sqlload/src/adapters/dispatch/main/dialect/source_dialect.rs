//! The SQL dialect a warehouse source reads in.

use crate::bigquery_source::constants::BIGQUERY_SOURCE;
use crate::column_types::models::Warehouse;
use crate::loader::constants::SNOWFLAKE_SOURCE;

/// The warehouse whose SQL a warehouse source of `system` (its kind as written) filters in; `None` for a system without one.
pub fn source_dialect(system: &str) -> Option<Warehouse> {
    match system {
        SNOWFLAKE_SOURCE => Some(Warehouse::Snowflake),
        BIGQUERY_SOURCE => Some(Warehouse::BigQuery),
        _ => None,
    }
}
