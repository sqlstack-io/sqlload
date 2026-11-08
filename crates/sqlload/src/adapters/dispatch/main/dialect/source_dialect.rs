//! The SQL dialect a warehouse source reads in.

use crate::column_types::models::Warehouse;
use crate::loader::constants::SNOWFLAKE_SOURCE;

/// The warehouse whose SQL a warehouse source of `system` (its kind as written) filters in; `None` for a system without one.
pub fn source_dialect(system: &str) -> Option<Warehouse> {
    (system == SNOWFLAKE_SOURCE).then_some(Warehouse::Snowflake)
}
