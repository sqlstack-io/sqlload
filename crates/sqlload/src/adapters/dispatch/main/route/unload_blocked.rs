//! Why the destination cannot load a warehouse source's unloaded files exactly by reading them itself.

use crate::column_types::models::Warehouse;
use crate::loader::constants::UNLOADED_TIMES;
use crate::loader::models::TableSpec;

/// Why the destination cannot load the table's unloaded files exactly by reading them itself, so SQLLoad reads them instead; `None` when it can (where it also reaches their store). Only DuckDB turns the files' whole microseconds back into times and timestamps; masks need SQLLoad to see the rows.
pub fn unload_blocked(spec: &TableSpec) -> Option<String> {
    match (spec.warehouse, spec.masked.columns.is_empty()) {
        (Warehouse::DuckDb, true) => None,
        (Warehouse::DuckDb, false) => Some("the table masks columns".to_owned()),
        _ => Some(UNLOADED_TIMES.to_owned()),
    }
}
