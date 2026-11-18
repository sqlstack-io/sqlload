//! Why the destination cannot load a warehouse source's unloaded files exactly by reading them itself.

use crate::column_types::models::Warehouse;
use crate::loader::constants::{UNLOADED_OUTSIDE_S3, UNLOADED_TIMES};
use crate::loader::models::{TableSpec, WarehouseRoute, WarehouseSource};
use crate::routing::models::Store;

/// Why the destination cannot load the table's unloaded files exactly by reading them itself, so SQLLoad reads them instead; `None` when it can (where it also reaches their store). Only DuckDB and Snowflake turn the files' whole microseconds back into times and timestamps; masks need SQLLoad to see the rows; Snowflake reads another session's unload only from S3, through an external stage of its own, as a user or named stage of the source's is not its to read.
pub fn unload_blocked(spec: &TableSpec) -> Option<String> {
    let in_s3 = matches!(
        &spec.warehouse_source,
        Some(WarehouseSource {
            route: WarehouseRoute::Unload {
                store: Store::ObjectStore(url),
                ..
            },
            ..
        }) if url.starts_with("s3://")
    );
    match (spec.warehouse, spec.masked.columns.is_empty()) {
        (Warehouse::DuckDb | Warehouse::Snowflake, false) => {
            Some("the table masks columns".to_owned())
        }
        (Warehouse::Snowflake, true) if !in_s3 => Some(UNLOADED_OUTSIDE_S3.to_owned()),
        (Warehouse::DuckDb | Warehouse::Snowflake, true) => None,
        _ => Some(UNLOADED_TIMES.to_owned()),
    }
}
