//! Why the destination cannot load a warehouse source's unloaded files exactly by reading them itself.

use crate::adapters::snowflake::main::copy::_stage_covers::stage_covers;
use crate::adapters::snowflake::models::StageRoute;
use crate::column_types::models::Warehouse;
use crate::loader::constants::{UNLOADED_OUTSIDE_STAGE, UNLOADED_TIMES};
use crate::loader::models::WarehouseRoute;
use crate::routing::models::Store;
use crate::store_paths::main::resolve_location::resolve_location;

/// Why `warehouse` cannot load a table's unloaded files exactly by reading them itself, so SQLLoad reads them instead; `None` when it can (where it also reaches their store). Only DuckDB and Snowflake turn the files' whole microseconds back into times and timestamps; a table that `masks` columns needs SQLLoad to see the rows; Snowflake reads another session's unload only through the target's external `stage`, which must cover the S3 folder `route` unloads to, as a user or named stage of the source's is not its to read.
pub fn unload_blocked(
    warehouse: Warehouse,
    masks: bool,
    route: Option<&WarehouseRoute>,
    stage: Option<&StageRoute>,
) -> Option<String> {
    let covered = match (route, stage) {
        (
            Some(WarehouseRoute::Unload {
                store: Store::ObjectStore(url),
                ..
            }),
            Some(StageRoute::External(stage)),
        ) => resolve_location(None, &format!("{}/", url.trim_end_matches('/')))
            .is_ok_and(|unloaded| stage_covers(&stage.location, &unloaded)),
        _ => false,
    };
    match (warehouse, masks) {
        (Warehouse::DuckDb | Warehouse::Snowflake, true) => {
            Some("the table masks columns".to_owned())
        }
        (Warehouse::Snowflake, false) if !covered => Some(UNLOADED_OUTSIDE_STAGE.to_owned()),
        (Warehouse::DuckDb | Warehouse::Snowflake, false) => None,
        _ => Some(UNLOADED_TIMES.to_owned()),
    }
}
