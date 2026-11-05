//! The stage route a destination loads a table through.

use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::snowflake::main::stage::_stage_route::stage_route;
use crate::adapters::snowflake::models::{PlannedStage, RouteRequest, StageRoute};

/// The stage route `destination` loads `target` through (`exists`: the table is live; `direct_append`: an unlogged append may write it directly), or why none is usable; `None` when the destination loads without a stage.
pub async fn choose_stage(
    destination: &Destination,
    (target, exists, direct_append): (&TableName, bool, bool),
) -> Option<Result<(StageRoute, Option<PlannedStage>), String>> {
    let Destination::Snowflake(snowflake) = destination else {
        return None;
    };
    let request = RouteRequest {
        destination: target,
        table_exists: exists,
        direct_append,
        snowflake,
    };
    Some(stage_route(&request).await)
}
