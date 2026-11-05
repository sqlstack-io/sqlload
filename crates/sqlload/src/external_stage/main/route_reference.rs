//! Name in SQL the stage a table's route loads from.

use crate::external_stage::_helpers::table_stage::table_stage;
use crate::external_stage::main::stage_reference::stage_reference;
use crate::adapters::snowflake::constants::USER_STAGE;
use crate::adapters::snowflake::models::StageRoute;
use crate::adapters::contract::models::TableName;

/// The stage `route` loads from, as SQL; `target` is the table loaded, `staging` its staging table if any; `None` for a route without files.
pub fn route_reference(
    route: &StageRoute,
    target: &TableName,
    staging: Option<&TableName>,
) -> Option<String> {
    match route {
        StageRoute::External(stage) => Some(stage_reference(stage)),
        StageRoute::Internal { name } => Some(format!("@{name}")),
        StageRoute::User => Some(format!("@{USER_STAGE}")),
        StageRoute::TargetTable => Some(format!("@{}", table_stage(target))),
        StageRoute::StagingTable => staging.map(|staging| format!("@{}", table_stage(staging))),
        StageRoute::Streaming { .. } | StageRoute::Insert => None,
    }
}
