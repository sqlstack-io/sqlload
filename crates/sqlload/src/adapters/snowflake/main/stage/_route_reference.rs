//! Name in SQL the stage a table's route loads from.

use crate::adapters::contract::models::TableName;
use crate::adapters::snowflake::_helpers::stage::table_stage::table_stage;
use crate::adapters::snowflake::constants::USER_STAGE;
use crate::adapters::snowflake::main::stage::_stage_reference::stage_reference;
use crate::adapters::snowflake::models::StageRoute;

/// The stage `route` loads from, as SQL; `target` is the table loaded, `staging` its staging table if any; `None` for a route without files.
pub(in crate::adapters) fn route_reference(
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
