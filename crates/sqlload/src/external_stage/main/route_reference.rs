//! Name in SQL the stage a table's route loads from.

use crate::column_types::models::Warehouse;
use crate::external_stage::main::stage_reference::stage_reference;
use crate::snowflake_destination::constants::USER_STAGE;
use crate::snowflake_destination::models::StageRoute;
use crate::write_sql::main::quoted_identifier::quoted_identifier;
use crate::write_sql::models::TableName;

/// The table stage of `table` without its `@`, `"DB"."SCHEMA".%"TABLE"`.
pub fn table_stage(table: &TableName) -> String {
    let snowflake = Warehouse::Snowflake;
    let database = table
        .database
        .as_ref()
        .map(|database| format!("{}.", quoted_identifier(database, snowflake)))
        .unwrap_or_default();
    format!(
        "{database}{}.%{}",
        quoted_identifier(&table.schema, snowflake),
        quoted_identifier(&table.table, snowflake)
    )
}

/// The stage `route` loads from, as SQL; `target` is the table loaded, `staging` its staging table if any; `None` for a route without files.
pub fn route_reference(
    route: &StageRoute,
    target: &TableName,
    staging: Option<&TableName>,
) -> Option<String> {
    match route {
        StageRoute::External(stage) => Some(stage_reference(stage, target)),
        StageRoute::Internal { name } => Some(format!("@{name}")),
        StageRoute::User => Some(format!("@{USER_STAGE}")),
        StageRoute::TargetTable => Some(format!("@{}", table_stage(target))),
        StageRoute::StagingTable => staging.map(|staging| format!("@{}", table_stage(staging))),
        StageRoute::Streaming { .. } | StageRoute::Insert => None,
    }
}
