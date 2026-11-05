//! Name a stage in SQL.

use crate::column_types::models::Warehouse;
use crate::external_stage::constants::MANAGED_STAGE_NAME;
use crate::adapters::snowflake::models::{ExternalStage, StageObject};
use crate::adapters::contract::main::qualified_name::qualified_name;
use crate::adapters::contract::models::TableName;

/// The stage loads use, as SQL: `@"DB"."SCHEMA"."_SQLLOAD_STAGE"` in the target's loader schema, or `@` and the existing stage's name.
pub fn stage_reference(stage: &ExternalStage) -> String {
    match &stage.object {
        StageObject::Managed { .. } => {
            let managed = TableName {
                table: MANAGED_STAGE_NAME.to_owned(),
                ..stage.home.clone()
            };
            format!("@{}", qualified_name(&managed, Warehouse::Snowflake))
        }
        StageObject::Existing { name } => format!("@{name}"),
    }
}
