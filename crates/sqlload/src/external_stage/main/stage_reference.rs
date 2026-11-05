//! Name a stage in SQL.

use crate::column_types::models::Warehouse;
use crate::external_stage::constants::MANAGED_STAGE_NAME;
use crate::snowflake_destination::models::{ExternalStage, StageObject};
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::models::TableName;

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
