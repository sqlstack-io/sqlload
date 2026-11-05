//! Name a stage in SQL.

use crate::column_types::models::Warehouse;
use crate::external_stage::constants::MANAGED_STAGE_NAME;
use crate::snowflake_destination::models::{ExternalStage, StageObject};
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::models::TableName;

/// The stage loads into `table` use, as SQL: `@"DB"."SCHEMA"."_SQLLOAD_STAGE"` in the table's schema, or `@` and the existing stage's name.
pub fn stage_reference(stage: &ExternalStage, table: &TableName) -> String {
    match &stage.object {
        StageObject::Managed { .. } => {
            let managed = TableName {
                table: MANAGED_STAGE_NAME.to_owned(),
                ..table.clone()
            };
            format!("@{}", qualified_name(&managed, Warehouse::Snowflake))
        }
        StageObject::Existing { name } => format!("@{name}"),
    }
}
