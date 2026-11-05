//! Name a stage in SQL.

use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::constants::MANAGED_STAGE_NAME;
use crate::adapters::snowflake::models::SnowflakeSql;
use crate::adapters::snowflake::models::{ExternalStage, StageObject};

/// The stage loads use, as SQL: `@"DB"."SCHEMA"."_SQLLOAD_STAGE"` in the target's loader schema, or `@` and the existing stage's name.
pub(in crate::adapters) fn stage_reference(stage: &ExternalStage) -> String {
    match &stage.object {
        StageObject::Managed { .. } => {
            let managed = TableName {
                table: MANAGED_STAGE_NAME.to_owned(),
                ..stage.home.clone()
            };
            format!("@{}", SnowflakeSql.render_qualified_name(&managed))
        }
        StageObject::Existing { name } => format!("@{name}"),
    }
}
