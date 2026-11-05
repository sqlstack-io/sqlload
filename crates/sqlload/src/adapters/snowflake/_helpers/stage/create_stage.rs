use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::constants::MANAGED_STAGE_NAME;
use crate::adapters::snowflake::models::SnowflakeSql;
use crate::adapters::snowflake::models::{ExternalStage, StageObject};

/// `CREATE STAGE IF NOT EXISTS` for `_SQLLOAD_STAGE` in the target's loader schema on the stage URL; `None` for an existing stage, which SQLLoad never creates.
pub(crate) fn create_stage(stage: &ExternalStage) -> Option<String> {
    let StageObject::Managed { integration } = &stage.object else {
        return None;
    };
    let managed = TableName {
        table: MANAGED_STAGE_NAME.to_owned(),
        ..stage.home.clone()
    };
    Some(format!(
        "CREATE STAGE IF NOT EXISTS {} URL = {} STORAGE_INTEGRATION = {}",
        SnowflakeSql.render_qualified_name(&managed),
        SnowflakeSql.render_loader_value_literal(&stage.location.to_string()),
        SnowflakeSql.render_identifier(integration)
    ))
}
