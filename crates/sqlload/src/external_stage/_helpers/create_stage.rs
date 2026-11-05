use crate::column_types::models::Warehouse;
use crate::external_stage::constants::MANAGED_STAGE_NAME;
use crate::adapters::snowflake::models::{ExternalStage, StageObject};
use crate::adapters::contract::main::qualified_name::qualified_name;
use crate::adapters::contract::main::quoted_identifier::quoted_identifier;
use crate::adapters::contract::main::sql_literal::sql_literal;
use crate::adapters::contract::models::TableName;

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
        qualified_name(&managed, Warehouse::Snowflake),
        sql_literal(&stage.location.to_string(), Warehouse::Snowflake),
        quoted_identifier(integration, Warehouse::Snowflake)
    ))
}
