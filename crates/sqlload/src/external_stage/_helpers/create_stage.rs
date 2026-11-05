use crate::column_types::models::Warehouse;
use crate::external_stage::constants::MANAGED_STAGE_NAME;
use crate::snowflake_destination::models::{ExternalStage, StageObject};
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::main::quoted_identifier::quoted_identifier;
use crate::write_sql::main::sql_literal::sql_literal;
use crate::write_sql::models::TableName;

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
