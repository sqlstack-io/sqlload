//! Find a stage object.

use crate::column_types::models::Warehouse;
use crate::external_stage::constants::MANAGED_STAGE_NAME;
use crate::external_stage::models::StageLookup;
use crate::snowflake_destination::models::{ExternalStage, StageObject};
use crate::write_sql::main::quoted_identifier::quoted_identifier;
use crate::write_sql::main::sql_literal::sql_literal;
use crate::write_sql::main::stored_name::stored_name;
use crate::write_sql::models::TableName;

/// `SHOW STAGES LIKE ... IN SCHEMA ...` for the stage loads into `table` use, and its name as reported.
pub fn stage_lookup(stage: &ExternalStage, table: &TableName) -> StageLookup {
    let snowflake = Warehouse::Snowflake;
    let (schema, name) = match &stage.object {
        StageObject::Managed { .. } => {
            let schema = match &table.database {
                Some(database) => format!(
                    "{}.{}",
                    quoted_identifier(database, snowflake),
                    quoted_identifier(&table.schema, snowflake)
                ),
                None => quoted_identifier(&table.schema, snowflake),
            };
            (schema, stored_name(MANAGED_STAGE_NAME, snowflake))
        }
        StageObject::Existing { name } => match name.rsplit_once('.') {
            Some((schema, stage)) => (schema.to_owned(), stage.to_owned()),
            None => (String::new(), name.clone()),
        },
    };
    StageLookup {
        query: format!(
            "SHOW STAGES LIKE {} IN SCHEMA {schema}",
            sql_literal(&name, snowflake)
        ),
        name,
    }
}
