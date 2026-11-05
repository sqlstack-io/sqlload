//! Find a stage object.

use crate::column_types::models::Warehouse;
use crate::external_stage::constants::MANAGED_STAGE_NAME;
use crate::external_stage::models::StageLookup;
use crate::adapters::snowflake::models::{ExternalStage, StageObject};
use crate::adapters::contract::main::quoted_identifier::quoted_identifier;
use crate::adapters::contract::main::sql_literal::sql_literal;
use crate::adapters::contract::main::stored_name::stored_name;

/// `SHOW STAGES LIKE ... IN SCHEMA ...` for the stage loads use, and its name as reported.
pub fn stage_lookup(stage: &ExternalStage) -> StageLookup {
    let home = &stage.home;
    let snowflake = Warehouse::Snowflake;
    let (schema, name) = match &stage.object {
        StageObject::Managed { .. } => {
            let schema = match &home.database {
                Some(database) => format!(
                    "{}.{}",
                    quoted_identifier(database, snowflake),
                    quoted_identifier(&home.schema, snowflake)
                ),
                None => quoted_identifier(&home.schema, snowflake),
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
