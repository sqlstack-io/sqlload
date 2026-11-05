//! Find a stage object.

use crate::adapters::dispatch::main::sql_adapter::sql_adapter;
use crate::adapters::snowflake::constants::MANAGED_STAGE_NAME;
use crate::adapters::snowflake::models::StageLookup;
use crate::adapters::snowflake::models::{ExternalStage, StageObject};
use crate::column_types::models::Warehouse;

/// `SHOW STAGES LIKE ... IN SCHEMA ...` for the stage loads use, and its name as reported.
pub(in crate::adapters) fn stage_lookup(stage: &ExternalStage) -> StageLookup {
    let home = &stage.home;
    let snowflake = Warehouse::Snowflake;
    let (schema, name) = match &stage.object {
        StageObject::Managed { .. } => {
            let schema = match &home.database {
                Some(database) => format!(
                    "{}.{}",
                    sql_adapter(snowflake).render_identifier(database),
                    sql_adapter(snowflake).render_identifier(&home.schema)
                ),
                None => sql_adapter(snowflake).render_identifier(&home.schema),
            };
            (
                schema,
                sql_adapter(snowflake).stored_name(MANAGED_STAGE_NAME),
            )
        }
        StageObject::Existing { name } => match name.rsplit_once('.') {
            Some((schema, stage)) => (schema.to_owned(), stage.to_owned()),
            None => (String::new(), name.clone()),
        },
    };
    StageLookup {
        query: format!(
            "SHOW STAGES LIKE {} IN SCHEMA {schema}",
            sql_adapter(snowflake).render_loader_value_literal(&name)
        ),
        name,
    }
}
