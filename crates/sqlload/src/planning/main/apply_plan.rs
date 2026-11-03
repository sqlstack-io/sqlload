//! Create missing objects; never alter or drop.

use crate::planning::errors::ApplyError;
use crate::planning::models::{Plan, PlannedObject};
use crate::sql_api::main::execute_sql::execute_sql;
use crate::sql_api::models::SqlClient;

/// Create every missing object, failing before any change when an existing object is incompatible.
pub async fn apply_plan(sql: &SqlClient, plan: &Plan) -> Result<Vec<PlannedObject>, ApplyError> {
    let problems = plan.problems();
    if !problems.is_empty() {
        return Err(ApplyError::Incompatible(problems));
    }
    let mut created: Vec<PlannedObject> = Vec::new();
    for object in plan.missing() {
        execute_sql(sql, &object.create_sql)
            .await
            .map_err(|source| ApplyError::Create {
                name: object.name.clone(),
                source,
            })?;
        created.push(object.clone());
    }
    Ok(created)
}
