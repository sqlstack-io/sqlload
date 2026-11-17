//! Create missing objects; never alter or drop.

use crate::delivery::models::Warehouse;
use crate::planning::errors::ApplyError;
use crate::planning::models::{Plan, PlannedObject};

/// Create every missing object, failing before any change when an existing object is incompatible.
pub async fn apply_plan(
    warehouse: &Warehouse,
    plan: &Plan,
) -> Result<Vec<PlannedObject>, ApplyError> {
    let problems = plan.problems();
    if !problems.is_empty() {
        return Err(ApplyError::Incompatible(problems));
    }
    let mut created: Vec<PlannedObject> = Vec::new();
    for object in plan.missing() {
        warehouse
            .execute(&object.create_sql)
            .await
            .map_err(|source| ApplyError::Create {
                name: object.name.clone(),
                source,
            })?;
        created.push(object.clone());
    }
    Ok(created)
}
