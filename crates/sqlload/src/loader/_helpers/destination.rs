use std::collections::{BTreeMap, BTreeSet};

use crate::adapters::dispatch::main::clean_stage::clean_stage;
use crate::adapters::dispatch::main::drop_stale_staging::drop_stale_staging;
use crate::loader::models::{LoadRun, ProjectPlan, TablePlan};

/// Before a load: drop the staging tables crashed loads left in the schemas it writes to. A failure only warns.
pub(crate) async fn clean_staging(run: &LoadRun<'_>, plan: &ProjectPlan) {
    let mut schemas: BTreeMap<(String, Option<String>, String), &TablePlan> = BTreeMap::new();
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none() && table.output.is_none())
    {
        let name = &table.spec.destination;
        schemas
            .entry((
                table.spec.target.clone(),
                name.database.clone(),
                name.schema.clone(),
            ))
            .or_insert(table);
    }
    for table in schemas.into_values() {
        let Some(destination) = run.destinations.get(&table.spec.target) else {
            continue;
        };
        let name = &table.spec.destination;
        match drop_stale_staging(destination, name, table.spec.warehouse).await {
            Ok(0) => {}
            Ok(dropped) => {
                tracing::info!(dropped, schema = %name.schema, "dropped staging tables crashed loads left")
            }
            Err(error) => {
                tracing::warn!(%error, schema = %name.schema, "cannot clean up old staging tables")
            }
        }
    }
}

/// Remove the load folders failed runs left, over a day ago, on the stages this plan's tables use. A failure only warns.
pub(crate) async fn clean_stages(run: &LoadRun<'_>, plan: &ProjectPlan) {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
    {
        let (Some(route), Some(destination)) =
            (&table.stage, run.destinations.get(&table.spec.target))
        else {
            continue;
        };
        let stage = (table.spec.target.as_str(), route, &table.spec.destination);
        let (checked, cleaned) = clean_stage(destination, stage, seen).await;
        seen = checked;
        match cleaned {
            None | Some((_, Ok(0))) => {}
            Some((stage, Ok(removed))) => {
                tracing::info!(removed, %stage, "removed load folders failed runs left on the stage")
            }
            Some((stage, Err(error))) => {
                tracing::warn!(%error, %stage, "cannot clean up the stage")
            }
        }
    }
}
