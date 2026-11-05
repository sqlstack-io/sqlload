//! Load every unblocked table of a plan, reporting each table's outcome.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::task::JoinSet;

use crate::loader::_helpers::destination::{clean_stage, prepare_schemas};
use crate::loader::constants::DEFAULT_THREADS;
use crate::loader::main::_load_table::load_one;
use crate::loader::models::{LoadRun, ProjectPlan, SharedRun, TableOutcome, TablePlan};
use crate::write_sql::main::display_name::display_name;

/// Load each unblocked table, Python tables through `host`, each on a thread of its own, up to `threads` at once (one table's batches commit one at a time), so tables use several cores (decoding, masking and metadata included) and a slow one never holds up the others; tables of a Python source sharing a client run one at a time unless it sets `parallel_tables`. A table that fails is reported and the others still load; outcomes are in plan order. Load folders and staging tables crashed runs left are removed first.
pub async fn load_tables(run: &LoadRun<'_>, plan: &ProjectPlan) -> Vec<TableOutcome> {
    for destination in run.destinations.values() {
        clean_stage(destination).await;
    }
    prepare_schemas(run, plan).await;
    let threads = run
        .options
        .threads
        .or(run.project.threads)
        .unwrap_or(DEFAULT_THREADS)
        .max(1);
    let shared = Arc::new(SharedRun {
        project: run.project.clone(),
        destinations: run.destinations.clone(),
        host: run.host.clone(),
        options: run.options.clone(),
    });
    let tables: Vec<&TablePlan> = plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
        .collect();
    let mut waiting: Vec<usize> = (0..tables.len()).collect();
    let mut busy: BTreeSet<&str> = BTreeSet::new();
    let mut running: JoinSet<TableOutcome> = JoinSet::new();
    let mut started: BTreeMap<tokio::task::Id, usize> = BTreeMap::new();
    let mut outcomes: Vec<(usize, TableOutcome)> = Vec::with_capacity(tables.len());
    loop {
        while running.len() < threads {
            let free = waiting
                .iter()
                .position(|index| !busy.contains(tables[*index].spec.source_name.as_str()));
            let Some(position) = free else {
                break;
            };
            let index = waiting.remove(position);
            let table = tables[index];
            if one_at_a_time(run, table) {
                busy.insert(table.spec.source_name.as_str());
            }
            let (shared, table, handle) = (shared.clone(), table.clone(), Handle::current());
            let task = running.spawn_blocking(move || handle.block_on(load_task(shared, table)));
            started.insert(task.id(), index);
        }
        let Some(joined) = running.join_next_with_id().await else {
            break;
        };
        let (id, outcome) = match joined {
            Ok((id, outcome)) => (id, Ok(outcome)),
            Err(error) => (error.id(), Err(error.to_string())),
        };
        let Some(index) = started.remove(&id) else {
            continue;
        };
        let table = tables[index];
        busy.remove(table.spec.source_name.as_str());
        outcomes.push((
            index,
            outcome.unwrap_or_else(|reason| TableOutcome {
                table: display_name(&table.spec.destination, table.spec.warehouse),
                loaded: Err(reason),
            }),
        ));
    }
    outcomes.sort_by_key(|(index, _)| *index);
    outcomes.into_iter().map(|(_, outcome)| outcome).collect()
}

/// Whether the table's source shares a client its tables must not use at the same time.
fn one_at_a_time(run: &LoadRun<'_>, table: &TablePlan) -> bool {
    run.project
        .sources
        .get(&table.spec.source_name)
        .is_some_and(|source| source.shared_client && !source.parallel_tables)
}

/// Load one table, with everything it needs owned.
async fn load_task(shared: Arc<SharedRun>, table: TablePlan) -> TableOutcome {
    let run = LoadRun {
        project: &shared.project,
        destinations: &shared.destinations,
        host: shared.host.clone(),
        options: &shared.options,
    };
    match load_one(&run, &table).await {
        Ok((name, loaded)) => TableOutcome {
            table: name,
            loaded: Ok(loaded),
        },
        Err(error) => TableOutcome {
            table: display_name(&table.spec.destination, table.spec.warehouse),
            loaded: Err(error.to_string()),
        },
    }
}
