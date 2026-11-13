//! Load every unblocked table of a plan, reporting each table's outcome.

use tokio::runtime::Handle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::cdc::main::follow_cdc::follow_cdc;
use crate::loader::_helpers::destination::clean_stages;
use crate::loader::_helpers::destination::clean_staging;
use crate::loader::_helpers::table_queue::{TableQueue, load_task, shared_run, thread_count};
use crate::loader::models::{LoadRun, ProjectPlan, TableOutcome};

/// Load each unblocked table, Python tables through `host`, each on a thread of its own, up to `concurrency` at once (one table's batches commit one at a time), so tables use several cores (decoding, masking and metadata included) and a slow one never holds up the others; tables of a Python source sharing a client run one at a time unless it sets `parallel_tables`. A table that fails is reported and the others still load, unless `--fail-fast` starts no table after it (those not started, `postgres_cdc` sources' included, are reported skipped); outcomes are in plan order. Load folders and staging tables crashed runs left are removed first. `postgres_cdc` sources load after the other tables, each as one unit. Given `follow` (`load --follow`), the plan's one `postgres_cdc` source is followed instead until it is cancelled (see [`follow_cdc`]).
pub async fn load_tables(
    run: &LoadRun<'_>,
    plan: &ProjectPlan,
    follow: Option<&CancellationToken>,
) -> Vec<TableOutcome> {
    clean_stages(run, plan).await;
    clean_staging(run, plan).await;
    if let Some(stop) = follow {
        return follow_cdc(run, plan.clone(), stop).await;
    }
    let captured = plan;
    let plan = &plan.uncaptured();
    let threads = thread_count(run);
    let shared = shared_run(run);
    let mut queue = TableQueue::new(run.project, plan, run.options.fail_fast);
    let mut running: JoinSet<TableOutcome> = JoinSet::new();
    loop {
        while running.len() < threads {
            let Some((index, table)) = queue.next() else {
                break;
            };
            let (shared, table, handle) = (shared.clone(), table.clone(), Handle::current());
            let task = running.spawn_blocking(move || handle.block_on(load_task(shared, table)));
            queue.started(task.id(), index);
        }
        let Some(joined) = running.join_next_with_id().await else {
            break;
        };
        queue.finish(joined);
    }
    let cdc = queue.cdc_outcomes(run, captured).await;
    let mut outcomes = queue.outcomes();
    outcomes.extend(cdc);
    outcomes
}
