//! Load every unblocked table of a plan, reporting each table's outcome.

use tokio_util::sync::CancellationToken;

use crate::loader::_helpers::destination::clean_stages;
use crate::loader::_helpers::destination::clean_staging;
use crate::loader::_helpers::table_queue::{
    TableQueue, follow_tables, shared_run, thread_count, watch_load,
};
use crate::loader::models::{LoadRun, ProjectPlan, TableOutcome};

/// Load each unblocked table, Python tables through `host`, each on a thread of its own, up to `concurrency` at once, or fewer when the memory budget's table shares don't fit that many (one table's batches commit one at a time), none starting while the watchdog holds the load at 80% of its budget, so tables use several cores (decoding, masking and metadata included) and a slow one never holds up the others; tables of a Python source sharing a client run one at a time unless it sets `parallel_tables`. A table that fails is reported and the others still load, unless `--fail-fast` starts no table after it (those not started, `postgres_cdc` sources' included, are reported skipped); outcomes are in plan order. Load folders and staging tables crashed runs left are removed first. `postgres_cdc` sources load after the other tables, each as one unit. Given `follow` (`load --follow`), the plan's one `postgres_cdc` source is followed instead until it is cancelled (see [`follow_tables`]).
pub async fn load_tables(
    run: &LoadRun<'_>,
    plan: &ProjectPlan,
    follow: Option<&CancellationToken>,
) -> Vec<TableOutcome> {
    clean_stages(run, plan).await;
    clean_staging(run, plan).await;
    if let Some(stop) = follow {
        return follow_tables(run, plan, stop).await;
    }
    let captured = plan;
    let plan = &plan.uncaptured();
    let guard = watch_load(run);
    let threads = thread_count(run);
    let shared = shared_run(run, guard.clone());
    let mut queue = TableQueue::new(run.project, plan, run.options.fail_fast);
    queue.load_all(threads, (shared, guard)).await;
    let cdc = queue.cdc_outcomes(run, captured).await;
    let mut outcomes = queue.outcomes();
    outcomes.extend(cdc);
    outcomes
}
