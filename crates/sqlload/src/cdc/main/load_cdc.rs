//! Load the plan's `postgres_cdc` sources, each as one unit: its tables share one slot, which confirms a batch only once all of them have written it.

use crate::cdc::_helpers::stream::{captured_sources, load_source, source_outcomes};
use crate::loader::models::{LoadRun, ProjectPlan, TableOutcome};

/// Load each `postgres_cdc` source of `plan` in turn; a source with a blocked table does not load at all, since its slot would skip that table's changes. Every unblocked table of a source reports the source's error when it fails.
pub async fn load_cdc(run: &LoadRun<'_>, plan: &ProjectPlan) -> Vec<TableOutcome> {
    let mut outcomes: Vec<TableOutcome> = Vec::new();
    for (name, (tables, blocked)) in captured_sources(plan) {
        let started = std::time::Instant::now();
        let loaded = match blocked.is_empty() {
            true => load_source(run, name, &tables).await,
            false => Err(format!(
                "source {name} does not load while {} {} blocked: its tables share one slot, so loading the rest would skip the blocked tables' changes",
                blocked.join(", "),
                match blocked.len() {
                    1 => "is",
                    _ => "are",
                }
            )),
        };
        outcomes.extend(source_outcomes(&tables, &loaded, started.elapsed()));
    }
    outcomes
}
