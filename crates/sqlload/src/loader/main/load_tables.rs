//! Load every unblocked table of a plan, reporting each table's outcome.

use futures_util::StreamExt;

use crate::loader::_helpers::destination::clean_stage;
use crate::loader::constants::DEFAULT_THREADS;
use crate::loader::main::_load_table::load_one;
use crate::loader::models::{LoadRun, ProjectPlan, TableOutcome};
use crate::write_sql::main::display_name::display_name;

/// Load each unblocked table, Python tables through `host`, up to `threads` tables at once (one table's batches commit one at a time); a table that fails is reported and the others still load. Load folders failed runs left on a stage are removed first.
pub async fn load_tables(run: &LoadRun<'_>, plan: &ProjectPlan) -> Vec<TableOutcome> {
    for destination in run.destinations.values() {
        clean_stage(destination).await;
    }
    let threads = run
        .options
        .threads
        .or(run.project.threads)
        .unwrap_or(DEFAULT_THREADS)
        .max(1);
    let tables = plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none());
    futures_util::stream::iter(tables)
        .map(async |table| match load_one(run, table).await {
            Ok((name, loaded)) => TableOutcome {
                table: name,
                loaded: Ok(loaded),
            },
            Err(error) => TableOutcome {
                table: display_name(&table.spec.destination, table.spec.warehouse),
                loaded: Err(error.to_string()),
            },
        })
        .buffered(threads)
        .collect()
        .await
}
