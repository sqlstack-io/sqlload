//! Load every unblocked table of a plan, reporting each table's outcome.

use crate::loader::main::_load_table::load_one;
use crate::loader::models::{LoadRun, ProjectPlan, TableOutcome};
use crate::write_sql::main::display_name::display_name;

/// Load each unblocked table, Python tables through `host`; a table that fails is reported and the others still load.
pub async fn load_tables(run: &LoadRun<'_>, plan: &ProjectPlan) -> Vec<TableOutcome> {
    let mut outcomes: Vec<TableOutcome> = Vec::new();
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
    {
        let outcome = match load_one(run, table).await {
            Ok((name, rows)) => TableOutcome {
                table: name,
                loaded: Ok(rows),
            },
            Err(error) => TableOutcome {
                table: display_name(&table.spec.destination, table.spec.warehouse),
                loaded: Err(error.to_string()),
            },
        };
        outcomes.push(outcome);
    }
    outcomes
}
