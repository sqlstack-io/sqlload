//! Refuse writes that would touch another table's rows where tables with `constant_columns` share one destination table.

use crate::adapters::contract::models::WriteMode;
use crate::loader::_helpers::table_plan::blocked_plan;
use crate::loader::models::{ProjectPlan, TablePlan, TableSpec};
use crate::project::models::WriteStrategy;

/// The plan with every selected table blocked that shares its destination table with another configured table (selected or not) when either sets `constant_columns` and the load could remove or overwrite the other's rows: either uses `replace` or `delete_insert`, this run reloads it, it keeps its position in the run log (which is per destination table), or it merges on a `unique_key` without a constant column.
pub(in crate::loader) fn block_shared_destinations(
    mut plan: ProjectPlan,
    configured: &[TableSpec],
) -> ProjectPlan {
    for table in plan
        .tables
        .iter_mut()
        .filter(|table| table.schema.blocked.is_none())
    {
        let others: Vec<&TableSpec> = configured
            .iter()
            .filter(|other| shares_destination(&table.spec, other))
            .collect();
        let opted_in = !table.spec.constants.is_empty()
            || others.iter().any(|other| !other.constants.is_empty());
        if others.is_empty() || !opted_in {
            continue;
        }
        let reasons = shared_problems(table, &others);
        if !reasons.is_empty() {
            table.schema = blocked_plan(reasons);
        }
    }
    plan
}

/// Whether `other` is a different table loading into `spec`'s destination table.
fn shares_destination(spec: &TableSpec, other: &TableSpec) -> bool {
    other.target == spec.target
        && other.destination == spec.destination
        && (other.source_name != spec.source_name
            || other.source_table.qualified_name() != spec.source_table.qualified_name())
}

/// Why `table` cannot load into a destination table `others` also load.
fn shared_problems(table: &TablePlan, others: &[&TableSpec]) -> Vec<String> {
    let spec = &table.spec;
    let names: Vec<String> = others
        .iter()
        .map(|other| {
            format!(
                "{}.{}",
                other.source_name,
                other.source_table.qualified_name()
            )
        })
        .collect();
    let place = format!(
        "{} is also loaded by {}, and constant_columns tell their rows apart",
        spec.destination.table,
        names.join(", ")
    );
    let mut problems: Vec<String> = Vec::new();
    let removes = std::iter::once(spec)
        .chain(others.iter().copied())
        .find(|each| {
            matches!(
                each.strategy,
                WriteStrategy::Replace | WriteStrategy::DeleteInsert
            )
        });
    if let Some(removing) = removes {
        problems.push(format!(
            "{place}; {}.{} uses write_strategy = \"{}\", which would remove the other tables' rows: use merge on a unique_key that includes a constant column, or append",
            removing.source_name,
            removing.source_table.qualified_name(),
            removing.strategy
        ));
    }
    if table.mode == WriteMode::Recreate {
        problems.push(format!(
            "{place}; --reload would drop the other tables' rows"
        ));
    }
    if spec.logged {
        problems.push(format!(
            "{place}; state = \"logged\" keeps one position per destination table, which they would share"
        ));
    }
    let keyed = spec.unique_key.iter().any(|key| {
        spec.constants
            .iter()
            .any(|(name, _, _)| name.eq_ignore_ascii_case(key))
    });
    if spec.strategy == WriteStrategy::Merge && !keyed {
        problems.push(format!(
            "{place}; its unique_key ({}) has no constant column, so its merge could overwrite their rows: add one of its constant columns to it",
            spec.unique_key.join(", ")
        ));
    }
    problems
}
