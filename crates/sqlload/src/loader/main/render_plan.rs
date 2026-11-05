//! Print a plan in the `sqb plan` style.

use crate::loader::_helpers::render::{
    blocked_lines, load_lines, object_lines, stage_object_lines, tree, warning_lines,
};
use crate::loader::models::{PlannedStage, ProjectPlan};
use crate::schema_plan::models::TableAction;

/// The plan's lines, and whether every selected table can load.
pub fn render_plan(plan: &ProjectPlan) -> (Vec<String>, bool) {
    let blocked: Vec<_> = plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_some())
        .collect();
    let ready: Vec<_> = plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
        .collect();
    let objects: Vec<_> = ready
        .iter()
        .filter(|table| table.schema.action != TableAction::Unchanged && table.output.is_none())
        .copied()
        .collect();
    let mut stages: Vec<&PlannedStage> = Vec::new();
    for table in &ready {
        if let Some(stage) = &table.planned_stage
            && !stages.iter().any(|seen| seen.name == stage.name)
        {
            stages.push(stage);
        }
    }
    let ok = blocked.is_empty() && plan.problems.is_empty();
    let summary = format!(
        "{} selected, {} objects to create or change, {} tables to load{}",
        plan.tables.len(),
        objects.len() + stages.len(),
        ready.len(),
        match blocked.len() {
            0 => String::new(),
            count => format!(", {count} blocked"),
        }
    );
    let mut lines = vec![format!("Plan ready  {summary}"), String::new()];
    lines.extend(tree(
        &format!("Objects ({})", objects.len() + stages.len()),
        "sqlload apply",
        stages
            .iter()
            .map(|stage| stage_object_lines(stage))
            .chain(objects.iter().map(|table| object_lines(table)))
            .collect(),
    ));
    lines.extend(tree(
        &format!("Loads ({})", ready.len()),
        "sqlload load",
        ready.iter().map(|table| load_lines(table)).collect(),
    ));
    lines.extend(tree(
        &format!("Skipped ({})", plan.skipped.len()),
        "",
        plan.skipped
            .iter()
            .map(|skipped| vec![format!("{}  {}", skipped.source_table, skipped.reason)])
            .collect(),
    ));
    lines.extend(tree(
        &format!("Blocked ({})", blocked.len()),
        "",
        blocked.iter().map(|table| blocked_lines(table)).collect(),
    ));
    lines.extend(tree(
        &format!("Problems ({})", plan.problems.len()),
        "",
        plan.problems
            .iter()
            .map(|problem| vec![problem.clone()])
            .collect(),
    ));
    let warnings: Vec<Vec<String>> = plan
        .warnings
        .iter()
        .map(|warning| vec![warning.clone()])
        .chain(ready.iter().flat_map(|table| warning_lines(table)))
        .collect();
    lines.extend(tree(
        &format!("Warnings ({})", warnings.len()),
        "",
        warnings,
    ));
    lines.push(match ok {
        true => format!("✓ Plan complete  {summary}"),
        false => format!("✗ Plan failed with errors  {summary}"),
    });
    (lines, ok)
}
