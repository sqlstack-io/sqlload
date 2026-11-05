//! A plan in sections, and those sections as the lines `plan` prints.

use crate::adapters::snowflake::models::PlannedStage;
use crate::loader::_helpers::render::{
    blocked_item, error_item, load_item, object_item, problem_item, stage_item, tree, warning_item,
};
use crate::loader::constants::{BOLD, DIM, GREEN, RED, RESET, YELLOW};
use crate::loader::models::{PlanItem, PlanView, ProjectPlan};
use crate::schema_plan::models::TableAction;

/// `count` and the noun for it, as `1 object` or `2 objects`.
fn counted(count: usize, (one, many): (&str, &str)) -> String {
    match count {
        1 => format!("1 {one}"),
        _ => format!("{count} {many}"),
    }
}

/// The plan's sections: what `apply` creates or changes, what `load` moves, and what stops or should be known.
pub(crate) fn plan_view(plan: &ProjectPlan) -> PlanView {
    let ready: Vec<_> = plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
        .collect();
    let mut stages: Vec<&PlannedStage> = Vec::new();
    for table in &ready {
        if let Some(stage) = &table.planned_stage
            && !stages.iter().any(|seen| seen.name == stage.name)
        {
            stages.push(stage);
        }
    }
    let changed: Vec<_> = ready
        .iter()
        .filter(|table| table.schema.action != TableAction::Unchanged && table.output.is_none())
        .collect();
    let creates = stages.len()
        + changed
            .iter()
            .filter(|table| {
                matches!(
                    table.schema.action,
                    TableAction::Create | TableAction::Recreate
                )
            })
            .count();
    let alters = stages.len() + changed.len() - creates;
    let errors: Vec<PlanItem> = plan
        .tables
        .iter()
        .filter_map(blocked_item)
        .chain(plan.problems.iter().map(|problem| problem_item(problem)))
        .collect();
    let clauses = [
        (creates, ("object to create", "objects to create")),
        (alters, ("to alter", "to alter")),
        (ready.len(), ("to load", "to load")),
        (plan.skipped.len(), ("skipped", "skipped")),
    ];
    let mut summary = vec![format!("{} selected", plan.tables.len())];
    summary.extend(
        clauses
            .into_iter()
            .filter(|(count, _)| *count > 0)
            .map(|(count, words)| counted(count, words)),
    );
    PlanView {
        ok: errors.is_empty(),
        summary: summary.join(", "),
        selected: plan.tables.len(),
        objects: stages
            .iter()
            .map(|stage| stage_item(stage))
            .chain(changed.iter().map(|table| object_item(table)))
            .collect(),
        loads: ready.iter().map(|table| load_item(table)).collect(),
        skipped: plan
            .skipped
            .iter()
            .map(|skipped| PlanItem {
                name: skipped.source_table.clone(),
                action: String::new(),
                details: vec![crate::loader::models::PlanDetail {
                    label: "reason".to_owned(),
                    text: skipped.reason.clone(),
                }],
            })
            .collect(),
        errors,
        warnings: plan
            .warnings
            .iter()
            .map(|warning| error_item(warning, &[]))
            .chain(ready.iter().filter_map(|table| warning_item(table)))
            .collect(),
    }
}

/// The lines `plan` prints for `view`: `Plan ready`, each section with items, then whether the plan is complete.
pub(crate) fn view_lines(view: &PlanView) -> Vec<String> {
    let mut lines = vec![format!("Plan ready  {}", view.summary), String::new()];
    lines.extend(tree("Objects", "sqlload apply", &view.objects));
    lines.extend(tree("Loads", "sqlload load", &view.loads));
    lines.extend(tree("Skipped", "", &view.skipped));
    lines.extend(tree("Errors", "", &view.errors));
    lines.extend(tree("Warnings", "", &view.warnings));
    lines.push(match view.ok {
        true => format!("✓ Plan complete  {}", view.summary),
        false => format!("✗ Plan failed with errors  {}", view.summary),
    });
    lines
}

/// Problems found before any plan, such as in the configuration, as a failed plan's `Errors`.
pub(crate) fn failed_lines(problems: &[String]) -> Vec<String> {
    let items: Vec<PlanItem> = problems
        .iter()
        .map(|problem| problem_item(problem))
        .collect();
    let mut lines = tree("Errors", "", &items);
    lines.push("✗ Plan failed with errors".to_owned());
    lines
}

/// `lines` with ANSI colours: bold headings, red errors and failures, yellow warnings, green completions and dim command notes.
pub(crate) fn paint(lines: Vec<String>) -> Vec<String> {
    lines
        .into_iter()
        .map(|line| {
            let heading = ["Objects (", "Loads (", "Skipped ("]
                .iter()
                .any(|start| line.starts_with(start));
            let painted = match line.split_once("  ") {
                _ if line.starts_with('✓') => Some(format!("{GREEN}{line}{RESET}")),
                _ if line.starts_with('✗') || line.starts_with("Errors (") => {
                    Some(format!("{RED}{line}{RESET}"))
                }
                _ if line.starts_with("Warnings (") => Some(format!("{YELLOW}{line}{RESET}")),
                Some((left, right)) if heading => {
                    let note = right.trim_start();
                    let gap = " ".repeat(line.len() - left.len() - note.len());
                    Some(format!("{BOLD}{left}{RESET}{gap}{DIM}{note}{RESET}"))
                }
                _ if heading || line.starts_with("Plan ready") => {
                    Some(format!("{BOLD}{line}{RESET}"))
                }
                _ if line.starts_with("failed ") => Some(format!("{RED}{line}{RESET}")),
                _ => None,
            };
            painted.unwrap_or(line)
        })
        .collect()
}
