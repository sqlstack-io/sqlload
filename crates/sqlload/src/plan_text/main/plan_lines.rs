//! Print a plan with only the lines that are a table's own or ask for something.

use crate::loader::models::{PlanItem, PlanView};
use crate::plan_text::_helpers::loads::loads_section;
use crate::plan_text::_helpers::objects::object_line;
use crate::plan_text::_helpers::skipped::skipped_section;
use crate::plan_text::_helpers::tree::section;
use crate::plan_text::models::PlanParts;

/// The plan's lines without `-v`: the run's memory budget when it asks for something (no budget, or `concurrency` lowered to fit); what `apply` creates, one line per object; what `load` loads, grouped by source under the targets, one line per table with its write strategy, key and where its read starts, and child lines only for what that table does unlike the defaults or should have looked at; the skipped tables grouped by reason; the errors and warnings as `-v` shows them; then whether the plan is complete.
pub fn plan_lines(parts: &PlanParts<'_>) -> Vec<String> {
    let objects: Vec<PlanItem> = parts.view.objects.iter().map(object_line).collect();
    let mut lines: Vec<String> = (parts.view.memory.iter())
        .filter(|_| parts.view.memory_asks)
        .flat_map(|memory| [format!("Memory  {memory}"), String::new()])
        .collect();
    lines.extend(section("Objects", "sqlload apply", &objects));
    lines.extend(loads_section(&parts.tables));
    lines.extend(skipped_section(&parts.view.skipped));
    lines.extend(parts.problems.iter().cloned());
    lines.push(closing_line(parts.view));
    lines
}

/// `\u{2713} Plan complete  15 to load, 7 skipped`, or `\u{2717} Plan failed  2 to load, 1 error`: the plan's counts after how many tables it selected.
fn closing_line(view: &PlanView) -> String {
    let mut clauses: Vec<String> = view
        .summary
        .split(", ")
        .skip(1)
        .map(str::to_owned)
        .collect();
    if !view.ok {
        clauses.push(match view.errors.len() {
            1 => "1 error".to_owned(),
            count => format!("{count} errors"),
        });
    }
    let summary = match clauses.is_empty() {
        true => String::new(),
        false => format!("  {}", clauses.join(", ")),
    };
    match view.ok {
        true => format!("\u{2713} Plan complete{summary}"),
        false => format!("\u{2717} Plan failed{summary}"),
    }
}
