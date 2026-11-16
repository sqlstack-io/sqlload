//! Tree lines, as the plan draws them.

use crate::loader::models::{PlanDetail, PlanItem};
use crate::plan_text::constants::HINT_COLUMN;

/// The branch drawn before a node and the indent below it: the last of its siblings closes the tree.
pub(crate) fn marks(last: bool) -> (&'static str, &'static str) {
    match last {
        true => ("\u{2514}\u{2500}\u{2500} ", "    "),
        false => ("\u{251c}\u{2500}\u{2500} ", "\u{2502}   "),
    }
}

/// `heading` with `hint` from [`HINT_COLUMN`] on, or two spaces after a longer heading.
pub(crate) fn hinted(heading: &str, hint: &str) -> String {
    let width = HINT_COLUMN.max(heading.chars().count() + 2);
    format!("{heading:<width$}{hint}")
}

/// One tree node, `line`, under `indent`, with its `details` as children: each its label, two spaces and its text, the text's later lines under its first.
pub(crate) fn branch(line: &str, details: &[PlanDetail], last: bool, indent: &str) -> Vec<String> {
    let (mark, below) = marks(last);
    let mut lines: Vec<String> = vec![format!("{indent}{mark}{line}")];
    for (index, detail) in details.iter().enumerate() {
        let (sub, continued) = marks(index + 1 == details.len());
        let label = match detail.label.is_empty() {
            true => String::new(),
            false => format!("{}  ", detail.label),
        };
        let width = label.chars().count();
        for (line_index, text) in detail.text.split('\n').enumerate() {
            lines.push(match line_index {
                0 => format!("{indent}{below}{sub}{label}{text}"),
                _ => format!("{indent}{below}{continued}{:width$}{text}", ""),
            });
        }
    }
    lines
}

/// `items` as a section: `heading (count)` with `hint`, then each item's name and action two spaces apart and its details below it, then a blank line; nothing without items.
pub(crate) fn section(heading: &str, hint: &str, items: &[PlanItem]) -> Vec<String> {
    if items.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<String> = vec![hinted(&format!("{heading} ({})", items.len()), hint)];
    for (index, item) in items.iter().enumerate() {
        let line = match item.action.is_empty() {
            true => item.name.clone(),
            false => format!("{}  {}", item.name, item.action),
        };
        lines.extend(branch(&line, &item.details, index + 1 == items.len(), ""));
    }
    lines.push(String::new());
    lines
}
