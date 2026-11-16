use crate::batch_plan::_helpers::rules::{null_rule, range_rule, zone_rule};
use crate::loader::models::TablePlan;

/// `table`'s cursor `line` for `plan` with its rules after it: what it does with rows whose cursor is NULL, in brackets (with `nulls`); for a Python table, what it does with rows its function returns outside a window; and the zone times without an offset are read in, with where it comes from.
pub fn cursor_rules(table: &TablePlan, line: &str, nulls: bool) -> String {
    let mut text = match null_rule(table).filter(|_| nulls) {
        Some(rule) => format!("{line} ({rule})"),
        None => line.to_owned(),
    };
    if let Some(rule) = range_rule(table) {
        text.push_str(&format!("; {rule}"));
    }
    if let Some((zone, from)) = zone_rule(table) {
        text.push_str(&format!("; times without an offset in {zone} ({from})"));
    }
    text
}
