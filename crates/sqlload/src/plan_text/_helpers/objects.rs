//! The `Objects` section's items, one line each.

use crate::loader::models::{PlanDetail, PlanItem};
use crate::plan_text::constants::{CREATE_TABLE, LOAD_COLUMNS, NAMED_COLUMNS, RECREATE_TABLE};

/// An `Objects` item on one line: a new or rebuilt table's columns, its `_sqlload_*` ones too, counted rather than listed, as `load` counts them, the columns it leaves out or masks still named; any other object, a Python table's first-load create among them, as `-v` shows it.
pub(crate) fn object_line(item: &PlanItem) -> PlanItem {
    if item.action != CREATE_TABLE && !item.action.starts_with(RECREATE_TABLE) {
        return item.clone();
    }
    let mut named: Vec<PlanDetail> = Vec::new();
    let mut columns = 0;
    for detail in &item.details {
        if NAMED_COLUMNS
            .iter()
            .any(|start| detail.text.starts_with(start))
        {
            named.push(detail.clone());
        } else if let Some(names) = detail.text.strip_prefix(LOAD_COLUMNS) {
            columns += names.split(", ").count();
        } else {
            columns += 1;
        }
    }
    let action = match columns {
        0 => item.action.clone(),
        1 => format!("{}, 1 column", item.action),
        count => format!("{}, {count} columns", item.action),
    };
    PlanItem {
        name: item.name.clone(),
        action,
        details: named,
    }
}
