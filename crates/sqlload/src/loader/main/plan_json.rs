//! `sqlload plan --json`.

use crate::loader::_helpers::plan_view::plan_view;
use crate::loader::models::ProjectPlan;

/// The plan as one indented JSON document, and whether every selected table can load.
pub fn plan_json(plan: &ProjectPlan) -> (String, bool) {
    let view = plan_view(plan);
    let text = serde_json::to_string_pretty(&view).unwrap_or_else(|error| {
        serde_json::json!({ "error": { "code": "json", "message": error.to_string(), "help": null } })
            .to_string()
    });
    (text, view.ok)
}
