//! The JSON documents `plan --json`, `apply --json` and `load --json` print.

use serde::Serialize;

use crate::loader::_helpers::render::{plan_view, run_result};
use crate::loader::models::{ProjectPlan, RunReport};

/// The plan as one indented JSON document, or, with `run`, what `apply` or `load` did after it; and whether every selected table can load (and, after a run, loaded).
pub fn json_report(plan: &ProjectPlan, run: Option<&RunReport<'_>>) -> (String, bool) {
    match run {
        None => {
            let view = plan_view(plan);
            (pretty(&view), view.ok)
        }
        Some(run) => {
            let result = run_result(plan, run);
            (pretty(&result), result.ok)
        }
    }
}

/// `document` as indented JSON, or a JSON error saying why it cannot be.
fn pretty(document: &impl Serialize) -> String {
    serde_json::to_string_pretty(document).unwrap_or_else(|error| {
        serde_json::json!({ "error": { "code": "json", "message": error.to_string(), "help": null } })
            .to_string()
    })
}
