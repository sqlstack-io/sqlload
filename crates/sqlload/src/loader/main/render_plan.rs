//! Print a plan in the `sqb plan` style.

use crate::loader::_helpers::render::{plan_view, view_lines};
use crate::loader::models::ProjectPlan;

/// The plan's lines, and whether every selected table can load; a plan of only project problems shows just its `Errors`.
pub fn render_plan(plan: &ProjectPlan) -> (Vec<String>, bool) {
    let view = plan_view(plan);
    (view_lines(&view), view.ok)
}
