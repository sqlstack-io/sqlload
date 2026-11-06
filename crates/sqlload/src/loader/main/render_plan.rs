//! Print a plan in the `sqb plan` style.

use crate::loader::_helpers::render::{merged_errors, plan_view, view_lines};
use crate::loader::models::ProjectPlan;

/// The plan's lines, and whether every selected table can load; a plan of only project problems shows just its `Errors`, and tables stopped for the same reasons share one item.
pub fn render_plan(plan: &ProjectPlan) -> (Vec<String>, bool) {
    let mut view = plan_view(plan);
    view.errors = merged_errors(std::mem::take(&mut view.errors));
    (view_lines(&view), view.ok)
}
