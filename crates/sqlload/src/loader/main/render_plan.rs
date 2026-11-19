//! Print a plan in the `sqb plan` style.

use crate::loader::_helpers::render::{
    load_item, merged_errors, reads_text, text_item, text_view, tree, usual_details, view_lines,
};
use crate::loader::_helpers::resolve::display_name;
use crate::loader::models::{PlanItem, ProjectPlan, TablePlan};
use crate::plan_text::main::plan_lines::plan_lines;
use crate::plan_text::models::{LoadedTable, PlanParts};

/// The plan's lines, and whether every selected table can load: each `http` connection's settings, then only the lines a table has of its own or that ask for something (see [`plan_lines`]), or with `verbose` every line; a plan of only project problems shows just its `Errors`, and tables stopped for the same reasons share one item.
pub fn render_plan(plan: &ProjectPlan, verbose: bool) -> (Vec<String>, bool) {
    let mut view = text_view(plan);
    view.errors = merged_errors(std::mem::take(&mut view.errors));
    if verbose {
        return (
            with_http_lines(view_lines(&view), &plan.http, true),
            view.ok,
        );
    }
    let ready: Vec<&TablePlan> = plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
        .collect();
    let items: Vec<PlanItem> = ready.iter().map(|table| load_item(table)).collect();
    let usual = usual_details(&ready, &items);
    let tables: Vec<LoadedTable<'_>> = ready
        .iter()
        .zip(items)
        .map(|(table, item)| LoadedTable {
            table,
            name: display_name(table.spec.warehouse, &table.spec.destination),
            item: text_item(table, item, false, &usual),
            reads: reads_text(table, false),
        })
        .collect();
    let mut problems: Vec<String> = tree("Errors", "", &view.errors);
    problems.extend(tree("Warnings", "", &view.warnings));
    let parts = PlanParts {
        view: &view,
        tables,
        problems,
    };
    (
        with_http_lines(plan_lines(&parts), &plan.http, false),
        view.ok,
    )
}

/// `lines` with each `http` connection's line: under `-v` (`verbose`) after the `Plan ready`, `to` and `memory` lines, otherwise first, apart from the rest.
fn with_http_lines(mut lines: Vec<String>, http: &[String], verbose: bool) -> Vec<String> {
    let at = match verbose {
        true => lines.iter().position(String::is_empty).unwrap_or_default(),
        false => 0,
    };
    let gap = (!verbose && !http.is_empty()).then(String::new);
    lines.splice(at..at, http.iter().cloned().chain(gap));
    lines
}
