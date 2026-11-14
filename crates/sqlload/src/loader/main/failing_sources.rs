//! The sources a plan's errors come from.

use crate::loader::_helpers::render::problem_item;
use crate::loader::models::ProjectPlan;

/// The source of each error the plan stops on, without repeats: a blocked table's, the source a problem names (`[sources.<name>]...`), and each source recorded with a table problem. A problem with the project as a whole or a connection names none, as only fixing it gets past it.
pub fn failing_sources(plan: &ProjectPlan) -> Vec<String> {
    let mut sources: Vec<String> = Vec::new();
    let named = plan.problems.iter().filter_map(|problem| {
        let subject = problem_item(problem).name;
        let inner = subject.strip_prefix("[sources.")?;
        Some(inner.split(['.', ']']).next().unwrap_or(inner).to_owned())
    });
    let blocked = plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_some())
        .map(|table| table.spec.source_name.clone());
    for source in blocked
        .chain(named)
        .chain(plan.failed_sources.iter().cloned())
    {
        if !sources.contains(&source) {
            sources.push(source);
        }
    }
    sources
}
