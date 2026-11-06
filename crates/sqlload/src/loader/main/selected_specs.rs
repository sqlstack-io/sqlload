//! The settings of every selected table, without reading any destination.

use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::_helpers::sources::{selected, source_tables, unmatched_selectors};
use crate::loader::errors::LoaderError;
use crate::loader::models::{PlanOptions, ResolvedTables};
use crate::project::models::ProjectConfig;

/// Each source's tables resolved to their settings and narrowed to the selection in `options`; a source that cannot list its tables, a table with bad settings and a selector that matches nothing are problems, not errors.
pub async fn selected_specs(
    project: &ProjectConfig,
    options: &PlanOptions,
) -> Result<ResolvedTables, LoaderError> {
    let mut chosen = ResolvedTables::default();
    let mut names: Vec<(String, String)> = Vec::new();
    for (source_name, source) in &project.sources {
        let discovered = match source_tables(project, source_name, source).await? {
            Ok((discovered, _)) => discovered,
            Err(problem) => {
                chosen
                    .problems
                    .push(format!("[sources.{source_name}]: {problem}"));
                continue;
            }
        };
        let resolved = resolve_tables(project, source_name, &discovered);
        chosen.problems.extend(resolved.problems);
        for spec in resolved.specs {
            let qualified = spec.source_table.qualified_name();
            names.push((spec.destination.table.clone(), qualified.clone()));
            if selected(options, (source_name, &spec.destination.table, &qualified)) {
                chosen.specs.push(spec);
            }
        }
    }
    chosen
        .problems
        .extend(unmatched_selectors(project, options, &names));
    Ok(chosen)
}
