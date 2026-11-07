//! The settings of every selected table, without reading any destination.

use crate::loader::_helpers::python::python_table;
use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::_helpers::sources::{selected, source_tables, unmatched_selectors};
use crate::loader::_helpers::table_plan::{desired_columns, read_columns};
use crate::loader::errors::LoaderError;
use crate::loader::models::{PlanOptions, SelectedTable, SelectedTables};
use crate::project::models::ProjectConfig;

/// Each source's tables resolved to their settings (with their files, or the module declaring a Python table) and narrowed to the selection in `options`, each with the columns its source would load (read from what was listed, nothing more); a source that cannot list its tables, a table with bad settings and a selector that matches nothing are problems, not errors.
pub async fn selected_specs(
    project: &ProjectConfig,
    options: &PlanOptions,
) -> Result<SelectedTables, LoaderError> {
    let mut chosen = SelectedTables::default();
    let mut names: Vec<(String, String)> = Vec::new();
    for (source_name, source) in &project.sources {
        let (discovered, files) = match source_tables(project, source_name, source).await? {
            Ok(found) => found,
            Err(problem) => {
                chosen
                    .problems
                    .push(format!("[sources.{source_name}]: {problem}"));
                continue;
            }
        };
        let module = project.python_modules.get(source_name);
        let resolved = resolve_tables(project, source_name, &discovered);
        chosen.problems.extend(resolved.problems);
        for mut spec in resolved.specs {
            spec.files = files.clone();
            spec.python = module.map(|module| python_table(module, &spec.source_table));
            let qualified = spec.source_table.qualified_name();
            names.push((spec.destination.table.clone(), qualified.clone()));
            if selected(options, (source_name, &spec.destination.table, &qualified)) {
                let source_columns = read_columns(&spec).map(|read| desired_columns(&spec, &read));
                chosen.tables.push(SelectedTable {
                    spec,
                    source_columns,
                });
            }
        }
    }
    chosen
        .problems
        .extend(unmatched_selectors(project, options, &names));
    Ok(chosen)
}
