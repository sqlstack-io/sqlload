//! The settings of every selected table, without reading any destination.

use crate::loader::_helpers::python::python_table;
use crate::loader::_helpers::resolve::spec_names;
use crate::loader::_helpers::sources::{scoped_tables, selected, unmatched_selectors};
use crate::loader::_helpers::table_plan::{desired_columns, read_columns};
use crate::loader::errors::LoaderError;
use crate::loader::models::{PlanOptions, SelectedTable, SelectedTables};
use crate::project::models::ProjectConfig;

/// Each source's tables resolved to their settings (with their files, or the module declaring a Python table) and narrowed to the selection in `options`, each with the columns its source would load (read from what was listed, nothing more); a source that cannot list its tables, a table with bad settings and a selector that matches nothing are problems, not errors; a source's problems count only when the selection takes something from it (see [`scoped_tables`]).
pub async fn selected_specs(
    project: &ProjectConfig,
    options: &PlanOptions,
) -> Result<SelectedTables, LoaderError> {
    let mut chosen = SelectedTables::default();
    let mut names: Vec<String> = Vec::new();
    for (source_name, source) in &project.sources {
        let (resolved, files) =
            match scoped_tables((project, options), (source_name, source)).await? {
                Ok(found) => found,
                Err(problems) => {
                    chosen
                        .problems
                        .extend(problems.into_iter().map(|problem| problem.message));
                    continue;
                }
            };
        let module = project.python_modules.get(source_name);
        chosen.problems.extend(resolved.problems);
        chosen
            .problems
            .extend(resolved.drained.filter(|drained| drained.in_scope).map(|drained| format!(
                "[sources.{}]: after_load emptied its folder, so there are no files to read its columns from until new ones come",
                drained.skipped.source_name
            )));
        for mut spec in resolved.specs {
            spec.files = files.clone();
            spec.python = module.map(|module| python_table(module, &spec.source_table));
            let picked = spec_names(&spec);
            names.extend(picked.iter().cloned());
            if selected(options, (source_name, &picked)) {
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
