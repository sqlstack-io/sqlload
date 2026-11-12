//! Plan every selected table of a project against its live source and destination.

use std::collections::BTreeMap;

use crate::adapters::dispatch::main::dialect::files_destination::files_destination;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::files::main::folder_conflict::folder_conflict;
use crate::adapters::files::models::FolderClaim;
use crate::loader::_helpers::destination::table_folder;
use crate::loader::_helpers::python::python_table;
use crate::loader::_helpers::sources::{
    admit_tables, scoped_tables, selected, source_ends, unmatched_selectors,
};
use crate::loader::_helpers::table_plan::{blocked_plan, plan_table};
use crate::loader::errors::LoaderError;
use crate::loader::main::_mixed_cursors::block_mixed_cursors;
use crate::loader::main::_shared_destinations::block_shared_destinations;
use crate::loader::models::{FilesSource, PlanOptions, ProjectPlan, TablePlan, TableSpec};
use crate::project::models::ProjectConfig;

/// The plan: what `apply` would change and what `load` would move, read fresh from every system.
pub async fn plan_project(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<ProjectPlan, LoaderError> {
    let mut plan = ProjectPlan::default();
    let mut claims: Vec<FolderClaim> = Vec::new();
    let mut names: Vec<(String, String)> = Vec::new();
    let mut configured: Vec<TableSpec> = Vec::new();
    for (source_name, source) in &project.sources {
        let (resolved, files) =
            match scoped_tables((project, options), (source_name, source)).await? {
                Ok(found) => found,
                Err(problem) => {
                    if let Some(problem) = problem {
                        if problem.retryable {
                            plan.retryable_problems.push(problem.message.clone());
                        }
                        plan.problems.push(problem.message);
                    }
                    continue;
                }
            };
        let module = project.python_modules.get(source_name);
        plan.problems.extend(resolved.problems);
        plan.warnings.extend(resolved.warnings);
        plan.skipped.extend(resolved.skipped);
        claims.extend(folder_claims(&resolved.specs, files.as_ref(), destinations));
        configured.extend(resolved.specs.iter().cloned());
        names.extend(resolved.specs.iter().map(|spec| {
            (
                spec.destination.table.clone(),
                spec.source_table.qualified_name(),
            )
        }));
        let specs = source_ends(project, (source_name, source), resolved.specs, options).await?;
        let mut planned: Vec<TablePlan> = Vec::new();
        let mut unselected: Vec<TableSpec> = Vec::new();
        for mut spec in specs {
            spec.files = files.clone();
            spec.python = module.map(|module| python_table(module, &spec.source_table));
            let qualified = spec.source_table.qualified_name();
            if !selected(options, (source_name, &spec.destination.table, &qualified)) {
                unselected.push(spec);
                continue;
            }
            let destination = destinations.get(&spec.target).ok_or_else(|| {
                LoaderError::Invalid(format!("target {:?} is not connected", spec.target))
            })?;
            planned.push(plan_table(&spec, destination, options).await?);
        }
        plan = admit_tables(
            plan,
            (source_name, source),
            (planned, &unselected),
            (destinations, options),
        )
        .await?;
    }
    plan.warnings
        .extend(unmatched_selectors(project, options, &names));
    let plan = block_shared_destinations(plan, &configured);
    Ok(block_conflicts(plan, &claims, options))
}

/// The folders files tables write to, whether or not they are selected, with the folder their files source (`files`) reads; warehouse tables and paths that do not resolve claim none.
fn folder_claims(
    specs: &[TableSpec],
    files: Option<&FilesSource>,
    destinations: &BTreeMap<String, Destination>,
) -> Vec<FolderClaim> {
    let mut claims: Vec<FolderClaim> = Vec::new();
    for spec in specs {
        let Some(target) = destinations.get(&spec.target).and_then(files_destination) else {
            continue;
        };
        let name = format!(
            "{}.{}",
            spec.source_name,
            spec.source_table.qualified_name()
        );
        if let Ok(path) = table_folder(spec, target) {
            claims.push(FolderClaim {
                id: name.clone(),
                name,
                folder: path.folder,
                template: spec.destination_path.clone().unwrap_or_default(),
                schema: spec.source_table.schema.clone(),
                source: files.map(|files| files.location.clone()),
            });
        }
    }
    claims
}

/// The plan with every selected files table blocked whose folder clashes with any other configured table's, and every cursor table blocked when a cursor range meets cursors of different kinds.
fn block_conflicts(
    mut plan: ProjectPlan,
    claims: &[FolderClaim],
    options: &PlanOptions,
) -> ProjectPlan {
    for table in plan
        .tables
        .iter_mut()
        .filter(|table| table.output.is_some())
    {
        let id = format!(
            "{}.{}",
            table.spec.source_name,
            table.spec.source_table.qualified_name()
        );
        let own = claims.iter().find(|claim| claim.id == id);
        if let Some(problem) = own.and_then(|own| folder_conflict(own, claims)) {
            table.schema = blocked_plan(vec![problem]);
        }
    }
    block_mixed_cursors(plan, options)
}
