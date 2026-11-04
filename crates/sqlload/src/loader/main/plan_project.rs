//! Plan every selected table of a project against its live source and destination.

use std::collections::BTreeMap;

use crate::files_destination::main::folder_conflict::folder_conflict;
use crate::files_destination::models::FolderClaim;
use crate::loader::_helpers::python::python_table;
use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::_helpers::sources::{selected, source_tables};
use crate::loader::_helpers::table_plan::{blocked_plan, plan_table};
use crate::loader::errors::LoaderError;
use crate::loader::main::_table_folder::table_folder;
use crate::loader::models::{Destination, PlanOptions, ProjectPlan, SkippedTable, TableSpec};
use crate::project::models::{NewTables, ProjectConfig};

/// The plan: what `apply` would change and what `load` would move, read fresh from every system.
pub async fn plan_project(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<ProjectPlan, LoaderError> {
    let mut plan = ProjectPlan::default();
    let mut claims: Vec<FolderClaim> = Vec::new();
    for (source_name, source) in &project.sources {
        let (discovered, files) = match source_tables(project, source_name, source).await? {
            Ok(found) => found,
            Err(problem) => {
                plan.problems
                    .push(format!("[sources.{source_name}]: {problem}"));
                continue;
            }
        };
        let module = project.python_modules.get(source_name);
        let resolved = resolve_tables(project, source_name, &discovered);
        plan.problems.extend(resolved.problems);
        plan.skipped.extend(resolved.skipped);
        claims.extend(folder_claims(&resolved.specs, destinations));
        for mut spec in resolved.specs {
            spec.files = files.clone();
            spec.python = module.map(|module| python_table(module, &spec.source_table));
            let qualified = spec.source_table.qualified_name();
            if !selected(
                &options.select,
                source_name,
                &spec.destination.table,
                &qualified,
            ) {
                continue;
            }
            let destination = destinations.get(&spec.target).ok_or_else(|| {
                LoaderError::Invalid(format!("target {:?} is not connected", spec.target))
            })?;
            let table = plan_table(&spec, destination, options).await?;
            let is_new = !table.exists && !spec.listed;
            match (is_new, spec.new_tables) {
                (true, NewTables::Skip) => plan.skipped.push(SkippedTable {
                    source_name: source_name.clone(),
                    source_table: qualified,
                    reason: "new table (new_tables = \"skip\"); add it under [sources.*.tables] to sync it".to_owned(),
                }),
                (true, NewTables::Fail) => plan.problems.push(format!(
                    "{qualified}: new table and new_tables = \"fail\"; add it under [sources.{source_name}.tables] or exclude it"
                )),
                _ => plan.tables.push(table),
            }
        }
    }
    Ok(block_overlapping(plan, &claims))
}

/// The folders files tables write to, whether or not they are selected; warehouse tables and paths that do not resolve claim none.
fn folder_claims(
    specs: &[TableSpec],
    destinations: &BTreeMap<String, Destination>,
) -> Vec<FolderClaim> {
    let mut claims: Vec<FolderClaim> = Vec::new();
    for spec in specs {
        let Some(Destination::Files(files)) = destinations.get(&spec.target) else {
            continue;
        };
        let name = format!(
            "{}.{}",
            spec.source_name,
            spec.source_table.qualified_name()
        );
        if let Ok(path) = table_folder(spec, files) {
            claims.push(FolderClaim {
                id: name.clone(),
                name,
                folder: path.folder,
                template: spec.destination_path.clone().unwrap_or_default(),
                schema: spec.source_table.schema.clone(),
            });
        }
    }
    claims
}

/// The plan with every selected files table blocked whose folder clashes with any other configured table's.
fn block_overlapping(mut plan: ProjectPlan, claims: &[FolderClaim]) -> ProjectPlan {
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
    plan
}
