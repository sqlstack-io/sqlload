//! Plan every selected table of a project against its live source and destination.

use std::collections::BTreeMap;

use crate::adapters::dispatch::models::Destination;
use crate::adapters::files::main::folder_conflict::folder_conflict;
use crate::adapters::files::models::FolderClaim;
use crate::loader::_helpers::destination::{
    block_shared_destinations, mark_shared_destinations, unknown_sharing,
};
use crate::loader::_helpers::python::python_table;
use crate::loader::_helpers::sources::{
    admit_tables, destination_names, found_sources, selected, source_ends, unmatched_selectors,
};
use crate::loader::_helpers::table_plan::{blocked_plan, plan_table};
use crate::loader::errors::LoaderError;
use crate::loader::main::_mixed_cursors::block_mixed_cursors;
use crate::loader::models::{FoundSource, PlanOptions, ProjectPlan, TablePlan, TableSpec};
use crate::project::models::ProjectConfig;

/// The plan: what `apply` would change and what `load` would move, read fresh from every system.
pub async fn plan_project(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<ProjectPlan, LoaderError> {
    let found = found_sources(project, destinations, options).await?;
    let (mut plan, claims, configured) = (found.plan, found.claims, found.configured);
    let unresolved = found.unresolved;
    for FoundSource {
        name: source_name,
        source,
        mut resolved,
        files,
    } in found.sources
    {
        let module = project.python_modules.get(source_name);
        plan.problems.extend(resolved.problems);
        plan.warnings.extend(resolved.warnings);
        plan.skipped.extend(resolved.skipped);
        let unknown = !unresolved.is_empty() && files.is_none();
        let shared =
            mark_shared_destinations(std::mem::take(&mut resolved.specs), &configured, unknown);
        plan.warnings
            .extend(unknown_sharing(&shared, &configured, &unresolved));
        let specs = source_ends(project, (source_name, source), shared, options).await?;
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
    Ok(block_conflicts(
        plan,
        (&claims, &configured),
        (project, options),
    ))
}

/// The plan with a warning for each selector matching none of the `configured` tables, every selected files table blocked whose folder clashes with any other configured table's, every selected table blocked whose destination table it shares with configured tables it could remove or overwrite rows of (see [`block_shared_destinations`]), and every cursor table blocked when a cursor range meets cursors of different kinds.
fn block_conflicts(
    mut plan: ProjectPlan,
    (claims, configured): (&[FolderClaim], &[TableSpec]),
    (project, options): (&ProjectConfig, &PlanOptions),
) -> ProjectPlan {
    plan.warnings.extend(unmatched_selectors(
        project,
        options,
        &destination_names(configured),
    ));
    let mut plan = block_shared_destinations(plan, configured);
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
