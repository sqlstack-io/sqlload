//! Plan every selected table of a project against its live source and destination.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

use crate::adapters::dispatch::models::Destination;
use crate::adapters::files::main::folder_conflict::folder_conflict;
use crate::adapters::files::models::FolderClaim;
use crate::loader::_helpers::destination::{
    block_shared_destinations, mark_shared_destinations, unknown_sharing,
};
use crate::loader::_helpers::python::python_table;
use crate::loader::_helpers::resolve::{
    admit_tables, destination_names, found_sources, spec_names, unmatched_selectors,
    with_kept_names,
};
#[cfg(feature = "odbc")]
use crate::loader::_helpers::sources::with_fetch_memory;
use crate::loader::_helpers::sources::{PLANNED_SOURCES, selected, source_ends};
#[cfg(feature = "odbc")]
use crate::loader::_helpers::table_plan::block_refused_bounds;
use crate::loader::_helpers::table_plan::{blocked_plan, plan_table, skip_up_to_date};
use crate::loader::constants::NO_DELETES_TABLES;
use crate::loader::errors::LoaderError;
use crate::loader::main::_mixed_cursors::block_mixed_cursors;
use crate::loader::models::{
    FilesSource, FoundSource, PlanOptions, ProjectPlan, TablePlan, TableSpec,
};
use crate::project::models::{ProjectConfig, SourceConfig, SourceKind};

/// The plan: what `apply` would change and what `load` would move, read fresh from every system.
pub async fn plan_project(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<ProjectPlan, LoaderError> {
    PLANNED_SOURCES.store(0, Ordering::Relaxed);
    let found = found_sources(project, destinations, options).await?;
    let (mut plan, claims, configured) = (found.plan, found.claims, found.configured);
    let (unresolved, drained) = (found.unresolved, found.drained);
    PLANNED_SOURCES.store(
        project.sources.len() - found.sources.len(),
        Ordering::Relaxed,
    );
    for FoundSource {
        name: source_name,
        source,
        mut resolved,
        files,
    } in found.sources
    {
        if !resolved.problems.is_empty() {
            plan.failed_sources.insert(source_name.clone());
        }
        plan.problems.extend(resolved.problems);
        plan.problem_names.extend(resolved.problem_names);
        plan.problem_places.extend(resolved.problem_places);
        plan.warnings.extend(resolved.warnings);
        plan.skipped.extend(resolved.skipped);
        let unknown = !unresolved.is_empty() && files.is_none();
        let shared =
            mark_shared_destinations(std::mem::take(&mut resolved.specs), &configured, unknown);
        if let Some(sharing) = unknown_sharing(&shared, &configured, &unresolved) {
            plan.warnings.push(sharing.warning.clone());
            plan.unread_sharing.push(sharing);
        }
        let specs = source_ends(project, (source_name, source), shared, options).await?;
        let (planned, unselected) = planned_tables(
            project,
            (source_name, source, files),
            specs,
            (destinations, options),
        )
        .await?;
        plan = admit_tables(
            plan,
            (source_name, source),
            (planned, &unselected),
            (destinations, options),
        )
        .await?;
        PLANNED_SOURCES.fetch_add(1, Ordering::Relaxed);
    }
    let ranged = options.ranged();
    Ok(skip_up_to_date(
        block_conflicts(plan, (&claims, &configured, &drained), (project, options)),
        ranged,
    ))
}

/// One source's `specs`: the selected ones planned against their destinations, with an ODBC source's tables blocked whose cursor bounds its driver refuses, and the others.
async fn planned_tables(
    project: &ProjectConfig,
    (source_name, source, files): (&str, &SourceConfig, Option<FilesSource>),
    specs: Vec<TableSpec>,
    (destinations, options): (&BTreeMap<String, Destination>, &PlanOptions),
) -> Result<(Vec<TablePlan>, Vec<TableSpec>), LoaderError> {
    let module = project.python_modules.get(source_name);
    let paths = project.python_http_paths.get(source_name);
    let mut planned: Vec<TablePlan> = Vec::new();
    let mut unselected: Vec<TableSpec> = Vec::new();
    for mut spec in specs {
        spec.files = files.clone();
        spec.python = module.map(|module| python_table(module, &spec.source_table, paths));
        if let Some(deletes) = &mut spec.deletes {
            deletes.scan = options.sync_deletes;
        }
        if !selected(options, (source_name, &spec_names(&spec))) {
            unselected.push(spec);
            continue;
        }
        let destination = destinations.get(&spec.target).ok_or_else(|| {
            LoaderError::Invalid(format!("target {:?} is not connected", spec.target))
        })?;
        let plan = plan_table(&spec, destination, options).await?;
        planned.push(Box::pin(with_kept_names(project, spec, plan, (destination, options))).await?);
    }
    if source.kind == SourceKind::Odbc {
        #[cfg(feature = "odbc")]
        block_refused_bounds(project, (source_name, source), &mut planned).await?;
    }
    #[cfg(feature = "odbc")]
    if source.kind == SourceKind::Odbc {
        planned = with_fetch_memory(project, source, planned, options).await?;
    }
    Ok((planned, unselected))
}

/// The plan with a warning for each selector matching none of the `configured` tables, a problem when `--sync-deletes` selects no table with `deletes`, every selected files table blocked whose folder clashes with any other configured table's, every selected table blocked whose destination table it shares with configured tables it could remove or overwrite rows of (see [`block_shared_destinations`]), and every cursor table blocked when a cursor range meets cursors of different kinds.
fn block_conflicts(
    mut plan: ProjectPlan,
    (claims, configured, drained): (&[FolderClaim], &[TableSpec], &[String]),
    (project, options): (&ProjectConfig, &PlanOptions),
) -> ProjectPlan {
    let mut names = destination_names(configured);
    names.extend(drained.iter().cloned());
    plan.warnings
        .extend(unmatched_selectors(project, options, &names));
    let scans = plan.tables.iter().any(|table| table.spec.deletes.is_some());
    if options.sync_deletes && !scans {
        plan.problems.push(NO_DELETES_TABLES.to_owned());
    }
    #[cfg(feature = "odbc")]
    {
        let fetches: Vec<(usize, &str, Option<u64>)> = plan
            .tables
            .iter()
            .filter(|table| table.schema.blocked.is_none())
            .filter_map(|table| {
                let fetch = table.fetch_memory.as_ref()?;
                Some((
                    fetch.estimate?,
                    table.spec.source_name.as_str(),
                    fetch.limit,
                ))
            })
            .collect();
        let concurrency = options.concurrency.or(project.concurrency);
        plan.warnings
            .extend(crate::odbc_source::main::memory_warning::memory_warning(
                &fetches,
                concurrency.unwrap_or(crate::loader::constants::DEFAULT_THREADS),
            ));
    }
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
