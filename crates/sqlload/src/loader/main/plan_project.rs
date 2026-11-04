//! Plan every selected table of a project against its live source and destination.

use std::collections::BTreeMap;

use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::_helpers::sources::{connect_source, selected};
use crate::loader::_helpers::table_plan::plan_table;
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, PlanOptions, ProjectPlan, SkippedTable};
use crate::postgres_source::main::read_catalog::read_catalog;
use crate::project::models::{NewTables, ProjectConfig};

/// The plan: what `apply` would change and what `load` would move, read fresh from every system.
pub async fn plan_project(
    project: &ProjectConfig,
    destinations: &BTreeMap<String, Destination>,
    options: &PlanOptions,
) -> Result<ProjectPlan, LoaderError> {
    let mut plan = ProjectPlan::default();
    for (source_name, source) in &project.sources {
        let client = connect_source(project, source).await?;
        let discovered = read_catalog(&client, &source.include, &source.exclude).await?;
        let resolved = resolve_tables(project, source_name, &discovered);
        plan.problems.extend(resolved.problems);
        plan.skipped.extend(resolved.skipped);
        for spec in resolved.specs {
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
            let table = plan_table(&spec, destination, options.rebuild_allowed).await?;
            let is_new = table.live.is_none() && !spec.listed;
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
    Ok(plan)
}
