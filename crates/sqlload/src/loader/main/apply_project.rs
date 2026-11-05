//! Create and change destination tables; never loads data.

use crate::adapters::dispatch::main::sql_adapter::sql_adapter;
use std::collections::{BTreeMap, BTreeSet};

use crate::adapters::dispatch::main::execute::execute;
use crate::adapters::dispatch::main::execute_ddl::execute_ddl;
use crate::adapters::dispatch::models::Destination;
use crate::loader::_helpers::table_plan::schema_statements;
use crate::loader::errors::LoaderError;
use crate::loader::models::ProjectPlan;
use crate::schema_plan::models::TableAction;

/// Create the stages SQLLoad maintains, then apply every unblocked table's schema changes; one line per stage or table changed (files targets need none: folders appear with their files; Python tables are created and changed by their loads, once their data shows the columns).
pub async fn apply_project(
    plan: &ProjectPlan,
    destinations: &BTreeMap<String, Destination>,
) -> Result<Vec<String>, LoaderError> {
    let mut lines: Vec<String> = Vec::new();
    let mut created: BTreeSet<&str> = BTreeSet::new();
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
    {
        let Some(stage) = &table.planned_stage else {
            continue;
        };
        if !created.insert(stage.name.as_str()) {
            continue;
        }
        let destination = destinations.get(&table.spec.target).ok_or_else(|| {
            LoaderError::Invalid(format!("target {:?} is not connected", table.spec.target))
        })?;
        execute_ddl(destination, stage.statements.clone()).await?;
        lines.push(format!("created  stage {} on {}", stage.name, stage.url));
    }
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none() && table.spec.python.is_none())
    {
        let spec = &table.spec;
        let Some(statements) = schema_statements(table) else {
            continue;
        };
        let destination = destinations.get(&spec.target).ok_or_else(|| {
            LoaderError::Invalid(format!("target {:?} is not connected", spec.target))
        })?;
        execute(destination, statements).await?;
        let verb = match table.schema.action {
            TableAction::Create => "created",
            _ => "altered",
        };
        lines.push(format!(
            "{verb}  {}",
            sql_adapter(spec.warehouse).render_display_name(&spec.destination)
        ));
    }
    Ok(lines)
}
