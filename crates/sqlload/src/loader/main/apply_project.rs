//! Create and change destination tables; never loads data.

use std::collections::BTreeMap;

use crate::loader::_helpers::destination::execute;
use crate::loader::_helpers::table_plan::schema_statements;
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, ProjectPlan};
use crate::schema_plan::models::TableAction;
use crate::write_sql::main::display_name::display_name;

/// Apply every unblocked table's schema changes; one line per table changed (files targets need none: folders appear with their files; Python tables without declared columns are created by their first load).
pub async fn apply_project(
    plan: &ProjectPlan,
    destinations: &BTreeMap<String, Destination>,
) -> Result<Vec<String>, LoaderError> {
    let mut lines: Vec<String> = Vec::new();
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
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
            display_name(&spec.destination, spec.warehouse)
        ));
    }
    Ok(lines)
}
