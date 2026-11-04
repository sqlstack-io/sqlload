//! Create and change destination tables; never loads data.

use std::collections::BTreeMap;

use crate::loader::_helpers::destination::execute;
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, ProjectPlan};
use crate::schema_plan::models::TableAction;
use crate::write_sql::main::alter_statements::alter_statements;
use crate::write_sql::main::create_schema::create_schema;
use crate::write_sql::main::create_table::create_table;
use crate::write_sql::main::display_name::display_name;

/// Apply every unblocked table's schema changes; one line per table changed.
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
        let warehouse = spec.warehouse;
        let statements: Vec<String> = match &table.schema.action {
            TableAction::Unchanged | TableAction::Recreate => continue,
            TableAction::Create => vec![
                create_schema(&spec.destination, warehouse),
                create_table(&spec.destination, &table.schema.write_columns, warehouse),
            ],
            TableAction::Alter(changes) => {
                let live = table.live.as_deref().unwrap_or_default();
                alter_statements(&spec.destination, changes, live, warehouse)
            }
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
            display_name(&spec.destination, warehouse)
        ));
    }
    Ok(lines)
}
