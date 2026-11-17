//! Read the tables, stages, pipes and volumes already in the destination schema.

use std::collections::BTreeMap;

use crate::config::models::{Config, Destination};
use crate::delivery::models::Warehouse;
use crate::http::errors::ApiError;
use crate::naming::main::quote_name::quote_name;
use crate::planning::_helpers::ddl::literal;
use crate::planning::models::ExistingObjects;
use crate::sql_api::models::SqlRows;

/// What exists now; nothing is cached between runs.
pub async fn read_existing(
    warehouse: &Warehouse,
    config: &Config,
) -> Result<ExistingObjects, ApiError> {
    let mut existing = ExistingObjects::default();
    if let Destination::Databricks(databricks) = &config.destination {
        let catalog = format!("`{}`", databricks.catalog.replace('`', "``"));
        let schema = databricks_literal(&databricks.schema);
        let columns = warehouse.execute(&format!(
            "SELECT table_name, column_name, full_data_type FROM {catalog}.information_schema.columns WHERE table_schema = {schema}"
        )).await?;
        existing.tables = tables(&columns);
        let volumes = warehouse.execute(&format!(
            "SELECT volume_name FROM {catalog}.information_schema.volumes WHERE volume_schema = {schema}"
        )).await?;
        existing.stages = volumes
            .rows
            .iter()
            .filter_map(|row| row.first().cloned().flatten())
            .collect();
        return Ok(existing);
    }
    let database = quote_name(config.destination.database());
    let schema_name = config.destination.schema().to_ascii_uppercase();
    let in_schema = format!("{database}.{}", quote_name(config.destination.schema()));
    let columns = warehouse.execute(&format!(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM {database}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = {}",
            literal(&schema_name)
        ))
        .await?;
    existing.tables = tables(&columns);
    let stages = warehouse
        .execute(&format!("SHOW STAGES IN SCHEMA {in_schema}"))
        .await?;
    existing.stages = stages
        .rows
        .iter()
        .filter_map(|row| stages.value(row, "name").map(str::to_owned))
        .collect();
    let pipes = warehouse
        .execute(&format!("SHOW PIPES IN SCHEMA {in_schema}"))
        .await?;
    existing.pipes = pipes
        .rows
        .iter()
        .filter_map(|row| {
            let name = pipes.value(row, "name")?.to_owned();
            Some((name, pipes.value(row, "definition").map(str::to_owned)))
        })
        .collect();
    Ok(existing)
}

/// Each table's columns and types, from rows of table name, column name and type.
fn tables(columns: &SqlRows) -> BTreeMap<String, BTreeMap<String, String>> {
    let mut tables: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for row in &columns.rows {
        if let (Some(Some(table)), Some(Some(column)), Some(Some(data_type))) =
            (row.first(), row.get(1), row.get(2))
        {
            tables
                .entry(table.clone())
                .or_default()
                .insert(column.clone(), data_type.clone());
        }
    }
    tables
}

/// A Databricks SQL string literal, where `\` escapes.
fn databricks_literal(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
}
