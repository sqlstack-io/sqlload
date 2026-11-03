//! Read the tables, stages and pipes already in the destination schema.

use crate::config::models::Config;
use crate::http::errors::ApiError;
use crate::naming::main::quote_name::quote_name;
use crate::planning::_helpers::ddl::literal;
use crate::planning::models::ExistingObjects;
use crate::sql_api::main::execute_sql::execute_sql;
use crate::sql_api::models::SqlClient;

/// What exists now; nothing is cached between runs.
pub async fn read_existing(sql: &SqlClient, config: &Config) -> Result<ExistingObjects, ApiError> {
    let database = quote_name(&config.destination.database);
    let schema_name = config.destination.schema.to_ascii_uppercase();
    let in_schema = format!("{database}.{}", quote_name(&config.destination.schema));
    let mut existing = ExistingObjects::default();
    let columns = execute_sql(sql, &format!(
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM {database}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = {}",
            literal(&schema_name)
        ))
        .await?;
    for row in &columns.rows {
        if let (Some(Some(table)), Some(Some(column)), Some(Some(data_type))) =
            (row.first(), row.get(1), row.get(2))
        {
            existing
                .tables
                .entry(table.clone())
                .or_default()
                .insert(column.clone(), data_type.clone());
        }
    }
    let stages = execute_sql(sql, &format!("SHOW STAGES IN SCHEMA {in_schema}")).await?;
    existing.stages = stages
        .rows
        .iter()
        .filter_map(|row| stages.value(row, "name").map(str::to_owned))
        .collect();
    let pipes = execute_sql(sql, &format!("SHOW PIPES IN SCHEMA {in_schema}")).await?;
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
