//! Whether a live table is laid out as its config says.

use crate::adapters::bigquery::_helpers::dialect::layout::layout_drift;
use crate::adapters::bigquery::models::BigQueryDestination;
use crate::adapters::contract::models::TableName;
use crate::bigquery_api::main::table_metadata::table_metadata;
use crate::http::errors::ApiError;
use crate::loader::models::TableLayout;

/// How `table` is laid out otherwise than `layout`, from `tables.get` (metadata, so nothing is billed); `None` when it matches or does not exist.
pub(in crate::adapters) async fn layout_problems(
    destination: &BigQueryDestination,
    table: &TableName,
    layout: &TableLayout,
) -> Result<Option<String>, ApiError> {
    let project = table.database.as_deref().unwrap_or(&destination.project);
    let found = table_metadata(&destination.api, (project, &table.schema, &table.table)).await?;
    Ok(found.and_then(|found| layout_drift(&found, layout)))
}
