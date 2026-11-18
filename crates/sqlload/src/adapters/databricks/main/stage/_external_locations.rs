//! The Unity Catalog external locations a Databricks destination reads files through.

use crate::adapters::databricks::_helpers::stage::locations::location_url;
use crate::adapters::databricks::constants::{LIST_LOCATIONS, LOCATION_URL_COLUMN};
use crate::adapters::databricks::models::DatabricksDestination;
use crate::databricks_api::main::execute_statement::execute_statement;
use crate::http::errors::ApiError;

/// The URLs of the external locations `destination`'s service principal can see, each ending in `/` so that a folder only covers what lies below it; listed once per run.
pub(in crate::adapters) async fn external_locations(
    destination: &DatabricksDestination,
) -> Result<&[String], ApiError> {
    let listed = destination
        .external_locations
        .get_or_try_init(|| async {
            let rows = execute_statement(&destination.api, LIST_LOCATIONS).await?;
            let url = rows
                .columns
                .iter()
                .position(|column| column == LOCATION_URL_COLUMN)
                .ok_or_else(|| {
                    ApiError::Protocol(format!("{LIST_LOCATIONS} returned no url column"))
                })?;
            Ok::<Vec<String>, ApiError>(
                rows.rows
                    .iter()
                    .filter_map(|row| row.get(url).cloned().flatten())
                    .map(|url| location_url(&url))
                    .collect(),
            )
        })
        .await?;
    Ok(listed)
}
