//! Clients for a Databricks destination.

use std::sync::Arc;

use crate::adapters::databricks::_helpers::stage::folders::volume_path;
use crate::adapters::databricks::models::DatabricksDestination;
use crate::databricks_api::models::DatabricksClient;
use crate::http::errors::ApiError;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::project::models::ConnectionConfig;

/// Clients for a `databricks` connection, loading into `catalog` when the target overrides the connection's; nothing is sent until the first statement.
pub(in crate::adapters) fn connect_databricks(
    connection: &ConnectionConfig,
    catalog: Option<&str>,
) -> Result<DatabricksDestination, ApiError> {
    let ConnectionConfig::Databricks {
        host,
        client_id,
        client_secret,
        warehouse_id,
        catalog: own_catalog,
        volume,
    } = connection
    else {
        return Err(ApiError::Protocol(format!(
            "a {} connection is not a Databricks one",
            connection.kind()
        )));
    };
    let http = build_http_client(&ConnectionCounter::default())?;
    Ok(DatabricksDestination {
        api: DatabricksClient::new(http, host, warehouse_id, (client_id, client_secret.clone())),
        catalog: catalog.unwrap_or(own_catalog).to_owned(),
        volume: volume_path(volume),
        ddl: Arc::default(),
    })
}
