//! A reader for a `databricks` source's connection.

use crate::databricks_api::models::DatabricksClient;
use crate::databricks_source::errors::DatabricksSourceError;
use crate::databricks_source::models::DatabricksReader;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::project::models::ConnectionConfig;

/// A reader for a `databricks` connection, reading the tables of its catalog through its SQL warehouse; nothing is sent yet.
pub fn databricks_reader(
    connection: &ConnectionConfig,
) -> Result<DatabricksReader, DatabricksSourceError> {
    let ConnectionConfig::Databricks {
        host,
        client_id,
        client_secret,
        warehouse_id,
        catalog,
        ..
    } = connection
    else {
        return Err(DatabricksSourceError::Invalid(format!(
            "a {} connection is not a databricks one",
            connection.kind()
        )));
    };
    let http = build_http_client(&ConnectionCounter::default())?;
    Ok(DatabricksReader {
        client: DatabricksClient::new(http, host, warehouse_id, (client_id, client_secret.clone())),
        catalog: catalog.clone(),
    })
}
