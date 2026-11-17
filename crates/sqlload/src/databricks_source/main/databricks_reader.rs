//! A reader for a `databricks` source.

use crate::databricks_api::models::DatabricksClient;
use crate::databricks_source::errors::DatabricksSourceError;
use crate::databricks_source::models::DatabricksReader;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::project::models::{ConnectionConfig, ProjectConfig, SourceConfig};

/// A reader for `source`'s `databricks` connection, reading the tables of its catalog through its SQL warehouse; nothing is sent yet.
pub fn databricks_reader(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<DatabricksReader, DatabricksSourceError> {
    let Some(ConnectionConfig::Databricks {
        host,
        client_id,
        client_secret,
        warehouse_id,
        catalog,
        ..
    }) = project.connections.get(&source.connection)
    else {
        return Err(DatabricksSourceError::Invalid(format!(
            "connection {:?} is not a databricks connection",
            source.connection
        )));
    };
    let http = build_http_client(&ConnectionCounter::default())?;
    Ok(DatabricksReader {
        client: DatabricksClient::new(http, host, warehouse_id, (client_id, client_secret.clone())),
        catalog: catalog.clone(),
    })
}
