//! Build the shared clients every command uses.

use std::sync::Arc;

use crate::auth::main::account_url::account_url;
use crate::auth::models::Authenticator;
use crate::config::models::{Config, Destination, StagingKind};
use crate::databricks_api::models::DatabricksClient;
use crate::delivery::models::{DatabricksClients, SnowflakeClients, Warehouse, ZerobusSettings};
use crate::exporter::models::Clients;
use crate::http::errors::ApiError;
use crate::http::main::http_client::build_http_client;
use crate::http::models::{ConnectionCounter, InFlight};
use crate::sql_api::models::SqlClient;
use crate::stage_api::models::DriverSession;
use crate::streaming_api::models::StreamingClient;

/// Build every client on one counted HTTP pool.
pub fn build_clients(config: &Config) -> Result<Clients, ApiError> {
    let connections = ConnectionCounter::default();
    let http = build_http_client(&connections)?;
    let warehouse = match &config.destination {
        Destination::Snowflake(destination) => {
            let auth = Arc::new(Authenticator::from_config(destination)?);
            Warehouse::Snowflake(SnowflakeClients {
                sql: SqlClient::new(http.clone(), auth.clone(), destination),
                streaming: Arc::new(StreamingClient::new(
                    http.clone(),
                    auth.clone(),
                    destination,
                )),
                driver: Arc::new(DriverSession::new(http.clone(), auth.clone(), destination)),
                account_url: account_url(destination),
                auth,
            })
        }
        Destination::Databricks(destination) => {
            let api = DatabricksClient::new(
                http.clone(),
                &destination.host,
                &destination.warehouse_id,
                (&destination.client_id, destination.client_secret.clone()),
            );
            let zerobus = match (config.staging.kind, &destination.zerobus_endpoint) {
                (StagingKind::None, Some(endpoint)) => Some(ZerobusSettings {
                    endpoint: endpoint.clone(),
                    workspace_url: api.host.clone(),
                    client_id: destination.client_id.clone(),
                    client_secret: destination.client_secret.clone(),
                }),
                _ => None,
            };
            Warehouse::Databricks(DatabricksClients { api, zerobus })
        }
    };
    Ok(Clients {
        http,
        connections,
        in_flight: InFlight::new(config.staging.max_in_flight),
        warehouse,
    })
}
