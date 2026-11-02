//! Build the shared clients every command uses.

use std::sync::Arc;

use crate::auth::main::account_url::account_url;
use crate::auth::models::Authenticator;
use crate::config::models::Config;
use crate::exporter::models::Clients;
use crate::http::errors::ApiError;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::sql_api::models::SqlClient;
use crate::stage_api::models::DriverSession;
use crate::streaming_api::models::StreamingClient;

/// Build every client on one counted HTTP pool.
pub fn build_clients(config: &Config) -> Result<Clients, ApiError> {
    let connections = ConnectionCounter::default();
    let http = build_http_client(&connections)?;
    let auth = Arc::new(Authenticator::from_config(&config.destination)?);
    let destination = &config.destination;
    Ok(Clients {
        sql: SqlClient::new(http.clone(), auth.clone(), destination),
        streaming: Arc::new(StreamingClient::new(
            http.clone(),
            auth.clone(),
            destination,
        )),
        driver: Arc::new(DriverSession::new(http.clone(), auth.clone(), destination)),
        account_url: account_url(destination),
        http,
        connections,
        auth,
    })
}
