//! Clients for a BigQuery destination.

use std::sync::Arc;

use crate::adapters::bigquery::models::BigQueryDestination;
use crate::bigquery_api::main::parse_key::parse_key;
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::project::models::ConnectionConfig;

/// Clients for a `bigquery` connection, loading into `project` when the target overrides the connection's (which defaults to the key's); jobs run in the connection's project. Nothing is sent until the first statement.
pub(in crate::adapters) fn connect_bigquery(
    connection: &ConnectionConfig,
    project: Option<&str>,
) -> Result<BigQueryDestination, ApiError> {
    let ConnectionConfig::BigQuery {
        project: own_project,
        location,
        service_account_key,
        base_url,
        storage_url,
    } = connection
    else {
        return Err(ApiError::Protocol(format!(
            "a {} connection is not a BigQuery one",
            connection.kind()
        )));
    };
    let key = parse_key(service_account_key.expose())?;
    let job_project = own_project
        .clone()
        .unwrap_or_else(|| key.project_id.clone());
    let http = build_http_client(&ConnectionCounter::default())?;
    Ok(BigQueryDestination {
        project: project.map_or_else(|| job_project.clone(), str::to_owned),
        api: BigQueryClient::new(
            http,
            (&job_project, location),
            key,
            (base_url.as_deref(), storage_url.as_deref()),
        ),
        ddl: Arc::default(),
    })
}
