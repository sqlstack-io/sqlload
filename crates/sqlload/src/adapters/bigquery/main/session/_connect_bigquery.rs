//! Clients for a BigQuery destination.

use std::sync::Arc;

use crate::adapters::bigquery::models::BigQueryDestination;
use crate::bigquery_api::main::signed_in_client::signed_in_client;
use crate::http::errors::ApiError;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::project::models::ConnectionConfig;

/// Clients for a `bigquery` connection, loading into `project` when the target overrides the connection's (which defaults to the credentials' own); jobs run in the connection's project. Nothing is sent until the first statement.
pub(in crate::adapters) fn connect_bigquery(
    connection: &ConnectionConfig,
    project: Option<&str>,
) -> Result<BigQueryDestination, ApiError> {
    let ConnectionConfig::BigQuery {
        project: own_project,
        location,
        service_account_key,
        impersonate_service_account,
        base_url,
        storage_url,
    } = connection
    else {
        return Err(ApiError::Protocol(format!(
            "a {} connection is not a BigQuery one",
            connection.kind()
        )));
    };
    let api = signed_in_client(
        build_http_client(&ConnectionCounter::default())?,
        (own_project.as_deref(), location),
        (
            service_account_key.as_ref(),
            impersonate_service_account.as_deref(),
        ),
        (base_url.as_deref(), storage_url.as_deref()),
    )?;
    Ok(BigQueryDestination {
        project: project.map_or_else(|| api.project.clone(), str::to_owned),
        api,
        ddl: Arc::default(),
    })
}
