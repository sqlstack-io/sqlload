//! A reader for a `bigquery` source's connection.

use crate::bigquery_api::main::signed_in_client::signed_in_client;
use crate::bigquery_source::errors::BigQuerySourceError;
use crate::bigquery_source::models::BigQueryReader;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::project::models::ConnectionConfig;
use crate::time_text::main::now_ms::now_ms;

/// A reader for a `bigquery` connection whose tables live in `project` (the connection's own project, or the credentials', when `None`); jobs and read sessions are billed to the connection's project, and the tables it reads directly are read as of now, when it is built (once per table load). Nothing is sent yet.
pub fn bigquery_reader(
    connection: &ConnectionConfig,
    project: Option<&str>,
) -> Result<BigQueryReader, BigQuerySourceError> {
    let ConnectionConfig::BigQuery {
        project: own_project,
        location,
        service_account_key,
        impersonate_service_account,
        base_url,
        storage_url,
    } = connection
    else {
        return Err(BigQuerySourceError::Invalid(format!(
            "a {} connection is not a bigquery one",
            connection.kind()
        )));
    };
    let client = signed_in_client(
        build_http_client(&ConnectionCounter::default())?,
        (own_project.as_deref(), location),
        (
            service_account_key.as_ref(),
            impersonate_service_account.as_deref(),
        ),
        (base_url.as_deref(), storage_url.as_deref()),
    )?;
    Ok(BigQueryReader {
        project: project.map_or_else(|| client.project.clone(), str::to_owned),
        client,
        snapshot_ms: now_ms(),
    })
}
