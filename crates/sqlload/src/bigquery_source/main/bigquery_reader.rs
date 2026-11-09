//! A reader for a `bigquery` source's connection.

use crate::bigquery_api::main::parse_key::parse_key;
use crate::bigquery_api::models::BigQueryClient;
use crate::bigquery_source::errors::BigQuerySourceError;
use crate::bigquery_source::models::BigQueryReader;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::project::models::ConnectionConfig;
use crate::time_text::main::now_ms::now_ms;

/// A reader for a `bigquery` connection whose tables live in `project` (the connection's own project, or the key's, when `None`); jobs and read sessions are billed to the connection's project, and the tables it reads directly are read as of now, when it is built (once per table load). Nothing is sent yet.
pub fn bigquery_reader(
    connection: &ConnectionConfig,
    project: Option<&str>,
) -> Result<BigQueryReader, BigQuerySourceError> {
    let ConnectionConfig::BigQuery {
        project: own_project,
        location,
        service_account_key,
        base_url,
        storage_url,
    } = connection
    else {
        return Err(BigQuerySourceError::Invalid(format!(
            "a {} connection is not a bigquery one",
            connection.kind()
        )));
    };
    let key = parse_key(service_account_key.expose())?;
    let job_project = own_project
        .clone()
        .unwrap_or_else(|| key.project_id.clone());
    let http = build_http_client(&ConnectionCounter::default())?;
    Ok(BigQueryReader {
        project: project.map_or_else(|| job_project.clone(), str::to_owned),
        client: BigQueryClient::new(
            http,
            (&job_project, location),
            key,
            (base_url.as_deref(), storage_url.as_deref()),
        ),
        snapshot_ms: now_ms(),
    })
}
