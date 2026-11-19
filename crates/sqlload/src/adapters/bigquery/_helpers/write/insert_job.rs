//! One `INSERT ... SELECT` BigQuery runs as a query job, counting the rows it inserted.

use serde_json::{Value, json};

use crate::bigquery_api::main::load_job::load_job;
use crate::bigquery_api::models::BigQueryClient;
use crate::loader::errors::LoaderError;

/// Run `query` as one query job, with its temporary external tables `definitions` when it reads files: the rows it inserted. A job BigQuery fails, or whose count is not a number, fails the batch; `what` names what the job loads in the error.
pub(in crate::adapters::bigquery) async fn insert_job(
    api: &BigQueryClient,
    query: &str,
    definitions: Option<Value>,
    what: &str,
) -> Result<u64, LoaderError> {
    let mut configuration = json!({"query": query, "useLegacySql": false});
    if let Some(definitions) = definitions {
        configuration["tableDefinitions"] = definitions;
    }
    let job = json!({
        "jobReference": {
            "projectId": api.project,
            "jobId": format!("sqlload_insert_{}", uuid::Uuid::new_v4().simple()),
            "location": api.location,
        },
        "configuration": {"query": configuration},
    });
    let resource = load_job(api, &job, None).await?;
    let error = &resource["status"]["errorResult"];
    if !error.is_null() {
        return Err(LoaderError::Invalid(format!(
            "BigQuery could not load {what}: {}: {}",
            error["reason"].as_str().unwrap_or_default(),
            error["message"].as_str().unwrap_or_default()
        )));
    }
    let affected = resource["statistics"]["query"]["numDmlAffectedRows"]
        .as_str()
        .unwrap_or_default();
    affected.parse().map_err(|_| {
        LoaderError::Invalid(format!(
            "BigQuery's query job reported {affected:?} inserted rows, not a count; the batch is not committed"
        ))
    })
}
