//! Whether BigQuery can read a files table's Parquet files in GCS itself.

use crate::adapters::bigquery::_helpers::write::files_query_job::files_query_job;
use crate::adapters::bigquery::constants::GCS_FILES_TABLE;
use crate::adapters::bigquery::models::BigQueryDestination;
use crate::bigquery_api::main::load_job::load_job;

/// Check, at plan, that BigQuery reads `file`, the first Parquet file of a table it would load from GCS itself, as the connection's service account, by counting the file's rows in a query job over it; the error says why it can't, with the fix.
pub(in crate::adapters) async fn check_file(
    destination: &BigQueryDestination,
    file: &str,
) -> Result<(), String> {
    let api = &destination.api;
    let job_id = format!("sqlload_check_{}", uuid::Uuid::new_v4().simple());
    let job = files_query_job(
        (&job_id, api.location()),
        &format!("SELECT COUNT(*) FROM {GCS_FILES_TABLE}"),
        &[file.to_owned()],
    );
    let failure = match load_job(api, &job, None).await {
        Ok(resource) => resource["status"]["errorResult"]["message"]
            .as_str()
            .map(str::to_owned),
        Err(error) => Some(error.to_string()),
    };
    match failure {
        None => Ok(()),
        Some(reason) => Err(format!(
            "{file} cannot be read by BigQuery: {reason}. Fix: grant {} the Storage Object Viewer role on the bucket",
            api.service_account()
        )),
    }
}
