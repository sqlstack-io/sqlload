//! A query job reading Parquet files in GCS through a temporary external table.

use serde_json::{Value, json};

use crate::adapters::bigquery::constants::{GCS_FILES_TABLE, PARQUET_DECIMAL_TYPES};

/// The `jobs.insert` body of a query job `job_id` in `location` running `query` over the Parquet files `uris` as the temporary external table [`GCS_FILES_TABLE`]: decimals read as the first of [`PARQUET_DECIMAL_TYPES`] that holds them, lists as their own repeated fields rather than inferred arrays, enums as text.
pub(crate) fn files_query_job(
    (job_id, location): (&str, &str),
    query: &str,
    uris: &[String],
) -> Value {
    json!({
        "jobReference": {"jobId": job_id, "location": location},
        "configuration": {"query": {
            "query": query,
            "useLegacySql": false,
            "tableDefinitions": {GCS_FILES_TABLE: {
                "sourceFormat": "PARQUET",
                "sourceUris": uris,
                "decimalTargetTypes": PARQUET_DECIMAL_TYPES,
                "parquetOptions": {"enumAsString": true, "enableListInference": false},
            }},
        }},
    })
}
