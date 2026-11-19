//! Whether BigQuery reads a Parquet file in GCS itself.

use serde_json::json;

use crate::adapters::bigquery::_helpers::dialect::pull::{group_table, table_definition};
use crate::adapters::bigquery::models::BigQueryDestination;
use crate::bigquery_api::main::query_schema::query_schema;
use crate::http::errors::ApiError;

/// Check that BigQuery reads the Parquet file at `url` with its own identity (its connection's service account), by a dry run of a query over it as a temporary external table: BigQuery reads the file's schema, which needs `storage.objects.get` on the bucket, and bills nothing.
pub(in crate::adapters) async fn check_files(
    destination: &BigQueryDestination,
    url: &str,
) -> Result<(), ApiError> {
    let table = group_table(0);
    let definitions = json!({ table.clone(): table_definition(&[url]) });
    query_schema(
        &destination.api,
        &format!("SELECT * FROM {table}"),
        Some(&definitions),
    )
    .await
    .map(|_| ())
}
