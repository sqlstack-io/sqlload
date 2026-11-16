//! Fetch a fresh link to one chunk of a finished statement's result.

use crate::databricks_api::_helpers::requests::json_with_retries;
use crate::databricks_api::_helpers::statements::result_links;
use crate::databricks_api::constants::STATEMENTS_PATH;
use crate::databricks_api::models::{DatabricksClient, ResultLink};
use crate::http::errors::ApiError;

/// A new link to chunk `chunk_index` of statement `statement_id`'s `EXTERNAL_LINKS` result, as a chunk's link expires within 15 minutes; retried like any read.
pub async fn result_chunk(
    client: &DatabricksClient,
    (statement_id, chunk_index): (&str, usize),
) -> Result<ResultLink, ApiError> {
    let url = format!(
        "{}{STATEMENTS_PATH}/{statement_id}/result/chunks/{chunk_index}",
        client.host
    );
    let chunk = json_with_retries(client, &url).await?;
    result_links(&chunk)
        .into_iter()
        .find(|link| link.chunk_index == chunk_index)
        .ok_or_else(|| {
            ApiError::Protocol(format!(
                "Databricks gave no link to result chunk {chunk_index}"
            ))
        })
}
