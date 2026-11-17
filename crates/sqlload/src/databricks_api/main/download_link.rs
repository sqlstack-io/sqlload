//! Download one result file through its link.

use crate::databricks_api::models::{DatabricksClient, ResultLink};
use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;

/// The bytes behind `link`, fetched without the Databricks token (the link carries its own credential) and with the headers the link names.
pub async fn download_link(
    client: &DatabricksClient,
    link: &ResultLink,
) -> Result<bytes::Bytes, ApiError> {
    let mut request = client.http.get(&link.url);
    for (name, value) in &link.headers {
        request = request.header(name, value);
    }
    let response = check_response(request.send().await?).await?;
    Ok(response.bytes().await?)
}
