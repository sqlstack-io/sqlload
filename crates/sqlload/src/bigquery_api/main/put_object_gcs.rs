//! Upload an object to Cloud Storage.

use crate::bigquery_api::_helpers::requests::{google_response, token_for};
use crate::bigquery_api::constants::{STORAGE_SCOPE, STORAGE_UPLOAD_URL, UNAUTHORIZED};
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;

/// Write `body` as the object `name` in `bucket`, replacing one of that name, as the client's service account with a Cloud Storage token; a refused token is replaced once.
pub async fn put_object_gcs(
    client: &BigQueryClient,
    (bucket, name): (&str, &str),
    body: bytes::Bytes,
) -> Result<(), ApiError> {
    let url = format!("{STORAGE_UPLOAD_URL}/b/{bucket}/o");
    let mut refused = None;
    loop {
        let token = token_for(
            &client.http,
            &client.key,
            &client.storage_token,
            (STORAGE_SCOPE, refused.as_ref()),
        )
        .await?;
        let response = client
            .http
            .post(&url)
            .query(&[("uploadType", "media"), ("name", name)])
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .bearer_auth(token.expose())
            .body(body.clone())
            .send()
            .await?;
        if response.status().as_u16() == UNAUTHORIZED && refused.is_none() {
            refused = Some(token);
            continue;
        }
        google_response(response).await?;
        return Ok(());
    }
}
