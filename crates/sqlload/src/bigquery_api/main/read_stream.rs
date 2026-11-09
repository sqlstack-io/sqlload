//! Read one Storage Read API stream as Arrow batches.

use arrow_array::RecordBatch;

use crate::auth::models::Secret;
use crate::bigquery_api::_helpers::grpc::status_error;
use crate::bigquery_api::_helpers::read_rows::{open_rows, pass_rows, schema_decoder};
use crate::bigquery_api::_helpers::requests::access_token;
use crate::bigquery_api::constants::READ_RESUMES;
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;

/// Every row of `stream`, decoded with the session's serialized Arrow `schema` and passed to `on_batch` in order; returns the row count. A stream that breaks with a retryable status resumes after the rows already passed on, at most [`READ_RESUMES`] times, and a refused token is replaced once.
pub async fn read_stream(
    client: &BigQueryClient,
    (stream, schema): (&str, &[u8]),
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, ApiError> {
    let mut decoder = schema_decoder(schema)?;
    let mut rows: u64 = 0;
    let mut resumes: u32 = 0;
    let mut refused: Option<Secret> = None;
    loop {
        let token = access_token(client, refused.as_ref()).await?;
        let broken = match open_rows(client, &token, (stream, rows)).await? {
            Ok(mut messages) => {
                match pass_rows(&mut messages, (&mut decoder, &mut rows), &mut on_batch).await? {
                    None => return Ok(rows),
                    Some(status) => status,
                }
            }
            Err(status) => status,
        };
        match broken.code() {
            tonic::Code::Unauthenticated if refused.is_none() => refused = Some(token),
            tonic::Code::Unavailable | tonic::Code::Internal | tonic::Code::DeadlineExceeded
                if resumes < READ_RESUMES =>
            {
                resumes += 1;
            }
            _ => return Err(status_error(&broken)),
        }
    }
}
