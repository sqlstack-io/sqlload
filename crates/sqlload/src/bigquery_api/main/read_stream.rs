//! Read one Storage Read API stream as Arrow batches.

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use tonic::codegen::http::uri::PathAndQuery;

use crate::auth::models::Secret;
use crate::bigquery_api::_helpers::grpc::{authorized, channel, status_error};
use crate::bigquery_api::_helpers::requests::access_token;
use crate::bigquery_api::constants::{READ_RESUMES, READ_ROWS};
use crate::bigquery_api::models::{BigQueryClient, ReadRowsRequest, ReadRowsResponse};
use crate::http::errors::ApiError;

/// Every row of `stream`, decoded with the session's serialized Arrow `schema` and passed to `on_batch` in order; returns the row count. A stream that breaks with a retryable status resumes after the rows already passed on, at most [`READ_RESUMES`] times, and a refused token is replaced once.
pub async fn read_stream(
    client: &BigQueryClient,
    (stream, schema): (&str, &[u8]),
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, ApiError> {
    let mut decoder = StreamDecoder::new();
    let mut schema_buffer = Buffer::from(schema.to_vec());
    decoder
        .decode(&mut schema_buffer)
        .map_err(|error| ApiError::Protocol(format!("the read session's schema: {error}")))?;
    let mut rows: u64 = 0;
    let mut resumes: u32 = 0;
    let mut refused: Option<Secret> = None;
    loop {
        let token = access_token(client, refused.as_ref()).await?;
        let mut grpc = tonic::client::Grpc::new(channel(client).await?);
        grpc.ready()
            .await
            .map_err(|error| ApiError::Transport(error.to_string()))?;
        let request = authorized(
            ReadRowsRequest {
                read_stream: stream.to_owned(),
                offset: i64::try_from(rows).unwrap_or(i64::MAX),
            },
            &token,
            &format!("read_stream={stream}"),
        )?;
        let opened = grpc
            .server_streaming(
                request,
                PathAndQuery::from_static(READ_ROWS),
                tonic_prost::ProstCodec::<ReadRowsRequest, ReadRowsResponse>::default(),
            )
            .await;
        let broken = match opened {
            Ok(response) => {
                let mut messages = response.into_inner();
                loop {
                    match messages.message().await {
                        Ok(Some(message)) => {
                            let Some(arrow) = message.arrow_record_batch else {
                                continue;
                            };
                            let mut buffer = Buffer::from(arrow.serialized_record_batch);
                            while !buffer.is_empty() {
                                let Some(batch) = decoder.decode(&mut buffer).map_err(|error| {
                                    ApiError::Protocol(format!("a read batch: {error}"))
                                })?
                                else {
                                    break;
                                };
                                rows += batch.num_rows() as u64;
                                on_batch(batch).await.map_err(ApiError::Protocol)?;
                            }
                        }
                        Ok(None) => return Ok(rows),
                        Err(status) => break status,
                    }
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
