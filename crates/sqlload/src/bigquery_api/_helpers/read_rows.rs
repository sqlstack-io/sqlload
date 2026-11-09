//! The pieces of reading a Storage Read API stream: its schema, the server stream, and decoding its batches.

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use tonic::Streaming;
use tonic::codegen::http::uri::PathAndQuery;

use crate::auth::models::Secret;
use crate::bigquery_api::_helpers::grpc::{authorized, channel};
use crate::bigquery_api::constants::READ_ROWS;
use crate::bigquery_api::models::{BigQueryClient, ReadRowsRequest, ReadRowsResponse};
use crate::http::errors::ApiError;

/// A decoder primed with the session's serialized Arrow `schema`.
pub(crate) fn schema_decoder(schema: &[u8]) -> Result<StreamDecoder, ApiError> {
    let mut decoder = StreamDecoder::new();
    let mut buffer = Buffer::from(schema.to_vec());
    decoder
        .decode(&mut buffer)
        .map_err(|error| ApiError::Protocol(format!("the read session's schema: {error}")))?;
    Ok(decoder)
}

/// `stream`'s rows from row `offset` on, as a server stream; `Ok(Err(status))` when the call is refused.
pub(crate) async fn open_rows(
    client: &BigQueryClient,
    token: &Secret,
    (stream, offset): (&str, u64),
) -> Result<Result<Streaming<ReadRowsResponse>, tonic::Status>, ApiError> {
    let mut grpc = tonic::client::Grpc::new(channel(client).await?);
    grpc.ready()
        .await
        .map_err(|error| ApiError::Transport(error.to_string()))?;
    let request = authorized(
        ReadRowsRequest {
            read_stream: stream.to_owned(),
            offset: i64::try_from(offset).unwrap_or(i64::MAX),
        },
        token,
        &format!("read_stream={stream}"),
    )?;
    let opened = grpc
        .server_streaming(
            request,
            PathAndQuery::from_static(READ_ROWS),
            tonic_prost::ProstCodec::<ReadRowsRequest, ReadRowsResponse>::default(),
        )
        .await;
    Ok(opened.map(tonic::Response::into_inner))
}

/// Decode every batch `messages` sends and pass it to `on_batch`, counting rows into `rows`; `Ok(None)` when the stream ended, `Ok(Some(status))` when it broke.
pub(crate) async fn pass_rows<F>(
    messages: &mut Streaming<ReadRowsResponse>,
    (decoder, rows): (&mut StreamDecoder, &mut u64),
    on_batch: &mut F,
) -> Result<Option<tonic::Status>, ApiError>
where
    F: AsyncFnMut(RecordBatch) -> Result<(), String>,
{
    loop {
        let message = match messages.message().await {
            Ok(Some(message)) => message,
            Ok(None) => return Ok(None),
            Err(status) => return Ok(Some(status)),
        };
        let Some(arrow) = message.arrow_record_batch else {
            continue;
        };
        let mut buffer = Buffer::from(arrow.serialized_record_batch);
        while !buffer.is_empty() {
            let decoded = decoder
                .decode(&mut buffer)
                .map_err(|error| ApiError::Protocol(format!("a read batch: {error}")))?;
            let Some(batch) = decoded else {
                break;
            };
            *rows += batch.num_rows() as u64;
            on_batch(batch).await.map_err(ApiError::Protocol)?;
        }
    }
}
