//! Open an `AppendRows` call on a write stream.

use tonic::codegen::http::uri::PathAndQuery;

use crate::bigquery_api::_helpers::grpc::{authorized, channel, status_error};
use crate::bigquery_api::_helpers::requests::access_token;
use crate::bigquery_api::constants::{APPEND_ROWS, APPENDS_IN_FLIGHT};
use crate::bigquery_api::models::{
    AppendRowsRequest, AppendRowsResponse, AppendStream, BigQueryClient,
};
use crate::http::errors::ApiError;

/// Open the bidirectional `AppendRows` call on `first`'s stream, sending `first` (which names the stream and carries the writer schema) before waiting for the call to start; later requests go through the returned sender, at most [`APPENDS_IN_FLIGHT`] waiting.
pub async fn append_rows(
    client: &BigQueryClient,
    first: AppendRowsRequest,
) -> Result<AppendStream, ApiError> {
    let params = format!("write_stream={}", first.write_stream);
    let token = access_token(client, None).await?;
    let (requests, receiver) = tokio::sync::mpsc::channel(APPENDS_IN_FLIGHT);
    requests
        .send(first)
        .await
        .map_err(|_| ApiError::Protocol("the append stream closed before it opened".to_owned()))?;
    let outgoing = futures_util::stream::unfold(receiver, |mut receiver| async move {
        receiver.recv().await.map(|request| (request, receiver))
    });
    let mut grpc = tonic::client::Grpc::new(channel(client).await?);
    grpc.ready()
        .await
        .map_err(|error| ApiError::Transport(error.to_string()))?;
    let response = grpc
        .streaming(
            authorized(outgoing, &token, &params)?,
            PathAndQuery::from_static(APPEND_ROWS),
            tonic_prost::ProstCodec::<AppendRowsRequest, AppendRowsResponse>::default(),
        )
        .await
        .map_err(|status| status_error(&status))?;
    Ok(AppendStream {
        requests,
        responses: response.into_inner(),
    })
}
