//! Storage Write API calls over gRPC, authorized with the client's token and routed by their resource.

use prost::Message;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::auth::models::Secret;
use crate::bigquery_api::_helpers::requests::access_token;
use crate::bigquery_api::constants::REQUEST_PARAMS;
use crate::bigquery_api::models::BigQueryClient;
use crate::http::errors::ApiError;

/// The client's channel to the Storage Write API, opened (lazily, with TLS for `https`) on the first call.
pub(crate) async fn channel(client: &BigQueryClient) -> Result<Channel, ApiError> {
    let opened = client
        .channel
        .get_or_try_init(|| async {
            let endpoint = Endpoint::from_shared(client.storage_url.clone()).map_err(|error| {
                ApiError::Protocol(format!("bad Storage Write API URL: {error}"))
            })?;
            let endpoint = match client.storage_url.starts_with("https://") {
                true => endpoint
                    .tls_config(ClientTlsConfig::new().with_webpki_roots())
                    .map_err(|error| ApiError::Transport(error.to_string()))?,
                false => endpoint,
            };
            Ok::<Channel, ApiError>(endpoint.connect_lazy())
        })
        .await?;
    Ok(opened.clone())
}

/// `message` as a gRPC request carrying the bearer `token` and the routing `params` (`parent=...`, `name=...`, `write_stream=...`).
pub(crate) fn authorized<T>(
    message: T,
    token: &Secret,
    params: &str,
) -> Result<tonic::Request<T>, ApiError> {
    let mut request = tonic::Request::new(message);
    let mut bearer = MetadataValue::try_from(format!("Bearer {}", token.expose()))
        .map_err(|error| ApiError::Protocol(error.to_string()))?;
    bearer.set_sensitive(true);
    let routed =
        MetadataValue::try_from(params).map_err(|error| ApiError::Protocol(error.to_string()))?;
    request.metadata_mut().insert("authorization", bearer);
    request.metadata_mut().insert(REQUEST_PARAMS, routed);
    Ok(request)
}

/// A unary Storage Write API call to `path`, sent once more with a new token if the first was refused as unauthenticated.
pub(crate) async fn unary<Req, Res>(
    client: &BigQueryClient,
    path: &'static str,
    params: &str,
    message: Req,
) -> Result<Res, ApiError>
where
    Req: Message + Clone + 'static,
    Res: Message + Default + 'static,
{
    let mut refused: Option<Secret> = None;
    loop {
        let token = access_token(client, refused.as_ref()).await?;
        let mut grpc = tonic::client::Grpc::new(channel(client).await?);
        grpc.ready()
            .await
            .map_err(|error| ApiError::Transport(error.to_string()))?;
        let request = authorized(message.clone(), &token, params)?;
        let answer = grpc
            .unary(
                request,
                PathAndQuery::from_static(path),
                tonic_prost::ProstCodec::<Req, Res>::default(),
            )
            .await;
        match answer {
            Ok(response) => return Ok(response.into_inner()),
            Err(status) if status.code() == tonic::Code::Unauthenticated && refused.is_none() => {
                refused = Some(token);
            }
            Err(status) => return Err(status_error(&status)),
        }
    }
}

/// A gRPC status as an [`ApiError`] with the HTTP status Google maps its code to, so the same codes are retried.
pub(crate) fn status_error(status: &tonic::Status) -> ApiError {
    let http = match status.code() {
        tonic::Code::InvalidArgument
        | tonic::Code::FailedPrecondition
        | tonic::Code::OutOfRange => 400,
        tonic::Code::Unauthenticated => 401,
        tonic::Code::PermissionDenied => 403,
        tonic::Code::NotFound => 404,
        tonic::Code::Aborted | tonic::Code::AlreadyExists => 409,
        tonic::Code::ResourceExhausted => 429,
        tonic::Code::Cancelled => 499,
        tonic::Code::Unimplemented => 501,
        tonic::Code::Unavailable => 503,
        tonic::Code::DeadlineExceeded => 504,
        _ => 500,
    };
    ApiError::Status {
        status: http,
        code: format!("{:?}", status.code()),
        message: status.message().to_owned(),
    }
}
