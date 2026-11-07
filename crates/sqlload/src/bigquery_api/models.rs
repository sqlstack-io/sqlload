use std::sync::Arc;

use crate::auth::models::Secret;

/// The fields of a service-account key file that sqlload uses.
#[derive(Debug, Clone)]
pub struct ServiceAccountKey {
    pub client_email: String,
    /// The PEM private key.
    pub private_key: Secret,
    /// Where tokens are exchanged for signed assertions.
    pub token_uri: String,
    /// The project the key belongs to, used when the connection names none.
    pub project_id: String,
}

/// What the BigQuery REST API needs: the shared HTTP pool, the project jobs run in and their location, the key, and the token it last gave.
#[derive(Debug, Clone)]
pub struct BigQueryClient {
    pub(crate) http: reqwest::Client,
    /// The REST root, with no trailing `/`.
    pub(crate) base_url: String,
    pub(crate) project: String,
    pub(crate) location: String,
    pub(crate) key: ServiceAccountKey,
    /// The current access token and when it expires, in milliseconds since the epoch.
    pub(crate) token: Arc<tokio::sync::Mutex<Option<(Secret, i64)>>>,
    /// The Storage Write API's root, with its scheme.
    pub(crate) storage_url: String,
    /// The Storage Write API's gRPC channel, opened on its first call.
    pub(crate) channel: Arc<tokio::sync::OnceCell<tonic::transport::Channel>>,
}

impl BigQueryClient {
    /// Where the client's jobs run.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// A client running jobs in `project` at `location`, against the REST root `base_url` and the Storage Write API at `storage_url` (the public ones when `None`); nothing is sent until the first request.
    pub fn new(
        http: reqwest::Client,
        (project, location): (&str, &str),
        key: ServiceAccountKey,
        (base_url, storage_url): (Option<&str>, Option<&str>),
    ) -> Self {
        Self {
            http,
            base_url: base_url
                .unwrap_or(crate::bigquery_api::constants::BASE_URL)
                .trim_end_matches('/')
                .to_owned(),
            project: project.to_owned(),
            location: location.to_owned(),
            key,
            token: Arc::default(),
            storage_url: storage_url
                .unwrap_or(crate::bigquery_api::constants::STORAGE_URL)
                .trim_end_matches('/')
                .to_owned(),
            channel: Arc::default(),
        }
    }
}

/// `google.protobuf.Int64Value`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Int64Value {
    #[prost(int64, tag = "1")]
    pub value: i64,
}

/// `google.protobuf.Timestamp`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Timestamp {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}

/// `google.rpc.Status`, without its details.
#[derive(Clone, PartialEq, prost::Message)]
pub struct RpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
}

/// `WriteStream`: a stream's name and type ([`PENDING_STREAM`](crate::bigquery_api::constants::PENDING_STREAM) for rows that commit together later).
#[derive(Clone, PartialEq, prost::Message)]
pub struct WriteStream {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(int32, tag = "2")]
    pub r#type: i32,
}

/// `CreateWriteStreamRequest`: a new stream on the table `parent` (`projects/p/datasets/d/tables/t`).
#[derive(Clone, PartialEq, prost::Message)]
pub struct CreateWriteStreamRequest {
    #[prost(string, tag = "1")]
    pub parent: String,
    #[prost(message, optional, tag = "2")]
    pub write_stream: Option<WriteStream>,
}

/// `ArrowSchema`: an encapsulated Arrow IPC schema message.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ArrowSchema {
    #[prost(bytes = "vec", tag = "1")]
    pub serialized_schema: Vec<u8>,
}

/// `ArrowRecordBatch`: an encapsulated Arrow IPC record batch message.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ArrowRecordBatch {
    #[prost(bytes = "vec", tag = "1")]
    pub serialized_record_batch: Vec<u8>,
}

/// `AppendRowsRequest.ArrowData`: the rows, with the schema on a stream's first request.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ArrowData {
    #[prost(message, optional, tag = "1")]
    pub writer_schema: Option<ArrowSchema>,
    #[prost(message, optional, tag = "2")]
    pub rows: Option<ArrowRecordBatch>,
}

/// `AppendRowsRequest` with Arrow rows (field 5 of its `rows` oneof), at the stream row `offset` they start at.
#[derive(Clone, PartialEq, prost::Message)]
pub struct AppendRowsRequest {
    #[prost(string, tag = "1")]
    pub write_stream: String,
    #[prost(message, optional, tag = "2")]
    pub offset: Option<Int64Value>,
    #[prost(message, optional, tag = "5")]
    pub arrow_rows: Option<ArrowData>,
    #[prost(string, tag = "6")]
    pub trace_id: String,
}

/// `AppendRowsResponse.AppendResult`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct AppendResult {
    #[prost(message, optional, tag = "1")]
    pub offset: Option<Int64Value>,
}

/// `RowError`: a row of a request that BigQuery refused, and why.
#[derive(Clone, PartialEq, prost::Message)]
pub struct RowError {
    #[prost(int64, tag = "1")]
    pub index: i64,
    #[prost(int32, tag = "2")]
    pub code: i32,
    #[prost(string, tag = "3")]
    pub message: String,
}

/// `AppendRowsResponse`: the request's result or its error (fields 1 and 2 of its `response` oneof), and refused rows.
#[derive(Clone, PartialEq, prost::Message)]
pub struct AppendRowsResponse {
    #[prost(message, optional, tag = "1")]
    pub append_result: Option<AppendResult>,
    #[prost(message, optional, tag = "2")]
    pub error: Option<RpcStatus>,
    #[prost(message, repeated, tag = "4")]
    pub row_errors: Vec<RowError>,
    #[prost(string, tag = "5")]
    pub write_stream: String,
}

/// `FinalizeWriteStreamRequest`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct FinalizeWriteStreamRequest {
    #[prost(string, tag = "1")]
    pub name: String,
}

/// `FinalizeWriteStreamResponse`.
#[derive(Clone, PartialEq, prost::Message)]
pub struct FinalizeWriteStreamResponse {
    #[prost(int64, tag = "1")]
    pub row_count: i64,
}

/// `BatchCommitWriteStreamsRequest`: finalized pending streams of the table `parent`, committed at once.
#[derive(Clone, PartialEq, prost::Message)]
pub struct BatchCommitWriteStreamsRequest {
    #[prost(string, tag = "1")]
    pub parent: String,
    #[prost(string, repeated, tag = "2")]
    pub write_streams: Vec<String>,
}

/// `StorageError`: why a stream could not commit.
#[derive(Clone, PartialEq, prost::Message)]
pub struct StorageError {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub entity: String,
    #[prost(string, tag = "3")]
    pub error_message: String,
}

/// `BatchCommitWriteStreamsResponse`: when the streams committed, or why none did.
#[derive(Clone, PartialEq, prost::Message)]
pub struct BatchCommitWriteStreamsResponse {
    #[prost(message, optional, tag = "1")]
    pub commit_time: Option<Timestamp>,
    #[prost(message, repeated, tag = "2")]
    pub stream_errors: Vec<StorageError>,
}

/// An open `AppendRows` call: requests go in through `requests`, and each request's answer comes back, in order, on `responses`.
#[derive(Debug)]
pub struct AppendStream {
    pub requests: tokio::sync::mpsc::Sender<AppendRowsRequest>,
    pub responses: tonic::Streaming<AppendRowsResponse>,
}
