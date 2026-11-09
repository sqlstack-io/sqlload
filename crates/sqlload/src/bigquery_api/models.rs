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
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct Int64Value {
    #[prost(int64, tag = "1")]
    pub value: i64,
}

/// `google.protobuf.Timestamp`.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct Timestamp {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}

/// `google.rpc.Status`, without its details.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct RpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
}

/// `WriteStream`: a stream's name and type ([`PENDING_STREAM`](crate::bigquery_api::constants::PENDING_STREAM) for rows that commit together later).
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct WriteStream {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(int32, tag = "2")]
    pub r#type: i32,
}

/// `CreateWriteStreamRequest`: a new stream on the table `parent` (`projects/p/datasets/d/tables/t`).
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct CreateWriteStreamRequest {
    #[prost(string, tag = "1")]
    pub parent: String,
    #[prost(message, optional, tag = "2")]
    pub write_stream: Option<WriteStream>,
}

/// `ArrowSchema`: an encapsulated Arrow IPC schema message.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct ArrowSchema {
    #[prost(bytes = "vec", tag = "1")]
    pub serialized_schema: Vec<u8>,
}

/// `ArrowRecordBatch`: an encapsulated Arrow IPC record batch message.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct ArrowRecordBatch {
    #[prost(bytes = "vec", tag = "1")]
    pub serialized_record_batch: Vec<u8>,
}

/// `AppendRowsRequest.ArrowData`: the rows, with the schema on a stream's first request.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct ArrowData {
    #[prost(message, optional, tag = "1")]
    pub writer_schema: Option<ArrowSchema>,
    #[prost(message, optional, tag = "2")]
    pub rows: Option<ArrowRecordBatch>,
}

/// `AppendRowsRequest` with Arrow rows (field 5 of its `rows` oneof), at the stream row `offset` they start at.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
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
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct AppendResult {
    #[prost(message, optional, tag = "1")]
    pub offset: Option<Int64Value>,
}

/// `RowError`: a row of a request that BigQuery refused, and why.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct RefusedRow {
    #[prost(int64, tag = "1")]
    pub index: i64,
    #[prost(int32, tag = "2")]
    pub code: i32,
    #[prost(string, tag = "3")]
    pub message: String,
}

/// `AppendRowsResponse`: the request's result or its error (fields 1 and 2 of its `response` oneof), and refused rows.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct AppendRowsResponse {
    #[prost(message, optional, tag = "1")]
    pub append_result: Option<AppendResult>,
    #[prost(message, optional, tag = "2")]
    pub error: Option<RpcStatus>,
    #[prost(message, repeated, tag = "4")]
    pub row_errors: Vec<RefusedRow>,
    #[prost(string, tag = "5")]
    pub write_stream: String,
}

/// `FinalizeWriteStreamRequest`.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct FinalizeWriteStreamRequest {
    #[prost(string, tag = "1")]
    pub name: String,
}

/// `FinalizeWriteStreamResponse`.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct FinalizeWriteStreamResponse {
    #[prost(int64, tag = "1")]
    pub row_count: i64,
}

/// `BatchCommitWriteStreamsRequest`: finalized pending streams of the table `parent`, committed at once.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct BatchCommitWriteStreamsRequest {
    #[prost(string, tag = "1")]
    pub parent: String,
    #[prost(string, repeated, tag = "2")]
    pub write_streams: Vec<String>,
}

/// `StorageError`: why a stream could not commit.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct StreamRefusal {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub entity: String,
    #[prost(string, tag = "3")]
    pub error_message: String,
}

/// `BatchCommitWriteStreamsResponse`: when the streams committed, or why none did.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct BatchCommitWriteStreamsResponse {
    #[prost(message, optional, tag = "1")]
    pub commit_time: Option<Timestamp>,
    #[prost(message, repeated, tag = "2")]
    pub stream_errors: Vec<StreamRefusal>,
}

/// An open `AppendRows` call: requests go in through `requests`, and each request's answer comes back, in order, on `responses`.
#[derive(Debug)]
pub struct AppendStream {
    pub requests: tokio::sync::mpsc::Sender<AppendRowsRequest>,
    pub responses: tonic::Streaming<AppendRowsResponse>,
}

/// `ReadSession`: what a Storage Read API session reads and how, and, once created, its name, schema and streams.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct ReadSession {
    #[prost(string, tag = "1")]
    pub name: String,
    /// `DataFormat`: [`ARROW_FORMAT`](crate::bigquery_api::constants::ARROW_FORMAT).
    #[prost(int32, tag = "3")]
    pub data_format: i32,
    #[prost(message, optional, tag = "5")]
    pub arrow_schema: Option<ArrowSchema>,
    /// The table read, as `projects/p/datasets/d/tables/t`.
    #[prost(string, tag = "6")]
    pub table: String,
    #[prost(message, optional, tag = "7")]
    pub table_modifiers: Option<TableModifiers>,
    #[prost(message, optional, tag = "8")]
    pub read_options: Option<TableReadOptions>,
    #[prost(message, repeated, tag = "10")]
    pub streams: Vec<ReadStream>,
    #[prost(int64, tag = "12")]
    pub estimated_total_bytes_scanned: i64,
    #[prost(int64, tag = "14")]
    pub estimated_row_count: i64,
}

/// `ReadSession.TableModifiers`: the instant the table is read as of.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct TableModifiers {
    #[prost(message, optional, tag = "1")]
    pub snapshot_time: Option<Timestamp>,
}

/// `ReadSession.TableReadOptions`: the columns read, a filter on the rows, and how Arrow buffers are compressed.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct TableReadOptions {
    #[prost(string, repeated, tag = "1")]
    pub selected_fields: Vec<String>,
    /// A GoogleSQL condition on the table's columns; empty reads every row.
    #[prost(string, tag = "2")]
    pub row_restriction: String,
    #[prost(message, optional, tag = "3")]
    pub arrow_serialization_options: Option<ArrowSerializationOptions>,
}

/// `ArrowSerializationOptions`: the codec of the batches' buffers ([`LZ4_FRAME`](crate::bigquery_api::constants::LZ4_FRAME)).
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct ArrowSerializationOptions {
    #[prost(int32, tag = "2")]
    pub buffer_compression: i32,
}

/// `ReadStream`: one of a session's streams, by name.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct ReadStream {
    #[prost(string, tag = "1")]
    pub name: String,
}

/// `CreateReadSessionRequest`: a session billed to the project `parent` (`projects/p`), split into at most `max_stream_count` streams.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct CreateReadSessionRequest {
    #[prost(string, tag = "1")]
    pub parent: String,
    #[prost(message, optional, tag = "2")]
    pub read_session: Option<ReadSession>,
    #[prost(int32, tag = "3")]
    pub max_stream_count: i32,
}

/// `ReadRowsRequest`: a stream's rows from row `offset` on.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct ReadRowsRequest {
    #[prost(string, tag = "1")]
    pub read_stream: String,
    #[prost(int64, tag = "2")]
    pub offset: i64,
}

/// `ReadRowsResponse` with Arrow rows: one record batch and its row count.
#[derive(Clone, PartialEq, Debug, prost::Message)]
#[prost(skip_debug)]
pub struct ReadRowsResponse {
    #[prost(message, optional, tag = "4")]
    pub arrow_record_batch: Option<ArrowRecordBatch>,
    #[prost(int64, tag = "6")]
    pub row_count: i64,
}

/// What a read session reads: the columns, a GoogleSQL filter on them, the instant (milliseconds since the epoch; `None` for now), and the most streams to split it into.
#[derive(Debug, Clone)]
pub struct ReadSessionRequest<'a> {
    pub fields: &'a [String],
    pub restriction: Option<&'a str>,
    pub snapshot_ms: Option<i64>,
    pub max_streams: i32,
}
