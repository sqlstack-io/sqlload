//! The fake's Storage Write API: pending streams created, appended to with Arrow rows, finalized and committed together.

use serde_json::Value;
use sqlload::bigquery_api::constants::{PENDING_STREAM, REQUEST_PARAMS};
use sqlload::bigquery_api::models::{
    AppendResult, AppendRowsRequest, AppendRowsResponse, BatchCommitWriteStreamsRequest,
    BatchCommitWriteStreamsResponse, CreateWriteStreamRequest, FinalizeWriteStreamRequest,
    FinalizeWriteStreamResponse, Int64Value, RefusedRow, RpcStatus, StreamRefusal, Timestamp,
    WriteStream,
};
use tonic::codegen::{BoxFuture, BoxStream, Context, Poll, Service, StdError, http};
use tonic::server::{Grpc, NamedService};
use tonic::{Request, Response, Status, Streaming};
use tonic_prost::ProstCodec;

use crate::fake_bigquery::constants::{
    INVALID_STREAM_STATE, OUT_OF_RANGE, STREAM_NOT_FOUND, TOKEN_PREFIX, WRITE_SERVICE,
};
use crate::fake_bigquery::models::{FakeState, FakeStream, SharedState};

/// The Storage Write API service, answering each call from the shared state.
#[derive(Debug, Clone)]
pub(crate) struct WriteService(pub(crate) SharedState);

impl NamedService for WriteService {
    const NAME: &'static str = WRITE_SERVICE;
}

/// A unary call answered by `handle` while the state is locked.
type Handler<M1, M2> = fn(&mut FakeState, M1) -> Result<M2, Status>;

struct Unary<M1, M2>(SharedState, Handler<M1, M2>);

impl<M1, M2> Service<Request<M1>> for Unary<M1, M2> {
    type Response = Response<M2>;
    type Error = Status;
    type Future = std::future::Ready<Result<Response<M2>, Status>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<M1>) -> Self::Future {
        let answered = (self.1)(&mut self.0.lock(), request.into_inner());
        std::future::ready(answered.map(Response::new))
    }
}

/// An `AppendRows` call: each request is answered in turn, until the client half-closes.
struct Appends(SharedState);

impl Service<Request<Streaming<AppendRowsRequest>>> for Appends {
    type Response = Response<BoxStream<AppendRowsResponse>>;
    type Error = Status;
    type Future = std::future::Ready<Result<Self::Response, Status>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Streaming<AppendRowsRequest>>) -> Self::Future {
        let state = self.0.clone();
        let mut incoming = request.into_inner();
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move {
            while let Ok(Some(append)) = incoming.message().await {
                let answer = state.lock().append_rows(append);
                if let Err(_closed) = sender.send(Ok(answer)).await {
                    return;
                }
            }
        });
        let answers: BoxStream<AppendRowsResponse> = Box::pin(
            tonic::codegen::tokio_stream::wrappers::ReceiverStream::new(receiver),
        );
        std::future::ready(Ok(Response::new(answers)))
    }
}

impl<B> Service<http::Request<B>> for WriteService
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let state = self.0.clone();
        let authorized = request
            .headers()
            .get("authorization")
            .is_some_and(|value| value.as_bytes().starts_with(TOKEN_PREFIX.as_bytes()));
        if !request.headers().contains_key(REQUEST_PARAMS) {
            state.lock().unrouted_calls += 1;
        }
        let method = request
            .uri()
            .path()
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_owned();
        Box::pin(async move {
            if !authorized {
                return Ok(Status::unauthenticated("invalid token").into_http());
            }
            let answered = match method.as_str() {
                "CreateWriteStream" => {
                    Grpc::new(ProstCodec::<WriteStream, CreateWriteStreamRequest>::default())
                        .unary(Unary(state, FakeState::create_write_stream), request)
                        .await
                }
                "AppendRows" => {
                    Grpc::new(ProstCodec::<AppendRowsResponse, AppendRowsRequest>::default())
                        .streaming(Appends(state), request)
                        .await
                }
                "FinalizeWriteStream" => {
                    Grpc::new(ProstCodec::<
                        FinalizeWriteStreamResponse,
                        FinalizeWriteStreamRequest,
                    >::default())
                    .unary(Unary(state, FakeState::finalize_write_stream), request)
                    .await
                }
                "BatchCommitWriteStreams" => {
                    Grpc::new(ProstCodec::<
                        BatchCommitWriteStreamsResponse,
                        BatchCommitWriteStreamsRequest,
                    >::default())
                    .unary(Unary(state, FakeState::batch_commit_write_streams), request)
                    .await
                }
                other => Status::unimplemented(other.to_owned()).into_http(),
            };
            Ok(answered)
        })
    }
}

/// `projects/p/datasets/d/tables/t` (and anything after) → `d.t`, lower-cased.
fn table_of(path: &str) -> String {
    let parts: Vec<&str> = path.split('/').collect();
    match parts.as_slice() {
        [_, _, _, dataset, _, table, ..] => format!("{dataset}.{table}").to_ascii_lowercase(),
        _ => String::new(),
    }
}

/// An append refused as a whole with `code` and `message`.
fn rejected(write_stream: String, code: tonic::Code, message: String) -> AppendRowsResponse {
    AppendRowsResponse {
        append_result: None,
        error: Some(RpcStatus {
            code: code as i32,
            message,
        }),
        row_errors: Vec::new(),
        write_stream,
    }
}

/// The fields of an Arrow IPC schema message the table has no column for, matched case-insensitively as BigQuery does.
fn extra_fields(schema: &[u8], columns: &[(String, String)]) -> Vec<String> {
    let Ok(reader) = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(schema), None)
    else {
        return Vec::new();
    };
    reader
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| !has_column(columns, name))
        .collect()
}

/// Whether the table has a column `name`, matched case-insensitively as BigQuery does.
fn has_column(columns: &[(String, String)], name: &str) -> bool {
    columns
        .iter()
        .any(|(column, _)| column.eq_ignore_ascii_case(name))
}

/// Every row of an Arrow IPC schema and record batch message, each value as its display text or null.
fn decoded_rows(schema: &[u8], rows: &[u8]) -> Result<Vec<Value>, String> {
    let mut bytes = schema.to_vec();
    bytes.extend_from_slice(rows);
    let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|error| error.to_string())?;
    let options = arrow_cast::display::FormatOptions::default();
    let mut decoded: Vec<Value> = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|error| error.to_string())?;
        let schema = batch.schema();
        let mut formatters = Vec::with_capacity(batch.num_columns());
        for column in batch.columns() {
            formatters.push(
                arrow_cast::display::ArrayFormatter::try_new(column, &options)
                    .map_err(|error| error.to_string())?,
            );
        }
        for row in 0..batch.num_rows() {
            let mut object = serde_json::Map::new();
            for ((field, column), formatter) in
                schema.fields().iter().zip(batch.columns()).zip(&formatters)
            {
                let value = match column.is_null(row) {
                    true => Value::Null,
                    false => Value::String(formatter.value(row).to_string()),
                };
                object.insert(field.name().clone(), value);
            }
            decoded.push(Value::Object(object));
        }
    }
    Ok(decoded)
}

impl FakeState {
    pub(crate) fn create_write_stream(
        &mut self,
        request: CreateWriteStreamRequest,
    ) -> Result<WriteStream, Status> {
        let table = table_of(&request.parent);
        if !self.tables.contains_key(&table) {
            return Err(Status::not_found(format!(
                "Requested entity was not found. Entity: {}",
                request.parent
            )));
        }
        self.stream_ids += 1;
        let name = format!("{}/streams/s{}", request.parent, self.stream_ids);
        self.streams.insert(
            name.clone(),
            FakeStream {
                table,
                ..FakeStream::default()
            },
        );
        Ok(WriteStream {
            name,
            r#type: PENDING_STREAM,
        })
    }

    /// One append: the rows at the stream's end, or why they were refused, as BigQuery answers in band.
    pub(crate) fn append_rows(&mut self, request: AppendRowsRequest) -> AppendRowsResponse {
        let refused = self.refused_appends.pop_front();
        let name = request.write_stream.clone();
        let Some(stream) = self.streams.get_mut(&name) else {
            return rejected(
                name.clone(),
                tonic::Code::NotFound,
                format!("Stream not found: {name}"),
            );
        };
        if stream.finalized {
            return rejected(
                name,
                tonic::Code::FailedPrecondition,
                "Stream is finalized.".to_owned(),
            );
        }
        let end = i64::try_from(stream.rows.len()).unwrap_or(i64::MAX);
        let offset = request.offset.map_or(end, |offset| offset.value);
        if offset != end {
            return AppendRowsResponse {
                error: Some(RpcStatus {
                    code: OUT_OF_RANGE,
                    message: format!(
                        "The offset is beyond stream, expected offset {end}, received {offset}"
                    ),
                }),
                ..rejected(name, tonic::Code::OutOfRange, String::new())
            };
        }
        let data = request.arrow_rows.unwrap_or_default();
        if let Some(schema) = data.writer_schema {
            stream.schema = schema.serialized_schema;
        }
        let columns = self
            .tables
            .get(&stream.table)
            .map(|table| table.columns.clone())
            .unwrap_or_default();
        let extra = extra_fields(&stream.schema, &columns);
        if !extra.is_empty() {
            return rejected(
                name,
                tonic::Code::InvalidArgument,
                format!(
                    "Input schema has more fields than BigQuery schema. Extra: {}",
                    extra.join(", ")
                ),
            );
        }
        let rows = match decoded_rows(
            &stream.schema,
            &data.rows.unwrap_or_default().serialized_record_batch,
        ) {
            Ok(rows) => rows,
            Err(error) => return rejected(name, tonic::Code::InvalidArgument, error),
        };
        if let Some(message) = refused {
            return AppendRowsResponse {
                row_errors: vec![RefusedRow {
                    index: 0,
                    code: 1,
                    message: message.clone(),
                }],
                ..rejected(name, tonic::Code::InvalidArgument, message)
            };
        }
        stream.rows.extend(rows);
        AppendRowsResponse {
            append_result: Some(AppendResult {
                offset: Some(Int64Value { value: offset }),
            }),
            error: None,
            row_errors: Vec::new(),
            write_stream: name,
        }
    }

    pub(crate) fn finalize_write_stream(
        &mut self,
        request: FinalizeWriteStreamRequest,
    ) -> Result<FinalizeWriteStreamResponse, Status> {
        let stream = self
            .streams
            .get_mut(&request.name)
            .ok_or_else(|| Status::not_found(format!("Stream not found: {}", request.name)))?;
        stream.finalized = true;
        Ok(FinalizeWriteStreamResponse {
            row_count: i64::try_from(stream.rows.len()).unwrap_or(i64::MAX),
        })
    }

    /// Commit every named stream into its table at once, or none, with an error for each that cannot commit.
    pub(crate) fn batch_commit_write_streams(
        &mut self,
        request: BatchCommitWriteStreamsRequest,
    ) -> Result<BatchCommitWriteStreamsResponse, Status> {
        let refused = self.refused_commits.pop_front();
        let mut stream_errors: Vec<StreamRefusal> = Vec::new();
        for name in &request.write_streams {
            let problem = match (self.streams.get(name), &refused) {
                (None, _) => Some((STREAM_NOT_FOUND, "Stream not found.".to_owned())),
                (Some(stream), _) if !stream.finalized => {
                    Some((INVALID_STREAM_STATE, "Stream is not finalized.".to_owned()))
                }
                (Some(_), Some(message)) => Some((INVALID_STREAM_STATE, message.clone())),
                (Some(_), None) => None,
            };
            stream_errors.extend(problem.map(|(code, error_message)| StreamRefusal {
                code,
                entity: name.clone(),
                error_message,
            }));
        }
        if !stream_errors.is_empty() {
            return Ok(BatchCommitWriteStreamsResponse {
                commit_time: None,
                stream_errors,
            });
        }
        for name in &request.write_streams {
            if let Some(stream) = self.streams.remove(name) {
                self.tables
                    .entry(stream.table)
                    .or_default()
                    .rows
                    .extend(stream.rows);
            }
        }
        self.commits
            .push((table_of(&request.parent), request.write_streams.len()));
        Ok(BatchCommitWriteStreamsResponse {
            commit_time: Some(Timestamp {
                seconds: 1_800_000_000,
                nanos: 0,
            }),
            stream_errors: Vec::new(),
        })
    }
}

/// Serve the Storage Write API on `listener` in the background.
pub(crate) fn serve_write_api(listener: tokio::net::TcpListener, state: SharedState) {
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(WriteService(state))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener))
            .await;
    });
}
