//! The fake's Storage Read API: a session over a table's rows (filtered by its row restriction, split into two streams), and each stream's rows as Arrow IPC, one row per message.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions, write_message,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde_json::Value;
use sqlload::bigquery_api::constants::REQUEST_PARAMS;
use sqlload::bigquery_api::models::{
    ArrowRecordBatch, ArrowSchema, CreateReadSessionRequest, ReadRowsRequest, ReadRowsResponse,
    ReadSession, ReadStream,
};
use tonic::codegen::{BoxFuture, BoxStream, Context, Poll, Service, StdError, http};
use tonic::server::{Grpc, NamedService};
use tonic::{Request, Response, Status};
use tonic_prost::ProstCodec;

use crate::fake_bigquery::constants::{READ_SERVICE, READ_STREAMS, TOKEN_PREFIX};
use crate::fake_bigquery::models::{FakeReadSession, FakeState, FakeStreamRows, SharedState};

/// The Storage Read API service, answering each call from the shared state.
#[derive(Debug, Clone)]
pub(crate) struct ReadService(pub(crate) SharedState);

impl NamedService for ReadService {
    const NAME: &'static str = READ_SERVICE;
}

/// A `CreateReadSession` call, answered while the state is locked.
struct Sessions(SharedState);

impl Service<Request<CreateReadSessionRequest>> for Sessions {
    type Response = Response<ReadSession>;
    type Error = Status;
    type Future = std::future::Ready<Result<Response<ReadSession>, Status>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<CreateReadSessionRequest>) -> Self::Future {
        let answered = self.0.lock().create_read_session(request.into_inner());
        std::future::ready(answered.map(Response::new))
    }
}

/// A `ReadRows` call: the stream's rows from the offset on, then its end, or a break after the first row while `broken_reads` lasts.
struct Reads(SharedState);

impl Service<Request<ReadRowsRequest>> for Reads {
    type Response = Response<BoxStream<ReadRowsResponse>>;
    type Error = Status;
    type Future = std::future::Ready<Result<Self::Response, Status>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<ReadRowsRequest>) -> Self::Future {
        let messages = self.0.lock().read_rows(&request.into_inner());
        let answers: BoxStream<ReadRowsResponse> =
            Box::pin(tonic::codegen::tokio_stream::iter(messages));
        std::future::ready(Ok(Response::new(answers)))
    }
}

impl<B> Service<http::Request<B>> for ReadService
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
                "CreateReadSession" => {
                    Grpc::new(ProstCodec::<ReadSession, CreateReadSessionRequest>::default())
                        .unary(Sessions(state), request)
                        .await
                }
                "ReadRows" => {
                    Grpc::new(ProstCodec::<ReadRowsResponse, ReadRowsRequest>::default())
                        .server_streaming(Reads(state), request)
                        .await
                }
                other => Status::unimplemented(other.to_owned()).into_http(),
            };
            Ok(answered)
        })
    }
}

/// The Arrow type the Read API sends a column of DDL type `sql_type` in: `NUMERIC` as `Decimal128(38, 9)` whatever its parameters, and anything it doesn't model as text.
fn read_type(sql_type: &str) -> DataType {
    let upper = sql_type.to_ascii_uppercase();
    let base = upper
        .trim_end_matches(" NOT NULL")
        .split('(')
        .next()
        .unwrap_or_default()
        .trim()
        .to_owned();
    match base.as_str() {
        "INT64" | "INTEGER" => DataType::Int64,
        "FLOAT64" | "FLOAT" => DataType::Float64,
        "BOOL" | "BOOLEAN" => DataType::Boolean,
        "NUMERIC" => DataType::Decimal128(38, 9),
        "DATE" => DataType::Date32,
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        _ => DataType::Utf8,
    }
}

/// Whether a column of DDL type `sql_type` compares as a number, so a quoted literal has no matching signature.
fn numeric(sql_type: &str) -> bool {
    matches!(
        read_type(sql_type),
        DataType::Int64 | DataType::Float64 | DataType::Decimal128(..)
    )
}

/// Whether `row` passes `restriction`: conditions `` `column` <op> literal `` joined by ` AND `, a literal bare or in single quotes; numbers compare as numbers, anything else as text. A condition the fake cannot read, or a quoted literal against a numeric column (`INT64 >= STRING` has no signature in BigQuery), is refused.
fn passes(row: &Value, restriction: &str, columns: &[(String, String)]) -> Result<bool, Status> {
    for condition in restriction
        .split(" AND ")
        .filter(|text| !text.trim().is_empty())
    {
        let refused =
            || Status::invalid_argument(format!("cannot read the row restriction {condition:?}"));
        let (name, rest) = condition
            .trim()
            .strip_prefix('`')
            .and_then(|rest| rest.split_once('`'))
            .ok_or_else(refused)?;
        let rest = rest.trim();
        let (operator, literal) = [">=", "<=", ">", "<", "="]
            .into_iter()
            .find_map(|operator| Some((operator, rest.strip_prefix(operator)?.trim())))
            .ok_or_else(refused)?;
        let sql_type = columns
            .iter()
            .find(|(column, _)| column == name)
            .map(|(_, sql_type)| sql_type.as_str())
            .ok_or_else(refused)?;
        let quoted = literal.starts_with('\'');
        if quoted && numeric(sql_type) {
            return Err(Status::invalid_argument(format!(
                "No matching signature for operator {operator} for argument types: {sql_type}, STRING"
            )));
        }
        let literal = literal.trim_matches('\'').replace("''", "'");
        let Some(value) = row[name].as_str() else {
            return Ok(false);
        };
        let ordering = match (value.parse::<f64>(), literal.parse::<f64>()) {
            (Ok(left), Ok(right)) => left.partial_cmp(&right).ok_or_else(refused)?,
            _ => value.cmp(literal.as_str()),
        };
        let kept = match operator {
            ">=" => ordering.is_ge(),
            "<=" => ordering.is_le(),
            ">" => ordering.is_gt(),
            "<" => ordering.is_lt(),
            _ => ordering.is_eq(),
        };
        if !kept {
            return Ok(false);
        }
    }
    Ok(true)
}

/// One encapsulated Arrow IPC message: `schema`'s, or `batch`'s rows.
fn encoded(schema: &SchemaRef, batch: Option<&RecordBatch>) -> Result<Vec<u8>, Status> {
    let failed = |error: arrow_schema::ArrowError| Status::internal(error.to_string());
    let generator = IpcDataGenerator::default();
    let options = IpcWriteOptions::default();
    let mut tracker = DictionaryTracker::new(false);
    let message = match batch {
        None => generator.schema_to_bytes_with_dictionary_tracker(schema, &mut tracker, &options),
        Some(batch) => {
            generator
                .encode(
                    batch,
                    &mut tracker,
                    &options,
                    &mut IpcWriteContext::default(),
                )
                .map_err(failed)?
                .1
        }
    };
    let mut bytes: Vec<u8> = Vec::new();
    write_message(&mut bytes, message, &options).map_err(failed)?;
    Ok(bytes)
}

/// `rows` as one batch of `schema`, each value cast from its text.
fn rows_batch(schema: &SchemaRef, rows: &[Value]) -> Result<RecordBatch, Status> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let texts: StringArray = rows
            .iter()
            .map(|row| row[field.name()].as_str().map(str::to_owned))
            .collect();
        columns.push(
            arrow_cast::cast(&texts, field.data_type())
                .map_err(|error| Status::internal(error.to_string()))?,
        );
    }
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|error| Status::internal(error.to_string()))
}

impl FakeState {
    pub(crate) fn create_read_session(
        &mut self,
        request: CreateReadSessionRequest,
    ) -> Result<ReadSession, Status> {
        let asked = request.read_session.unwrap_or_default();
        let options = asked.read_options.clone().unwrap_or_default();
        let key = match asked.table.split('/').collect::<Vec<&str>>().as_slice() {
            ["projects", _, "datasets", dataset, "tables", table] => {
                format!("{dataset}.{table}").to_ascii_lowercase()
            }
            _ => {
                return Err(Status::invalid_argument(format!(
                    "bad table {}",
                    asked.table
                )));
            }
        };
        self.read_sessions.push(FakeReadSession {
            table: key.clone(),
            fields: options.selected_fields.clone(),
            restriction: options.row_restriction.clone(),
            snapshot: asked.table_modifiers.is_some(),
        });
        let found = self
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("Not found: Table {key}")))?;
        let mut columns: Vec<(String, String)> = Vec::new();
        for name in &options.selected_fields {
            let Some(column) = found.columns.iter().find(|(column, _)| column == name) else {
                return Err(Status::invalid_argument(format!("no column {name}")));
            };
            columns.push(column.clone());
        }
        if columns.is_empty() {
            columns.clone_from(&found.columns);
        }
        let mut rows: Vec<Value> = Vec::new();
        for row in &found.rows {
            if passes(row, &options.row_restriction, &found.columns)? {
                rows.push(row.clone());
            }
        }
        let schema: SchemaRef = Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, sql_type)| Field::new(name, read_type(sql_type), true))
                .collect::<Vec<Field>>(),
        ));
        self.read_session_ids += 1;
        let session = format!(
            "projects/fake/locations/eu/sessions/session_{}",
            self.read_session_ids
        );
        let mut streams: Vec<ReadStream> = Vec::new();
        let per_stream = rows.len().div_ceil(READ_STREAMS).max(1);
        for (index, part) in rows.chunks(per_stream).enumerate() {
            let name = format!("{session}/streams/{index}");
            self.read_streams.insert(
                name.clone(),
                FakeStreamRows {
                    schema: schema.clone(),
                    rows: part.to_vec(),
                },
            );
            streams.push(ReadStream { name });
        }
        Ok(ReadSession {
            name: session,
            data_format: asked.data_format,
            arrow_schema: Some(ArrowSchema {
                serialized_schema: encoded(&schema, None)?,
            }),
            table: asked.table,
            streams,
            ..ReadSession::default()
        })
    }

    pub(crate) fn read_rows(
        &mut self,
        request: &ReadRowsRequest,
    ) -> Vec<Result<ReadRowsResponse, Status>> {
        self.read_calls
            .push((request.read_stream.clone(), request.offset));
        let Some(stream) = self.read_streams.get(&request.read_stream).cloned() else {
            return vec![Err(Status::not_found(request.read_stream.clone()))];
        };
        let offset = usize::try_from(request.offset).unwrap_or(usize::MAX);
        let mut messages: Vec<Result<ReadRowsResponse, Status>> = Vec::new();
        for row in stream.rows.iter().skip(offset) {
            let message = rows_batch(&stream.schema, std::slice::from_ref(row))
                .and_then(|batch| encoded(&stream.schema, Some(&batch)))
                .map(|bytes| ReadRowsResponse {
                    arrow_record_batch: Some(ArrowRecordBatch {
                        serialized_record_batch: bytes,
                    }),
                    row_count: 1,
                });
            messages.push(message);
            if self.broken_reads > 0 {
                self.broken_reads -= 1;
                messages.push(Err(Status::unavailable("the stream broke")));
                return messages;
            }
        }
        messages
    }
}

/// Serve the Storage Write and Read APIs on `listener` in the background.
pub(crate) fn serve_storage_api(listener: tokio::net::TcpListener, state: SharedState) {
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(crate::fake_bigquery::_helpers::write_api::WriteService(
                state.clone(),
            ))
            .add_service(ReadService(state))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener))
            .await;
    });
}
