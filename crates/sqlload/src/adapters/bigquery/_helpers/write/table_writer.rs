//! BigQuery's write session: Arrow rows appended to pending Storage Write API streams on the staging or build table, finalized and committed together once the batch is sent.

use arrow_array::RecordBatch;

use crate::adapters::bigquery::_helpers::write::arrow_rows::{append_requests, write_api_batch};
use crate::adapters::bigquery::constants::{APPEND_REQUEST_BYTES, STREAMS_PER_BATCH};
use crate::adapters::bigquery::models::{BigQueryWriter, OpenStream};
use crate::adapters::contract::models::WriteTarget;
use crate::adapters::contract::types::TableWriter;
use crate::bigquery_api::main::append_answer::append_answer;
use crate::bigquery_api::main::append_rows::append_rows;
use crate::bigquery_api::main::batch_commit_write_streams::batch_commit_write_streams;
use crate::bigquery_api::main::create_write_stream::create_write_stream;
use crate::bigquery_api::main::finalize_write_stream::finalize_write_stream;
use crate::bigquery_api::main::table_path::table_path;
use crate::bigquery_api::models::{AppendRowsRequest, AppendRowsResponse, Int64Value};
use crate::http::errors::ApiError;
use crate::loader::errors::LoaderError;

impl TableWriter for BigQueryWriter {
    /// Never: a stream on a table that `replace` or a rebuild has just recreated is refused for a while, so rows always go to a staging or build table named for the load.
    fn writes_directly(&self) -> bool {
        false
    }

    async fn write_batch(
        &mut self,
        target: &WriteTarget<'_>,
        batch: RecordBatch,
    ) -> Result<(), LoaderError> {
        let table = target.staging.unwrap_or(target.table);
        let project = table
            .database
            .as_deref()
            .unwrap_or(&self.destination.project);
        self.parent = table_path(project, &table.schema, &table.table);
        let columns = match target.builds {
            true => target.write_columns.to_vec(),
            false => target.columns.to_vec(),
        };
        let builds = target.builds;
        let requests = tokio::task::spawn_blocking(move || {
            append_requests(
                &write_api_batch(&batch, (&columns, builds))?,
                APPEND_REQUEST_BYTES,
            )
        })
        .await
        .map_err(|error| {
            LoaderError::Invalid(format!("the Arrow encoding thread failed: {error}"))
        })?
        .map_err(LoaderError::Invalid)?;
        for (request, rows) in requests {
            self.send(request, rows).await?;
        }
        Ok(())
    }

    fn staged_bytes(&self) -> Option<usize> {
        None
    }

    async fn finish_files(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        Ok(())
    }

    /// The batch's streams are committed by [`settle_rows`](TableWriter::settle_rows) just before; nothing is left to load.
    async fn load_batch(&mut self, _target: &WriteTarget<'_>) -> Result<(), LoaderError> {
        self.commit_streams().await
    }

    fn next_batch(&mut self) {
        self.streams.clear();
        self.requests_sent = 0;
    }

    /// Finalize the batch's streams and commit them at once, so every row sent is in the staging table before its schema changes or the batch loads.
    async fn settle_rows(&mut self) -> Result<(), LoaderError> {
        self.commit_streams().await
    }

    fn reopen_rows(&mut self) {
        self.next_batch();
    }

    /// Stop the batch's streams: rows of a stream never committed are never visible, and the staging table is dropped next.
    async fn rollback_batch(&mut self) {
        for stream in self.streams.drain(..) {
            drop(stream.requests);
            if let Some(answers) = stream.answers {
                answers.abort();
            }
        }
        self.requests_sent = 0;
    }

    async fn remove_batch_files(&mut self, _target: &WriteTarget<'_>) {}
}

impl BigQueryWriter {
    /// Send one request of `rows` rows to the batch's next stream in turn, opening it with the writer schema on its first request; a stream whose answers already failed, or that the server has ended, fails with the error it gave instead of taking more rows.
    async fn send(
        &mut self,
        mut request: AppendRowsRequest,
        rows: usize,
    ) -> Result<(), LoaderError> {
        let index = self.requests_sent % STREAMS_PER_BATCH;
        let rows = i64::try_from(rows).map_err(|error| LoaderError::Invalid(error.to_string()))?;
        if index >= self.streams.len() {
            let stream = create_write_stream(&self.destination.api, &self.parent).await?;
            request.write_stream.clone_from(&stream.name);
            request.offset = Some(Int64Value { value: 0 });
            let opened = append_rows(&self.destination.api, request).await?;
            self.streams.push(OpenStream {
                name: stream.name,
                offset: rows,
                requests: Some(opened.requests),
                answers: Some(tokio::spawn(read_answers(opened.responses))),
            });
            self.requests_sent += 1;
            return Ok(());
        }
        self.requests_sent += 1;
        let stream = &mut self.streams[index];
        if stream
            .answers
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            let answers = stream.answers.take();
            return Err(match answers {
                Some(answers) => ended(answers.await),
                None => ApiError::Protocol("the append stream was already closed".to_owned()),
            }
            .into());
        }
        request.write_stream.clone_from(&stream.name);
        request.offset = Some(Int64Value {
            value: stream.offset,
        });
        if let Some(arrow) = request.arrow_rows.as_mut() {
            arrow.writer_schema = None;
        }
        stream.offset += rows;
        let Some(requests) = &stream.requests else {
            return Err(
                ApiError::Protocol("the append stream was already closed".to_owned()).into(),
            );
        };
        let Err(tokio::sync::mpsc::error::SendError(_)) = requests.send(request).await else {
            return Ok(());
        };
        let answers = stream.answers.take();
        Err(match answers {
            Some(answers) => ended(answers.await),
            None => ApiError::Protocol("the append stream was already closed".to_owned()),
        }
        .into())
    }

    /// Close every open stream, wait for its answers, finalize it, check it holds every row sent, then commit them all in one step; nothing when no stream is open.
    async fn commit_streams(&mut self) -> Result<(), LoaderError> {
        if self.streams.is_empty() {
            return Ok(());
        }
        let mut names: Vec<String> = Vec::with_capacity(self.streams.len());
        for mut stream in self.streams.drain(..) {
            drop(stream.requests.take());
            if let Some(answers) = stream.answers.take() {
                answers.await.map_err(|error| {
                    ApiError::Protocol(format!("the append reader failed: {error}"))
                })??;
            }
            let held = finalize_write_stream(&self.destination.api, &stream.name).await?;
            if held != stream.offset {
                return Err(LoaderError::Invalid(format!(
                    "BigQuery's stream {} holds {held} rows but {} were sent",
                    stream.name, stream.offset
                )));
            }
            names.push(stream.name);
        }
        self.requests_sent = 0;
        batch_commit_write_streams(&self.destination.api, &self.parent, &names).await?;
        Ok(())
    }
}

/// The error a stream that stopped taking requests ended with.
fn ended(answers: Result<Result<usize, ApiError>, tokio::task::JoinError>) -> ApiError {
    match answers {
        Ok(Err(error)) => error,
        Ok(Ok(answered)) => ApiError::Protocol(format!(
            "BigQuery closed the append stream after {answered} answers without an error"
        )),
        Err(error) => ApiError::Protocol(format!("the append reader failed: {error}")),
    }
}

/// Read a stream's answers until it ends: the requests answered, or the first error or refused row.
async fn read_answers(
    mut responses: tonic::Streaming<AppendRowsResponse>,
) -> Result<usize, ApiError> {
    let mut answered = 0;
    while let Some(answer) = append_answer(&mut responses).await? {
        if let Some(refused) = answer.row_errors.first() {
            return Err(ApiError::Status {
                status: 400,
                code: "rowErrors".to_owned(),
                message: format!(
                    "BigQuery refused {} row(s) of an append, the first at index {}: {}",
                    answer.row_errors.len(),
                    refused.index,
                    refused.message
                ),
            });
        }
        if let Some(error) = answer.error {
            return Err(ApiError::Status {
                status: 400,
                code: format!("appendError{}", error.code),
                message: error.message,
            });
        }
        answered += 1;
    }
    Ok(answered)
}
