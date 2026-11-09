//! Read a table or query through the Storage Read API as Arrow batches.

use arrow_array::RecordBatch;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::bigquery_api::main::create_read_session::create_read_session;
use crate::bigquery_api::main::query_destination::query_destination;
use crate::bigquery_api::main::read_stream::read_stream;
use crate::bigquery_api::models::ReadSessionRequest;
use crate::bigquery_source::_helpers::convert::{convert_batch, output_schema};
use crate::bigquery_source::_helpers::query::select_query;
use crate::bigquery_source::constants::{BATCHES_AHEAD, READ_STREAMS};
use crate::bigquery_source::errors::BigQuerySourceError;
use crate::bigquery_source::models::{BigQueryReadRequest, BigQueryReader};
use crate::http::errors::ApiError;

/// Read `request`'s rows and pass them to `on_batch`, each batch converted to the read columns' types; returns the row count. A table is read directly, as of the reader's instant, with the filter as the session's row restriction; a view or a query table runs a query job, whose result table is then read the same way. The session's streams are read in parallel, at most a few batches ahead of `on_batch`, so batches arrive in no particular order.
pub async fn read_rows(
    reader: &BigQueryReader,
    request: &BigQueryReadRequest<'_>,
    mut on_batch: impl AsyncFnMut(RecordBatch) -> Result<(), String>,
) -> Result<u64, BigQuerySourceError> {
    let (schema, json) = output_schema(request.columns).map_err(BigQuerySourceError::Invalid)?;
    let direct = request.sql.is_none() && request.table.estimated_bytes >= 0;
    let fields: Vec<String> = request
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let (table, session_request) = match direct {
        true => (
            format!(
                "projects/{}/datasets/{}/tables/{}",
                reader.project, request.table.schema, request.table.name
            ),
            ReadSessionRequest {
                fields: &fields,
                restriction: request.filter,
                snapshot_ms: Some(reader.snapshot_ms),
                max_streams: READ_STREAMS,
            },
        ),
        false => (
            query_destination(&reader.client, &select_query(&reader.project, request)).await?,
            ReadSessionRequest {
                fields: &fields,
                restriction: None,
                snapshot_ms: None,
                max_streams: READ_STREAMS,
            },
        ),
    };
    let session = create_read_session(&reader.client, &table, &session_request).await?;
    let serialized = session
        .arrow_schema
        .map(|schema| schema.serialized_schema)
        .unwrap_or_default();
    let (sender, mut receiver) = mpsc::channel::<RecordBatch>(BATCHES_AHEAD);
    let mut streams: JoinSet<Result<u64, ApiError>> = JoinSet::new();
    for stream in session.streams {
        let (client, serialized, sender) =
            (reader.client.clone(), serialized.clone(), sender.clone());
        streams.spawn(async move {
            read_stream(&client, (&stream.name, &serialized), move |batch| {
                let sender = sender.clone();
                async move {
                    sender
                        .send(batch)
                        .await
                        .map_err(|_| "the reader stopped taking batches".to_owned())
                }
            })
            .await
        });
    }
    drop(sender);
    let mut rows: u64 = 0;
    while let Some(batch) = receiver.recv().await {
        let batch =
            convert_batch(&batch, (&schema, &json)).map_err(BigQuerySourceError::Invalid)?;
        rows += batch.num_rows() as u64;
        on_batch(batch)
            .await
            .map_err(BigQuerySourceError::Consumer)?;
    }
    while let Some(finished) = streams.join_next().await {
        finished.map_err(|error| {
            BigQuerySourceError::Invalid(format!("a read stream stopped: {error}"))
        })??;
    }
    Ok(rows)
}
