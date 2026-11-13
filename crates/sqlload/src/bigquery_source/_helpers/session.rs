//! The read session behind one read, and the tasks reading its streams.

use arrow_array::RecordBatch;
use tokio::sync::mpsc::{Receiver, channel};
use tokio::task::JoinSet;

use crate::bigquery_api::main::create_read_session::create_read_session;
use crate::bigquery_api::main::query_destination::query_destination;
use crate::bigquery_api::main::read_stream::read_stream;
use crate::bigquery_api::models::{ReadSession, ReadSessionRequest};
use crate::bigquery_source::_helpers::convert::source_name;
use crate::bigquery_source::_helpers::query::select_query;
use crate::bigquery_source::constants::{BATCHES_AHEAD, READ_STREAMS};
use crate::bigquery_source::models::{BigQueryReadRequest, BigQueryReader};
use crate::http::errors::ApiError;

/// A session reading `request`'s columns: on the table itself as of the reader's instant, with the filter as its row restriction; or, for a view or a query table, on the result table of a query job that applies the filter.
pub(crate) async fn read_session(
    reader: &BigQueryReader,
    request: &BigQueryReadRequest<'_>,
) -> Result<ReadSession, ApiError> {
    let fields: Vec<String> = request
        .columns
        .iter()
        .map(|column| source_name(request.table, &column.name).to_owned())
        .collect();
    let direct = request.sql.is_none() && request.table.estimated_bytes >= 0;
    let (table, restriction, snapshot_ms) = match direct {
        true => (
            format!(
                "projects/{}/datasets/{}/tables/{}",
                reader.project, request.table.schema, request.table.name
            ),
            request.filter,
            Some(reader.snapshot_ms),
        ),
        false => (
            query_destination(&reader.client, &select_query(&reader.project, request)).await?,
            None,
            None,
        ),
    };
    let session_request = ReadSessionRequest {
        fields: &fields,
        restriction,
        snapshot_ms,
        max_streams: READ_STREAMS,
    };
    create_read_session(&reader.client, &table, &session_request).await
}

/// One task per stream of `session`, each sending its batches on the returned channel, which holds at most [`BATCHES_AHEAD`].
pub(crate) fn read_streams(
    reader: &BigQueryReader,
    session: ReadSession,
) -> (JoinSet<Result<u64, ApiError>>, Receiver<RecordBatch>) {
    let serialized = session
        .arrow_schema
        .map(|schema| schema.serialized_schema)
        .unwrap_or_default();
    let (sender, receiver) = channel::<RecordBatch>(BATCHES_AHEAD);
    let mut streams: JoinSet<Result<u64, ApiError>> = JoinSet::new();
    for stream in session.streams {
        let (client, serialized, sender) =
            (reader.client.clone(), serialized.clone(), sender.clone());
        streams.spawn(async move {
            let send = move |batch: RecordBatch| {
                let sender = sender.clone();
                async move {
                    sender
                        .send(batch)
                        .await
                        .map_err(|_| "the reader stopped taking batches".to_owned())
                }
            };
            read_stream(&client, (&stream.name, &serialized), send).await
        });
    }
    (streams, receiver)
}
