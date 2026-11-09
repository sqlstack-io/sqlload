//! Open a Storage Read API session on a table.

use crate::bigquery_api::_helpers::grpc::unary;
use crate::bigquery_api::constants::{ARROW_FORMAT, CREATE_READ_SESSION, LZ4_FRAME};
use crate::bigquery_api::models::{
    ArrowSerializationOptions, BigQueryClient, CreateReadSessionRequest, ReadSession,
    ReadSessionRequest, TableModifiers, TableReadOptions, Timestamp,
};
use crate::http::errors::ApiError;

/// A session reading `request`'s columns of `table` (`projects/p/datasets/d/tables/t`) as LZ4-compressed Arrow, billed to the client's project; BigQuery may split it into fewer streams than asked, and into none when no row matches.
pub async fn create_read_session(
    client: &BigQueryClient,
    table: &str,
    request: &ReadSessionRequest<'_>,
) -> Result<ReadSession, ApiError> {
    let session = ReadSession {
        data_format: ARROW_FORMAT,
        table: table.to_owned(),
        table_modifiers: request.snapshot_ms.map(|ms| TableModifiers {
            snapshot_time: Some(Timestamp {
                seconds: ms.div_euclid(1000),
                nanos: i32::try_from(ms.rem_euclid(1000) * 1_000_000).unwrap_or_default(),
            }),
        }),
        read_options: Some(TableReadOptions {
            selected_fields: request.fields.to_vec(),
            row_restriction: request.restriction.unwrap_or_default().to_owned(),
            arrow_serialization_options: Some(ArrowSerializationOptions {
                buffer_compression: LZ4_FRAME,
            }),
        }),
        ..ReadSession::default()
    };
    let message = CreateReadSessionRequest {
        parent: format!("projects/{}", client.project),
        read_session: Some(session),
        max_stream_count: request.max_streams,
    };
    unary(
        client,
        CREATE_READ_SESSION,
        &format!("read_session.table={table}"),
        message,
    )
    .await
}
