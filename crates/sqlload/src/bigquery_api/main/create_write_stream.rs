//! Open a pending write stream.

use crate::bigquery_api::_helpers::grpc::unary;
use crate::bigquery_api::constants::{CREATE_WRITE_STREAM, PENDING_STREAM};
use crate::bigquery_api::models::{BigQueryClient, CreateWriteStreamRequest, WriteStream};
use crate::http::errors::ApiError;

/// A new pending stream on the table `parent` (see [`table_path`](crate::bigquery_api::main::table_path::table_path)): its rows stay invisible until it is finalized and committed, and vanish if it never is.
pub async fn create_write_stream(
    client: &BigQueryClient,
    parent: &str,
) -> Result<WriteStream, ApiError> {
    let request = CreateWriteStreamRequest {
        parent: parent.to_owned(),
        write_stream: Some(WriteStream {
            name: String::new(),
            r#type: PENDING_STREAM,
        }),
    };
    unary(
        client,
        CREATE_WRITE_STREAM,
        &format!("parent={parent}"),
        request,
    )
    .await
}
