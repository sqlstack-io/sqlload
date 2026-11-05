//! Rows sent to Snowflake without files: Snowpipe Streaming requests, or batched `INSERT`s.

use std::io::Write;

use arrow_array::RecordBatch;
use flate2::Compression as GzipLevel;
use flate2::write::GzEncoder;

use crate::adapters::contract::models::{TableName, WriteTarget};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::constants::{
    STREAM_COMMIT_WAIT_SECS, STREAM_PIPE_SUFFIX, STREAM_POLL_MILLIS, STREAM_REQUEST_BYTES,
};
use crate::adapters::snowflake::main::session::_run_statements::run_statements;
use crate::adapters::snowflake::main::session::streaming_client::streaming_client;
use crate::adapters::snowflake::models::{RowStream, SnowflakeSql, SnowflakeWriter, StageRoute};
use crate::loader::errors::LoaderError;
use crate::row_batches::main::text_rows::text_rows;
use crate::schema_plan::models::DesiredColumn;
use crate::streaming_api::main::append_rows::append_rows as append_stream_rows;
use crate::streaming_api::main::channel_statuses::channel_statuses;
use crate::streaming_api::main::open_channel::open_channel;
use crate::streaming_api::models::AppendRequest;

/// Add `batch` to the staging table without files: a request to the channel, or `INSERT`s.
pub(crate) async fn send_rows(
    writer: &mut SnowflakeWriter,
    target: &WriteTarget<'_>,
    batch: &RecordBatch,
) -> Result<(), LoaderError> {
    let Some(staging) = target.staging.cloned() else {
        return Err(LoaderError::Invalid(
            "rows go in through a staging table, and this load has none".to_owned(),
        ));
    };
    let schema = batch.schema();
    let mut in_batch: Vec<DesiredColumn> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column = target
            .columns
            .iter()
            .find(|column| column.name == *field.name());
        in_batch.extend(column.cloned());
    }
    let rows = text_rows(batch).map_err(LoaderError::Invalid)?;
    if matches!(writer.route, Some(StageRoute::Streaming { .. })) {
        open_stream(writer, target, &staging).await?;
    }
    match writer.stream.as_mut() {
        Some(stream) => send(stream, &in_batch, &rows).await,
        None => {
            for statement in SnowflakeSql.render_insert_rows(&staging, &in_batch, &rows) {
                run_statements(&writer.destination, &[statement]).await?;
            }
            Ok(())
        }
    }
}

/// Open the channel on first use. A chain-chosen stream that will not open drops to `INSERT`, as no rows have been sent; a forced one fails.
async fn open_stream(
    writer: &mut SnowflakeWriter,
    target: &WriteTarget<'_>,
    staging: &TableName,
) -> Result<(), LoaderError> {
    if writer.stream.is_some() {
        return Ok(());
    }
    let client = streaming_client(&writer.destination, staging);
    let pipe = format!(
        "{}{STREAM_PIPE_SUFFIX}",
        SnowflakeSql.stored_name(&staging.table)
    );
    let channel = format!("sqlload_{}", target.load_id.replace('-', "_"));
    match (open_channel(&client, &pipe, &channel).await, &writer.route) {
        (Ok(opened), _) => {
            writer.stream = Some(RowStream {
                client,
                pipe,
                channel,
                continuation: opened.continuation_token,
                offset: 0,
            });
            Ok(())
        }
        (
            Err(error),
            Some(StageRoute::Streaming {
                may_fall_back: true,
            }),
        ) => {
            tracing::warn!(%error, table = %target.table.table, "Snowpipe Streaming will not open; this table loads by batched INSERT");
            writer.route = Some(StageRoute::Insert);
            Ok(())
        }
        (Err(error), _) => Err(LoaderError::Invalid(format!(
            "Snowpipe Streaming will not open for {}: {error}",
            staging.table
        ))),
    }
}

/// Send `rows` to the channel in requests of at most about 4 MiB of gzip NDJSON, keyed by the columns' stored names.
async fn send(
    stream: &mut RowStream,
    columns: &[DesiredColumn],
    rows: &[Vec<Option<String>>],
) -> Result<(), LoaderError> {
    let keys: Vec<String> = columns
        .iter()
        .map(|column| SnowflakeSql.stored_name(&column.name))
        .collect();
    let mut body: Vec<u8> = Vec::new();
    for row in rows {
        let line: serde_json::Map<String, serde_json::Value> = keys
            .iter()
            .cloned()
            .zip(row.iter().map(|value| serde_json::json!(value)))
            .collect();
        serde_json::to_writer(&mut body, &line)
            .map_err(|error| LoaderError::Invalid(error.to_string()))?;
        body.push(b'\n');
        if body.len() >= STREAM_REQUEST_BYTES {
            send_body(stream, std::mem::take(&mut body)).await?;
        }
    }
    if !body.is_empty() {
        send_body(stream, body).await?;
    }
    Ok(())
}

async fn send_body(stream: &mut RowStream, body: Vec<u8>) -> Result<(), LoaderError> {
    let mut gzip = GzEncoder::new(Vec::new(), GzipLevel::default());
    gzip.write_all(&body)
        .map_err(|error| LoaderError::Invalid(error.to_string()))?;
    let compressed = gzip
        .finish()
        .map_err(|error| LoaderError::Invalid(error.to_string()))?;
    stream.offset += 1;
    stream.continuation = append_stream_rows(
        &stream.client,
        AppendRequest {
            pipe: &stream.pipe,
            channel: &stream.channel,
            continuation: &stream.continuation,
            offset: stream.offset,
            body: compressed,
        },
    )
    .await?;
    Ok(())
}

/// Wait until Snowflake has committed the channel's last request; a row it refused fails the load.
pub(crate) async fn settle_stream(stream: &RowStream) -> Result<(), LoaderError> {
    if stream.offset == 0 {
        return Ok(());
    }
    let waited = std::time::Instant::now();
    let channels = [stream.channel.clone()];
    loop {
        let statuses = channel_statuses(&stream.client, &stream.pipe, &channels).await?;
        let status = statuses.get(&stream.channel.to_ascii_uppercase());
        if let Some(status) = status {
            if status.rows_error_count.unwrap_or_default() > 0 {
                return Err(LoaderError::Invalid(format!(
                    "Snowpipe Streaming refused {} rows: {}",
                    status.rows_error_count.unwrap_or_default(),
                    status.last_error_message.as_deref().unwrap_or("no message")
                )));
            }
            if status
                .committed_offset()
                .is_some_and(|offset| offset >= stream.offset)
            {
                return Ok(());
            }
        }
        if waited.elapsed().as_secs() > STREAM_COMMIT_WAIT_SECS {
            return Err(LoaderError::Invalid(format!(
                "Snowpipe Streaming did not commit within {STREAM_COMMIT_WAIT_SECS} seconds"
            )));
        }
        tokio::time::sleep(std::time::Duration::from_millis(STREAM_POLL_MILLIS)).await;
    }
}
