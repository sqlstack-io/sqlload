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
use crate::adapters::snowflake::main::session::_streaming_client::streaming_client;
use crate::adapters::snowflake::models::{RowStream, SnowflakeSql, SnowflakeWriter, StageRoute};
use crate::loader::errors::LoaderError;
use crate::row_batches::main::text_rows::text_rows;
use crate::schema_plan::models::DesiredColumn;
use crate::streaming_api::main::append_rows::append_rows as append_stream_rows;
use crate::streaming_api::main::channel_statuses::channel_statuses;
use crate::streaming_api::main::open_channel::open_channel;
use crate::streaming_api::models::AppendRequest;

impl SnowflakeWriter {
    /// Add `batch` to the staging table without files: a request to the channel, or `INSERT`s.
    pub(crate) async fn send_rows(
        &mut self,
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
        if matches!(self.route, Some(StageRoute::Streaming)) {
            self.open_stream(target, &staging).await?;
        }
        match self.stream.as_mut() {
            Some(stream) => stream.send(&in_batch, &rows).await,
            None => {
                for statement in SnowflakeSql.render_insert_rows(&staging, &in_batch, &rows) {
                    run_statements(&self.destination, &[statement]).await?;
                }
                Ok(())
            }
        }
    }

    /// Open the channel on first use. A chain-chosen stream that will not open drops to `INSERT`, as no rows have been sent; a forced one fails.
    async fn open_stream(
        &mut self,
        target: &WriteTarget<'_>,
        staging: &TableName,
    ) -> Result<(), LoaderError> {
        if self.stream.is_some() {
            return Ok(());
        }
        let client = streaming_client(&self.destination, staging);
        let pipe = format!(
            "{}{STREAM_PIPE_SUFFIX}",
            SnowflakeSql.stored_name(&staging.table)
        );
        let channel = format!("sqlload_{}", target.load_id.replace('-', "_"));
        match open_channel(&client, &pipe, &channel).await {
            Ok(opened) => {
                self.stream = Some(RowStream {
                    client,
                    pipe,
                    channel,
                    continuation: opened.continuation_token,
                    offset: 0,
                });
                Ok(())
            }
            Err(error) => Err(LoaderError::Invalid(format!(
                "Snowpipe Streaming will not open for {}: {error}",
                staging.table
            ))),
        }
    }
}

impl RowStream {
    /// Send `rows` to the channel in requests of at most about 4 MiB of gzip NDJSON, keyed by the columns' stored names.
    async fn send(
        &mut self,
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
                self.send_body(std::mem::take(&mut body)).await?;
            }
        }
        if !body.is_empty() {
            self.send_body(body).await?;
        }
        Ok(())
    }

    async fn send_body(&mut self, body: Vec<u8>) -> Result<(), LoaderError> {
        let mut gzip = GzEncoder::new(Vec::new(), GzipLevel::default());
        gzip.write_all(&body)
            .map_err(|error| LoaderError::Invalid(error.to_string()))?;
        let compressed = gzip
            .finish()
            .map_err(|error| LoaderError::Invalid(error.to_string()))?;
        self.offset += 1;
        self.continuation = append_stream_rows(
            &self.client,
            AppendRequest {
                pipe: &self.pipe,
                channel: &self.channel,
                continuation: &self.continuation,
                offset: self.offset,
                body: compressed,
            },
        )
        .await?;
        Ok(())
    }
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
