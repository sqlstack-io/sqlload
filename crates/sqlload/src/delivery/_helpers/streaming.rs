use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::batching::main::compress_chunks::compress_chunks;
use crate::batching::models::{Batch, BatchRow, CompressedChunk, Partition};
use crate::config::models::OversizedPolicy;
use crate::delivery::constants::CONFLICT;
use crate::delivery::errors::DeliveryError;
use crate::delivery::models::{Confirmation, DeliveryReport, StreamingDelivery};
use crate::http::errors::ApiError;
use crate::naming::models::Stream;
use crate::streaming_api::main::append_rows::append_rows;
use crate::streaming_api::main::channel_statuses::channel_statuses;
use crate::streaming_api::main::open_channel::open_channel;
use crate::streaming_api::models::{AppendRequest, ChannelStatus, StreamingClient};

/// Chunks that fit the request limit and how many rows were skipped to get there.
struct Chunked {
    chunks: Vec<CompressedChunk>,
    skipped: u64,
}

impl StreamingDelivery {
    pub(crate) fn new(
        client: Arc<StreamingClient>,
        stream: Stream,
        max_request_bytes: u64,
        oversized: OversizedPolicy,
    ) -> Self {
        Self {
            client,
            stream,
            max_request_bytes,
            oversized,
            continuations: BTreeMap::new(),
            pending: BTreeMap::new(),
            error_rows: BTreeMap::new(),
        }
    }

    fn channel(&self, partition: &Partition) -> String {
        self.stream.channel(&partition.topic, partition.partition)
    }

    /// Open the partition's channel and return Snowflake's committed offset for it.
    pub(crate) async fn open(
        &mut self,
        partition: &Partition,
    ) -> Result<Option<i64>, DeliveryError> {
        let channel = self.channel(partition);
        let opened = open_channel(&self.client, &self.stream.streaming_pipe, &channel)
            .await
            .map_err(open_error)?;
        self.continuations
            .insert(partition.clone(), opened.continuation_token);
        self.error_rows.insert(channel, opened.error_rows);
        self.pending.remove(partition);
        Ok(opened.committed_offset)
    }

    pub(crate) fn is_open(&self, partition: &Partition) -> bool {
        self.continuations.contains_key(partition)
    }

    /// Append every row, split into requests under the size limit; rows count as confirmed only after [`Self::poll`].
    pub(crate) async fn deliver(&mut self, batch: &Batch) -> Result<DeliveryReport, DeliveryError> {
        let mut report = DeliveryReport::default();
        for (partition, rows) in batch.by_partition() {
            if !self.is_open(&partition) {
                return Err(DeliveryError::Reset(format!(
                    "channel for {}/{} is not open",
                    partition.topic, partition.partition
                )));
            }
            let chunked = self.chunk(rows)?;
            report.skipped_rows += chunked.skipped;
            for chunk in chunked.chunks {
                report = add_chunk(report, &chunk);
                self.append(&partition, chunk).await?;
            }
        }
        Ok(report)
    }

    async fn append(
        &mut self,
        partition: &Partition,
        chunk: CompressedChunk,
    ) -> Result<(), DeliveryError> {
        let continuation = self
            .continuations
            .get(partition)
            .cloned()
            .unwrap_or_default();
        let channel = self.channel(partition);
        let request = AppendRequest {
            pipe: &self.stream.streaming_pipe,
            channel: &channel,
            continuation: &continuation,
            offset: chunk.last_offset,
            body: chunk.body,
        };
        let next = append_rows(&self.client, request)
            .await
            .map_err(|error| reset("append", &error))?;
        self.continuations.insert(partition.clone(), next);
        self.pending.insert(partition.clone(), chunk.last_offset);
        Ok(())
    }

    fn chunk(&self, mut rows: Vec<&BatchRow>) -> Result<Chunked, DeliveryError> {
        let mut skipped = 0;
        loop {
            match compress_chunks(&rows, self.max_request_bytes) {
                Ok(chunks) => return Ok(Chunked { chunks, skipped }),
                Err(oversized) if self.oversized == OversizedPolicy::Skip => {
                    tracing::warn!(
                        topic = %oversized.topic,
                        partition = oversized.partition,
                        offset = oversized.offset,
                        compressed_bytes = oversized.compressed_bytes,
                        "skipping a row too large for one streaming request"
                    );
                    rows.retain(|row| row.offset != oversized.offset);
                    skipped += 1;
                }
                Err(oversized) => {
                    return Err(DeliveryError::Fatal(format!(
                        "row {}/{}@{} is {} bytes after gzip, over the {}-byte request limit; set oversized = \"skip\" to drop such rows",
                        oversized.topic,
                        oversized.partition,
                        oversized.offset,
                        oversized.compressed_bytes,
                        self.max_request_bytes
                    )));
                }
            }
        }
    }

    pub(crate) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Check channel status and return partitions whose appended rows are now committed.
    pub(crate) async fn poll(&mut self) -> Result<Confirmation, DeliveryError> {
        if self.pending.is_empty() {
            return Ok(Confirmation::default());
        }
        let names: BTreeSet<String> = self
            .pending
            .keys()
            .map(|partition| self.channel(partition))
            .collect();
        let names: Vec<String> = names.into_iter().collect();
        let statuses = channel_statuses(&self.client, &self.stream.streaming_pipe, &names)
            .await
            .map_err(|error| reset("channel status", &error))?;
        let pending = std::mem::take(&mut self.pending);
        let mut confirmation = Confirmation::default();
        for (partition, offset) in pending {
            let channel = self.channel(&partition);
            let status = statuses.get(&channel).ok_or_else(|| {
                DeliveryError::Reset(format!(
                    "Snowflake reported no status for channel {channel}"
                ))
            })?;
            confirmation = self.settle(confirmation, partition, offset, status)?;
        }
        Ok(confirmation)
    }

    fn settle(
        &mut self,
        mut confirmation: Confirmation,
        partition: Partition,
        offset: i64,
        status: &ChannelStatus,
    ) -> Result<Confirmation, DeliveryError> {
        let channel = self.channel(&partition);
        if !status.is_healthy() {
            return Err(DeliveryError::Reset(format!(
                "channel {channel} reports {}: {}",
                status.channel_status_code.clone().unwrap_or_default(),
                status.last_error_message.clone().unwrap_or_default()
            )));
        }
        confirmation.error_rows +=
            self.error_delta(&channel, status.rows_error_count.unwrap_or_default());
        let committed = status.committed_offset();
        if let Some(committed) = committed {
            confirmation
                .confirmed
                .insert(partition.clone(), committed.min(offset));
        }
        if committed.is_none_or(|committed| committed < offset) {
            self.pending.insert(partition, offset);
        }
        Ok(confirmation)
    }

    fn error_delta(&mut self, channel: &str, total: i64) -> u64 {
        let previous = self
            .error_rows
            .insert(channel.to_owned(), total)
            .unwrap_or_default();
        total.saturating_sub(previous).max(0) as u64
    }

    pub(crate) fn reset(&mut self) {
        self.continuations.clear();
        self.pending.clear();
    }

    pub(crate) fn forget(&mut self, partitions: &[Partition]) {
        for partition in partitions {
            self.continuations.remove(partition);
            self.pending.remove(partition);
        }
    }
}

fn add_chunk(mut report: DeliveryReport, chunk: &CompressedChunk) -> DeliveryReport {
    report.rows += chunk.rows as u64;
    report.uncompressed_bytes += chunk.uncompressed_bytes;
    report.sent_bytes += chunk.body.len() as u64;
    report.requests += 1;
    report
}

fn reset(action: &str, error: &ApiError) -> DeliveryError {
    DeliveryError::Reset(format!("{action} failed: {error}"))
}

fn open_error(error: ApiError) -> DeliveryError {
    match error.is_retryable() || error.status() == Some(CONFLICT) {
        true => DeliveryError::Retry(format!("opening the channel failed: {error}")),
        false => DeliveryError::Fatal(format!("opening the channel failed: {error}")),
    }
}
