use crate::batching::main::compress_chunks::compress_chunks;
use crate::batching::models::{Batch, BatchRow, CompressedChunk, Partition};
use crate::config::models::OversizedPolicy;
use crate::delivery::errors::DeliveryError;
use crate::delivery::models::{Confirmation, DeliveryReport, StreamingDelivery};
use crate::http::errors::ApiError;
use crate::naming::models::Stream;
use crate::streaming_api::models::StreamingClient;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

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

    /// Open the partition's channel if needed and return Snowflake's committed offset for it.
    pub(crate) async fn open(
        &mut self,
        partition: &Partition,
    ) -> Result<Option<i64>, DeliveryError> {
        let channel = self.stream.channel(&partition.topic, partition.partition);
        let opened = self
            .client
            .open_channel(&self.stream.streaming_pipe, &channel)
            .await
            .map_err(open_error)?;
        self.continuations
            .insert(partition.clone(), opened.continuation_token);
        self.pending.remove(partition);
        Ok(opened.committed_offset)
    }

    /// True when the partition's channel is open in this process.
    pub(crate) fn is_open(&self, partition: &Partition) -> bool {
        self.continuations.contains_key(partition)
    }

    /// Append every row, split into requests under the size limit; rows count as confirmed only after [`Self::poll`].
    pub(crate) async fn deliver(&mut self, batch: Batch) -> Result<DeliveryReport, DeliveryError> {
        let mut report = DeliveryReport::default();
        for (partition, rows) in batch.by_partition() {
            if !self.is_open(&partition) {
                return Err(DeliveryError::Reset(format!(
                    "channel for {}/{} is not open",
                    partition.topic, partition.partition
                )));
            }
            let chunks = self.chunks(rows, &mut report)?;
            for chunk in chunks {
                let continuation = self
                    .continuations
                    .get(&partition)
                    .cloned()
                    .unwrap_or_default();
                let channel = self.stream.channel(&partition.topic, partition.partition);
                let sent = chunk.body.len() as u64;
                let next = self
                    .client
                    .append(
                        &self.stream.streaming_pipe,
                        &channel,
                        &continuation,
                        chunk.last_offset,
                        chunk.body,
                    )
                    .await
                    .map_err(|error| reset("append", &error))?;
                self.continuations.insert(partition.clone(), next);
                self.pending.insert(partition.clone(), chunk.last_offset);
                report.rows += chunk.rows as u64;
                report.uncompressed_bytes += chunk.uncompressed_bytes;
                report.sent_bytes += sent;
                report.requests += 1;
            }
        }
        Ok(report)
    }

    fn chunks(
        &self,
        mut rows: Vec<&BatchRow>,
        report: &mut DeliveryReport,
    ) -> Result<Vec<CompressedChunk>, DeliveryError> {
        loop {
            match compress_chunks(&rows, self.max_request_bytes) {
                Ok(chunks) => return Ok(chunks),
                Err(oversized) if self.oversized == OversizedPolicy::Skip => {
                    tracing::warn!(
                        topic = %oversized.topic,
                        partition = oversized.partition,
                        offset = oversized.offset,
                        compressed_bytes = oversized.compressed_bytes,
                        "skipping a row too large for one streaming request"
                    );
                    rows.retain(|row| row.offset != oversized.offset);
                    report.skipped_rows += 1;
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

    /// True while appended rows wait for Snowflake to commit them.
    pub(crate) fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Check channel status and return partitions whose appended rows are now committed.
    pub(crate) async fn poll(&mut self) -> Result<Confirmation, DeliveryError> {
        let mut confirmation = Confirmation::default();
        if self.pending.is_empty() {
            return Ok(confirmation);
        }
        let names: BTreeSet<String> = self
            .pending
            .keys()
            .map(|partition| self.stream.channel(&partition.topic, partition.partition))
            .collect();
        let names: Vec<String> = names.into_iter().collect();
        let statuses = self
            .client
            .channel_statuses(&self.stream.streaming_pipe, &names)
            .await
            .map_err(|error| reset("channel status", &error))?;
        let pending = std::mem::take(&mut self.pending);
        for (partition, offset) in pending {
            let channel = self.stream.channel(&partition.topic, partition.partition);
            let status = statuses.get(&channel).cloned().unwrap_or_default();
            if !status.is_healthy() {
                return Err(DeliveryError::Reset(format!(
                    "channel {channel} reports {}: {}",
                    status.channel_status_code.unwrap_or_default(),
                    status.last_error_message.unwrap_or_default()
                )));
            }
            confirmation.error_rows +=
                self.error_delta(&channel, status.rows_error_count.unwrap_or_default());
            match status.committed_offset() {
                Some(committed) if committed >= offset => {
                    confirmation.confirmed.insert(partition, offset);
                }
                Some(committed) => {
                    confirmation.confirmed.insert(partition.clone(), committed);
                    self.pending.insert(partition, offset);
                }
                None => {
                    self.pending.insert(partition, offset);
                }
            }
        }
        Ok(confirmation)
    }

    fn error_delta(&mut self, channel: &str, total: i64) -> u64 {
        let previous = self.error_rows.insert(channel.to_owned(), total);
        match previous {
            Some(previous) if total > previous => (total - previous) as u64,
            _ => 0,
        }
    }

    /// Forget every channel; the next delivery reopens them.
    pub(crate) fn reset(&mut self) {
        self.continuations.clear();
        self.pending.clear();
    }

    /// Forget channels for partitions this process no longer owns.
    pub(crate) fn forget(&mut self, partitions: &[Partition]) {
        for partition in partitions {
            self.continuations.remove(partition);
            self.pending.remove(partition);
        }
    }
}

fn reset(action: &str, error: &ApiError) -> DeliveryError {
    DeliveryError::Reset(format!("{action} failed: {error}"))
}

fn open_error(error: ApiError) -> DeliveryError {
    match error.is_retryable() || error.status() == Some(409) {
        true => DeliveryError::Retry(format!("opening the channel failed: {error}")),
        false => DeliveryError::Fatal(format!("opening the channel failed: {error}")),
    }
}
