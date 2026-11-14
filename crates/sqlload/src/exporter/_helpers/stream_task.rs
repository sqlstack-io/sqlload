use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::{Offset, TopicPartitionList};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep, sleep_until};
use tokio_util::sync::CancellationToken;

use crate::batching::models::{BatchLimits, Batcher};
use crate::batching::models::{BatchRow, BufferGauge, Partition};
use crate::config::main::stream_settings::stream_settings;
use crate::config::models::{Config, StartFrom, StreamSettings};
use crate::delivery::errors::DeliveryError;
use crate::delivery::models::{Delivery, DeliveryReport};
use crate::envelope::main::encode_row::encode_row;
use crate::envelope::models::{KafkaRecord, RowContext};
use crate::http::models::RetryPolicy;
use crate::kafka::main::build_consumer::build_consumer;
use crate::kafka::main::partitions_of::partitions_of;
use crate::kafka::main::to_record::to_record;
use crate::kafka::models::RebalanceEvent;
use crate::kafka::types::StreamKafkaConsumer;
use crate::metrics::models::Metrics;
use crate::naming::models::Stream;
use crate::time_text::main::now_ms::now_ms;

const PRUNE_EVERY: Duration = Duration::from_secs(24 * 3600);
const SHUTDOWN_CONFIRM_WAIT: Duration = Duration::from_secs(60);
const KAFKA_TIMEOUT: Duration = Duration::from_secs(10);
/// Give up early enough that one more attempt (up to a 10-minute statement timeout) still ends inside the consumer's 15-minute poll interval.
const GIVE_UP_AFTER: Duration = Duration::from_secs(4 * 60);

/// The outcome of opening a partition's channel.
enum Opening {
    Opened(Option<i64>),
    Cancelled,
}

/// What a stream task needs.
pub(crate) struct StreamInputs {
    pub(crate) config: Arc<Config>,
    pub(crate) stream: Stream,
    pub(crate) settings: StreamSettings,
    pub(crate) delivery: Delivery,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) gauge: BufferGauge,
    pub(crate) prefetch_kbytes: u64,
}

struct StreamTask {
    config: Arc<Config>,
    stream: Stream,
    start_from: StartFrom,
    delivery: Delivery,
    metrics: Arc<Metrics>,
    consumer: StreamKafkaConsumer,
    batcher: Batcher,
    resume_after: BTreeMap<Partition, Option<i64>>,
    committed: BTreeMap<Partition, i64>,
    retry: RetryPolicy,
    cancel: CancellationToken,
    unwrap: BTreeMap<String, BTreeMap<String, String>>,
    no_unwrap: BTreeMap<String, String>,
    time_zone: chrono_tz::Tz,
    /// When the stream paused its partitions because its uncommitted files reached `max_pending_files`.
    paused_since: Option<std::time::Instant>,
}

/// Consume, batch, deliver and commit one stream until `cancel` fires.
pub(crate) async fn run_stream(
    inputs: StreamInputs,
    cancel: CancellationToken,
) -> Result<(), String> {
    let (consumer, events) = build_consumer(
        &inputs.config,
        &inputs.stream,
        inputs.settings.start_from,
        inputs.prefetch_kbytes,
    )?;
    let limits = BatchLimits {
        max_bytes: inputs.settings.max_bytes,
        max_rows: inputs.settings.max_rows,
        max_age: inputs.settings.max_age,
        max_total_bytes: inputs.config.batching.max_total_bytes.0,
    };
    let unwrap: BTreeMap<String, BTreeMap<String, String>> = inputs
        .stream
        .topics
        .iter()
        .map(|topic| (topic.clone(), stream_settings(&inputs.config, topic).unwrap))
        .filter(|(_, columns)| !columns.is_empty())
        .collect();
    let mut task = StreamTask {
        unwrap,
        no_unwrap: BTreeMap::new(),
        time_zone: inputs
            .config
            .envelope
            .time_zone
            .parse()
            .unwrap_or(chrono_tz::Tz::UTC),
        batcher: Batcher::new(limits, inputs.gauge),
        config: inputs.config,
        stream: inputs.stream,
        start_from: inputs.settings.start_from,
        delivery: inputs.delivery,
        metrics: inputs.metrics,
        consumer,
        resume_after: BTreeMap::new(),
        committed: BTreeMap::new(),
        retry: RetryPolicy::default(),
        cancel: cancel.clone(),
        paused_since: None,
    };
    tracing::info!(stream = %task.stream.name, topics = ?task.stream.topics, group = %task.stream.consumer_group, "stream started");
    let outcome = task.run(events, &cancel).await;
    let clean = match &outcome {
        Ok(()) => true,
        Err(_) => false,
    };
    task.finish(clean).await;
    tracing::info!(stream = %task.stream.name, "stream stopped");
    outcome
}

impl StreamTask {
    async fn run(
        &mut self,
        mut events: UnboundedReceiver<RebalanceEvent>,
        cancel: &CancellationToken,
    ) -> Result<(), String> {
        let mut confirm = interval(self.delivery.confirm_interval());
        confirm.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut prune = interval(PRUNE_EVERY);
        loop {
            self.pause_when_full();
            let deadline = self.batcher.deadline().map_or_else(
                || Instant::now() + Duration::from_secs(3600),
                Instant::from_std,
            );
            tokio::select! {
                biased;
                () = cancel.cancelled() => return Ok(()),
                Some(event) = events.recv() => self.on_rebalance(event),
                _ = confirm.tick(), if self.delivery.has_pending() => self.confirm().await?,
                message = self.consumer.recv() => {
                    let record = message.map(|message| to_record(&message));
                    match record {
                        Ok(record) => self.on_record(record).await?,
                        Err(error) => tracing::warn!(stream = %self.stream.name, %error, "Kafka consume error"),
                    }
                }
                () = sleep_until(deadline), if !self.batcher.is_empty() && self.paused_since.is_none() => {}
                _ = prune.tick() => self.prune().await,
            }
            if self.paused_since.is_none() && self.batcher.is_due(std::time::Instant::now()) {
                self.flush(cancel).await?;
            }
        }
    }

    fn on_rebalance(&mut self, event: RebalanceEvent) {
        match event {
            RebalanceEvent::Assigned(partitions) => {
                tracing::info!(stream = %self.stream.name, ?partitions, "partitions assigned");
                if self.paused_since.is_some() {
                    self.set_paused(true);
                }
            }
            RebalanceEvent::Revoked(partitions) => {
                tracing::info!(stream = %self.stream.name, ?partitions, "partitions revoked");
                self.batcher.retain(|row| {
                    !partitions.iter().any(|partition| {
                        partition.topic == row.topic && partition.partition == row.partition
                    })
                });
                self.delivery.forget(&partitions);
                for partition in &partitions {
                    self.resume_after.remove(partition);
                    self.committed.remove(partition);
                }
            }
        }
    }

    async fn on_record(&mut self, record: KafkaRecord) -> Result<(), String> {
        let partition = Partition {
            topic: record.topic.clone(),
            partition: record.partition,
        };
        if self.delivery.resumes_from_warehouse() && !self.delivery.is_open(&partition) {
            let Opening::Opened(committed) = self.open_partition(&partition).await? else {
                return Ok(());
            };
            self.resume_after.insert(partition.clone(), committed);
            if let Some(committed) = committed
                && record.offset > committed + 1
            {
                tracing::warn!(stream = %self.stream.name, topic = %partition.topic, partition = partition.partition, committed, offset = record.offset, "Kafka is ahead of the channel; rewinding to the channel's committed offset");
                if self
                    .seek(&partition, Offset::Offset(committed + 1))
                    .is_err()
                {
                    self.forget(&partition);
                }
                return Ok(());
            }
        }
        if let Some(Some(committed)) = self.resume_after.get(&partition)
            && record.offset <= *committed
        {
            return Ok(());
        }
        let context = RowContext {
            exported_at_ms: now_ms(),
            time_zone: self.time_zone,
            unwrap: self.unwrap.get(&record.topic).unwrap_or(&self.no_unwrap),
        };
        let line = encode_row(&self.config.envelope.columns, &record, &context);
        let row = BatchRow {
            topic: record.topic,
            partition: record.partition,
            offset: record.offset,
            line,
        };
        self.batcher.push(row, std::time::Instant::now());
        Ok(())
    }

    async fn open_partition(&mut self, partition: &Partition) -> Result<Opening, String> {
        let started = std::time::Instant::now();
        let mut attempt = 0;
        loop {
            match self.delivery.open(partition).await {
                Ok(committed) => return Ok(Opening::Opened(committed)),
                Err(DeliveryError::Fatal(message)) => return Err(message),
                Err(error) if started.elapsed() > GIVE_UP_AFTER => {
                    return Err(format!(
                        "gave up opening {}/{}: {error}",
                        partition.topic, partition.partition
                    ));
                }
                Err(error) => {
                    attempt += 1;
                    self.count_error(&error);
                    tokio::select! {
                        () = self.cancel.cancelled() => return Ok(Opening::Cancelled),
                        () = sleep(self.retry.delay(attempt)) => {}
                    }
                }
            }
        }
    }

    /// Drop a partition's channel and resume point so its next record reopens and re-checks it.
    fn forget(&mut self, partition: &Partition) {
        self.delivery.forget(std::slice::from_ref(partition));
        self.resume_after.remove(partition);
    }

    async fn flush(&mut self, cancel: &CancellationToken) -> Result<(), String> {
        let Some(batch) = self.batcher.take() else {
            return Ok(());
        };
        let started = std::time::Instant::now();
        let mut attempt = 0;
        loop {
            match self.delivery.deliver(&batch).await {
                Ok(report) => {
                    self.record(&report);
                    self.commit(&report.confirmed);
                    self.record_backlog();
                    return Ok(());
                }
                Err(DeliveryError::Fatal(message)) => return Err(message),
                Err(error @ DeliveryError::Reset(_)) => {
                    self.count_error(&error);
                    return self.reset().await;
                }
                Err(error) if started.elapsed() > GIVE_UP_AFTER => {
                    return Err(format!(
                        "gave up delivering a batch after {GIVE_UP_AFTER:?}: {error}"
                    ));
                }
                Err(error @ DeliveryError::Retry(_)) => {
                    attempt += 1;
                    self.count_error(&error);
                    tokio::select! {
                        () = cancel.cancelled() => return Ok(()),
                        () = sleep(self.retry.delay(attempt)) => {}
                    }
                }
            }
        }
    }

    /// Pause the assigned partitions while the stream's uncommitted files fill `max_pending_files`, and resume once a confirmation frees room; the consumer keeps polling either way, so the group membership holds.
    fn pause_when_full(&mut self) {
        let full = self.delivery.is_full();
        match (full, self.paused_since) {
            (true, None) => {
                tracing::info!(stream = %self.stream.name, pending = self.delivery.pending_files(), "pausing partitions until Snowpipe reports pending files");
                self.paused_since = Some(std::time::Instant::now());
                self.set_paused(true);
            }
            (false, Some(since)) => {
                if !self.set_paused(false) {
                    return;
                }
                tracing::info!(stream = %self.stream.name, "resumed partitions");
                self.paused_since = None;
                let waited = u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX);
                self.metrics
                    .pending_wait
                    .get_or_create(&Metrics::label(&self.stream.name))
                    .inc_by(waited);
            }
            _ => {}
        }
    }

    /// Pause or resume the assigned partitions; false when that failed, so the caller tries again on its next pass.
    fn set_paused(&self, paused: bool) -> bool {
        let assigned = match self.consumer.assignment() {
            Ok(assigned) => assigned,
            Err(error) => {
                tracing::warn!(stream = %self.stream.name, %error, "cannot read the assignment to pause or resume it");
                return false;
            }
        };
        let outcome = match paused {
            true => self.consumer.pause(&assigned),
            false => self.consumer.resume(&assigned),
        };
        match outcome {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(stream = %self.stream.name, %error, paused, "cannot pause or resume partitions");
                false
            }
        }
    }

    async fn confirm(&mut self) -> Result<(), String> {
        let polled = self.delivery.poll().await;
        self.record_backlog();
        match polled {
            Ok(confirmation) => {
                let label = Metrics::label(&self.stream.name);
                self.metrics
                    .error_rows
                    .get_or_create(&label)
                    .inc_by(confirmation.error_rows);
                self.metrics
                    .skipped_files
                    .get_or_create(&label)
                    .inc_by(confirmation.skipped_files);
                if let Some(error) = &confirmation.deferred_error {
                    self.count_error(error);
                }
                self.commit(&confirmation.confirmed);
                Ok(())
            }
            Err(DeliveryError::Fatal(message)) => Err(message),
            Err(error @ DeliveryError::Reset(_)) => {
                self.count_error(&error);
                self.reset().await
            }
            Err(error) => {
                self.count_error(&error);
                Ok(())
            }
        }
    }

    async fn reset(&mut self) -> Result<(), String> {
        tracing::warn!(stream = %self.stream.name, "resetting channels and resuming from committed offsets");
        self.batcher.clear();
        self.delivery.reset();
        self.resume_after.clear();
        let assigned = self
            .consumer
            .assignment()
            .map_err(|error| error.to_string())?;
        let partitions = partitions_of(&assigned);
        for partition in partitions {
            let Opening::Opened(committed) = self.open_partition(&partition).await? else {
                return Ok(());
            };
            self.resume_after.insert(partition.clone(), committed);
            let offset = match committed {
                Some(committed) => Offset::Offset(committed + 1),
                None => match self.kafka_committed(&partition).await? {
                    Some(offset) => offset,
                    None => return Ok(()),
                },
            };
            self.seek(&partition, offset).map_err(|error| {
                format!(
                    "cannot rewind {}/{} after a reset: {error}",
                    partition.topic, partition.partition
                )
            })?;
        }
        Ok(())
    }

    /// Where `partition` resumes from Kafka's committed offset for the group: after it, or `start_from` when the group has none. Reading it is retried; when it still fails, the stream stops rather than resume from `start_from`, which with `latest` would skip records. `None` when cancelled.
    async fn kafka_committed(&mut self, partition: &Partition) -> Result<Option<Offset>, String> {
        let started = std::time::Instant::now();
        let mut attempt = 0;
        loop {
            let mut list = TopicPartitionList::new();
            list.add_partition(&partition.topic, partition.partition);
            let found = tokio::task::block_in_place(|| {
                self.consumer.committed_offsets(list, KAFKA_TIMEOUT)
            })
            .map_err(|error| error.to_string())
            .and_then(|found| committed_offset(&found));
            match resume_offset(found, self.start_from) {
                Ok(offset) => return Ok(Some(offset)),
                Err(error) if started.elapsed() > GIVE_UP_AFTER => {
                    return Err(format!(
                        "cannot read the committed offset of {}/{} for consumer group {} after {attempt} retries: {error}; the stream stops rather than resume from start_from, which could skip records. Fix: check the brokers and the group, then restart",
                        partition.topic, partition.partition, self.stream.consumer_group
                    ));
                }
                Err(error) => {
                    attempt += 1;
                    tracing::warn!(stream = %self.stream.name, topic = %partition.topic, partition = partition.partition, %error, attempt, "cannot read the committed offset; retrying");
                    tokio::select! {
                        () = self.cancel.cancelled() => return Ok(None),
                        () = sleep(self.retry.delay(attempt)) => {}
                    }
                }
            }
        }
    }

    fn seek(&self, partition: &Partition, offset: Offset) -> Result<(), String> {
        let sought = tokio::task::block_in_place(|| {
            self.consumer
                .seek(&partition.topic, partition.partition, offset, KAFKA_TIMEOUT)
        });
        sought.map_err(|error| {
            tracing::warn!(stream = %self.stream.name, topic = %partition.topic, partition = partition.partition, %error, "seek failed");
            error.to_string()
        })
    }

    fn commit(&mut self, offsets: &BTreeMap<Partition, i64>) {
        if offsets.is_empty() {
            return;
        }
        for (partition, offset) in offsets {
            self.committed.insert(partition.clone(), *offset);
        }
        self.commit_with(offsets, CommitMode::Async);
    }

    fn commit_with(&self, offsets: &BTreeMap<Partition, i64>, mode: CommitMode) {
        if offsets.is_empty() {
            return;
        }
        let mut list = TopicPartitionList::new();
        for (partition, offset) in offsets {
            let _ = list.add_partition_offset(
                &partition.topic,
                partition.partition,
                Offset::Offset(offset + 1),
            );
        }
        let outcome = match mode {
            CommitMode::Sync => tokio::task::block_in_place(|| self.consumer.commit(&list, mode)),
            CommitMode::Async => self.consumer.commit(&list, mode),
        };
        if let Err(error) = outcome {
            tracing::warn!(stream = %self.stream.name, %error, "offset commit failed; the rows will be delivered again after a restart");
        }
    }

    fn record(&self, report: &DeliveryReport) {
        let label = Metrics::label(&self.stream.name);
        let metrics = &self.metrics;
        metrics.rows.get_or_create(&label).inc_by(report.rows);
        metrics
            .uncompressed_bytes
            .get_or_create(&label)
            .inc_by(report.uncompressed_bytes);
        metrics
            .sent_bytes
            .get_or_create(&label)
            .inc_by(report.sent_bytes);
        metrics
            .requests
            .get_or_create(&label)
            .inc_by(report.requests);
        metrics
            .skipped_rows
            .get_or_create(&label)
            .inc_by(report.skipped_rows);
        metrics.batches.get_or_create(&label).inc();
        metrics
            .last_delivery
            .get_or_create(&label)
            .set(now_ms() / 1000);
        tracing::debug!(stream = %self.stream.name, rows = report.rows, sent_bytes = report.sent_bytes, requests = report.requests, "batch delivered");
    }

    fn record_backlog(&self) {
        let label = Metrics::label(&self.stream.name);
        let files = self.delivery.pending_files();
        self.metrics
            .pending_files
            .get_or_create(&label)
            .set(i64::try_from(files).unwrap_or(i64::MAX));
        let age_seconds = self
            .delivery
            .oldest_pending_ms()
            .map_or(0, |sent| (now_ms() - sent).max(0) / 1000);
        self.metrics
            .oldest_pending
            .get_or_create(&label)
            .set(age_seconds);
    }

    fn count_error(&self, error: &DeliveryError) {
        tracing::warn!(stream = %self.stream.name, %error, "delivery problem");
        self.metrics
            .delivery_errors
            .get_or_create(&Metrics::label(&self.stream.name))
            .inc();
    }

    async fn prune(&self) {
        match self.delivery.prune().await {
            Ok(0) => {}
            Ok(removed) => {
                tracing::info!(stream = %self.stream.name, removed, "removed expired staged files")
            }
            Err(error) => {
                tracing::warn!(stream = %self.stream.name, %error, "stage cleanup failed")
            }
        }
    }

    /// Deliver what is buffered and wait briefly for confirmations, unless the stream failed; then commit what was confirmed.
    async fn finish(&mut self, clean: bool) {
        if clean {
            self.drain().await;
        }
        let committed = std::mem::take(&mut self.committed);
        self.commit_with(&committed, CommitMode::Sync);
    }

    async fn drain(&mut self) {
        let never = CancellationToken::new();
        let deadline = std::time::Instant::now() + SHUTDOWN_CONFIRM_WAIT;
        let flushed = tokio::time::timeout(SHUTDOWN_CONFIRM_WAIT, self.flush(&never)).await;
        if !matches!(flushed, Ok(Ok(()))) {
            tracing::warn!(stream = %self.stream.name, "final flush did not finish; unconfirmed rows will be delivered again");
        }
        let poll = self.delivery.confirm_interval();
        while self.delivery.has_pending() && std::time::Instant::now() < deadline {
            if self.confirm().await.is_err() {
                break;
            }
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if self.delivery.has_pending() {
                sleep(poll.min(left)).await;
            }
        }
    }
}

/// The committed offset in the one-partition list `found`, `None` when the list is empty; an error Kafka reports for the partition is an error, not a missing offset.
fn committed_offset(found: &TopicPartitionList) -> Result<Option<Offset>, String> {
    let Some(element) = found.elements().into_iter().next() else {
        return Ok(None);
    };
    element.error().map_err(|error| error.to_string())?;
    Ok(Some(element.offset()))
}

/// Where a partition resumes from the committed offset Kafka `found` for the group: after it, or `start_from` when the group has none; a failed read stays an error, to retry, never `start_from`.
pub(crate) fn resume_offset(
    found: Result<Option<Offset>, String>,
    start_from: StartFrom,
) -> Result<Offset, String> {
    match (found?, start_from) {
        (Some(Offset::Offset(offset)), _) => Ok(Offset::Offset(offset)),
        (_, StartFrom::Earliest) => Ok(Offset::Beginning),
        (_, StartFrom::Latest) => Ok(Offset::End),
    }
}
