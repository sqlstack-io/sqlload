use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// One encoded row and where it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchRow {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub line: String,
}

/// A topic partition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Partition {
    pub topic: String,
    pub partition: i32,
}

/// The rows flushed from one stream together.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Batch {
    pub rows: Vec<BatchRow>,
    pub bytes: u64,
}

impl Batch {
    /// The highest offset per partition in this batch.
    pub fn last_offsets(&self) -> BTreeMap<Partition, i64> {
        let mut offsets: BTreeMap<Partition, i64> = BTreeMap::new();
        for row in &self.rows {
            let key = Partition {
                topic: row.topic.clone(),
                partition: row.partition,
            };
            let entry = offsets.entry(key).or_insert(row.offset);
            *entry = (*entry).max(row.offset);
        }
        offsets
    }

    /// The rows as newline-delimited JSON.
    pub fn ndjson(&self) -> Vec<u8> {
        let mut output: Vec<u8> = Vec::with_capacity(self.bytes as usize + self.rows.len());
        for row in &self.rows {
            output.extend_from_slice(row.line.as_bytes());
            output.push(b'\n');
        }
        output
    }

    /// The rows grouped by partition, each group in offset order.
    pub fn by_partition(&self) -> BTreeMap<Partition, Vec<&BatchRow>> {
        let mut grouped: BTreeMap<Partition, Vec<&BatchRow>> = BTreeMap::new();
        for row in &self.rows {
            grouped
                .entry(Partition {
                    topic: row.topic.clone(),
                    partition: row.partition,
                })
                .or_default()
                .push(row);
        }
        for rows in grouped.values_mut() {
            rows.sort_by_key(|row| row.offset);
        }
        grouped
    }
}

/// Bytes buffered across every stream, shared so one stream can flush early when the process is near its cap.
#[derive(Debug, Clone, Default)]
pub struct BufferGauge(Arc<AtomicU64>);

impl BufferGauge {
    pub fn add(&self, bytes: u64) {
        self.0.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn sub(&self, bytes: u64) {
        self.0.fetch_sub(bytes, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// One gzip request body for a single partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompressedChunk {
    pub body: Vec<u8>,
    pub rows: usize,
    pub uncompressed_bytes: u64,
    pub last_offset: i64,
}

/// A row too large for one request even on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OversizedRow {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub compressed_bytes: u64,
}

/// Flush limits for one stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimits {
    pub max_bytes: u64,
    pub max_rows: usize,
    pub max_age: Duration,
    pub max_total_bytes: u64,
}

/// Rows buffered for one stream.
#[derive(Debug)]
pub struct Batcher {
    limits: BatchLimits,
    gauge: BufferGauge,
    batch: Batch,
    started: Option<Instant>,
}

impl Batcher {
    pub fn new(limits: BatchLimits, gauge: BufferGauge) -> Self {
        Self {
            limits,
            gauge,
            batch: Batch::default(),
            started: None,
        }
    }

    pub fn push(&mut self, row: BatchRow, now: Instant) {
        let bytes = row.line.len() as u64 + 1;
        self.started.get_or_insert(now);
        self.batch.bytes += bytes;
        self.batch.rows.push(row);
        self.gauge.add(bytes);
    }

    pub fn len(&self) -> usize {
        self.batch.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.batch.rows.is_empty()
    }

    pub fn bytes(&self) -> u64 {
        self.batch.bytes
    }

    /// When the buffered rows must be flushed by age.
    pub fn deadline(&self) -> Option<Instant> {
        self.started.map(|started| started + self.limits.max_age)
    }

    /// True when any limit is reached, including the process-wide buffer cap.
    pub fn is_due(&self, now: Instant) -> bool {
        if self.is_empty() {
            return false;
        }
        self.batch.bytes >= self.limits.max_bytes
            || self.batch.rows.len() >= self.limits.max_rows
            || self.deadline().is_some_and(|deadline| now >= deadline)
            || self.gauge.get() >= self.limits.max_total_bytes
    }

    /// Take the buffered rows as one batch.
    pub fn take(&mut self) -> Option<Batch> {
        if self.is_empty() {
            return None;
        }
        self.started = None;
        let batch = std::mem::take(&mut self.batch);
        self.gauge.sub(batch.bytes);
        Some(batch)
    }

    /// Drop every buffered row, e.g. after a delivery reset.
    pub fn clear(&mut self) {
        let _ = self.take();
    }

    /// Keep only rows for which `keep` is true, e.g. dropping partitions revoked in a rebalance.
    pub fn retain(&mut self, keep: impl Fn(&BatchRow) -> bool) {
        let before = self.batch.bytes;
        self.batch.rows.retain(|row| keep(row));
        self.batch.bytes = self
            .batch
            .rows
            .iter()
            .map(|row| row.line.len() as u64 + 1)
            .sum();
        self.gauge.sub(before - self.batch.bytes);
        if self.batch.rows.is_empty() {
            self.started = None;
        }
    }
}

impl Drop for Batcher {
    fn drop(&mut self) {
        self.gauge.sub(self.batch.bytes);
    }
}
