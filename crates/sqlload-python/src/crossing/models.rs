use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_array::ffi_stream::RecordBatchStreamReader;
use arrow_schema::Schema;
use pyo3::pybacked::PyBackedBytes;
use sqlload::row_batches::main::merge_schema::merge_schema;
use sqlload::row_batches::models::{JsonRead, RowColumns};

use crate::crossing::errors::CrossError;

/// How gathered dicts cross into Rust.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictCrossing {
    /// Rust reads the dicts directly, holding the GIL.
    Walk,
    /// `orjson` writes the batch as JSON bytes, which Rust parses without the GIL; batches orjson cannot write are walked.
    Serialise,
}

/// How a table generator is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CrossingOptions {
    /// Rows gathered from dict yields before they cross; also the most rows per batch parsed from JSON bytes.
    pub batch_rows: usize,
    /// Crossed batches waiting to be parsed, and parsed batches waiting to be consumed, before the generator pauses.
    pub max_queued: usize,
    pub dict_crossing: DictCrossing,
    /// Threads parsing crossed batches into Arrow at once.
    pub parse_threads: usize,
}

/// One piece of yielded data, crossed into Rust and waiting to be parsed.
#[derive(Debug)]
pub(crate) enum Crossed {
    Columns(RowColumns),
    Json {
        bytes: PyBackedBytes,
        read: JsonRead,
    },
    ArrowStream(RecordBatchStreamReader),
    ArrowBatch(RecordBatch),
    Failed(CrossError),
}

/// Counters of one table read, for benchmarks and logs.
#[derive(Debug, Default)]
pub struct CrossingStats {
    pub(crate) rows: AtomicU64,
    pub(crate) batches: AtomicU64,
    /// Time spent reading or serialising dicts with the GIL held.
    pub(crate) gil_nanos: AtomicU64,
    /// Time spent parsing and converting to Arrow without the GIL.
    pub(crate) parse_nanos: AtomicU64,
}

impl CrossingStats {
    /// The counters so far.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            rows: self.rows.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            gil_seconds: self.gil_nanos.load(Ordering::Relaxed) as f64 / 1e9,
            parse_seconds: self.parse_nanos.load(Ordering::Relaxed) as f64 / 1e9,
        }
    }

    pub(crate) fn add(counter: &AtomicU64, elapsed: Duration) {
        counter.fetch_add(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}

/// A table read's counters at one moment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StatsSnapshot {
    pub rows: u64,
    pub batches: u64,
    /// Seconds spent reading or serialising dicts with the GIL held.
    pub gil_seconds: f64,
    /// Seconds spent parsing and converting to Arrow without the GIL, summed over parser threads.
    pub parse_seconds: f64,
}

/// What waiting for the next batch found.
#[derive(Debug)]
pub enum Polled {
    Batch(RecordBatch),
    Failed(CrossError),
    /// Nothing arrived within the timeout.
    Pending,
    Finished,
}

/// The record batches of one table generator, each typed by inference, and the schema merged from all so far.
#[derive(Debug)]
pub struct TableBatches {
    pub(crate) receiver: Receiver<Result<RecordBatch, CrossError>>,
    pub(crate) schema: Schema,
    pub(crate) stats: Arc<CrossingStats>,
}

impl TableBatches {
    /// The next batch, waiting at most `timeout`; call it with the GIL released.
    pub fn poll(&mut self, timeout: Duration) -> Polled {
        match self.receiver.recv_timeout(timeout) {
            Ok(Ok(batch)) => {
                self.schema = merge_schema(&self.schema, &batch.schema());
                Polled::Batch(batch)
            }
            Ok(Err(error)) => Polled::Failed(error),
            Err(RecvTimeoutError::Timeout) => Polled::Pending,
            Err(RecvTimeoutError::Disconnected) => Polled::Finished,
        }
    }

    /// The schema merged from every batch returned so far.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// This read's counters.
    pub fn stats(&self) -> &CrossingStats {
        &self.stats
    }
}

impl Iterator for TableBatches {
    type Item = Result<RecordBatch, CrossError>;

    /// The next batch, blocking; call it with the GIL released.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.poll(Duration::from_secs(3600)) {
                Polled::Batch(batch) => return Some(Ok(batch)),
                Polled::Failed(error) => return Some(Err(error)),
                Polled::Pending => {}
                Polled::Finished => return None,
            }
        }
    }
}
