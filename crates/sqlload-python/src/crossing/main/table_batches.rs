//! Start reading a table generator into Arrow.

use std::sync::Arc;
use std::sync::mpsc::sync_channel;

use arrow_schema::Schema;
use pyo3::{Bound, PyAny};

use crate::crossing::_helpers::parser::parse;
use crate::crossing::_helpers::producer::produce;
use crate::crossing::models::{CrossingOptions, CrossingStats, TableBatches};

/// Read `iterable` (a table function's generator) on a background thread, batching dict yields, and parse what it yields into Arrow on another; the queues between them hold `options.max_queued` batches each, so the generator pauses when the consumer falls behind.
pub fn table_batches(iterable: &Bound<'_, PyAny>, options: CrossingOptions) -> TableBatches {
    let queued = options.max_queued.max(1);
    let (crossed_sender, crossed_receiver) = sync_channel(queued);
    let (batch_sender, batch_receiver) = sync_channel(queued);
    let stats = Arc::new(CrossingStats::default());
    let source = iterable.clone().unbind();
    let producer_stats = stats.clone();
    std::thread::spawn(move || produce(source, options, crossed_sender, producer_stats));
    let parser_stats = stats.clone();
    let workers = options.parse_threads;
    std::thread::spawn(move || parse(crossed_receiver, batch_sender, parser_stats, workers));
    TableBatches {
        receiver: batch_receiver,
        schema: Schema::empty(),
        stats,
    }
}
