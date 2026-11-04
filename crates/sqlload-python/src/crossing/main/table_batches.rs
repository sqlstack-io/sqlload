//! Start reading a table generator into Arrow.

use std::sync::Arc;
use std::sync::mpsc::sync_channel;

use arrow_schema::Schema;
use pyo3::{Bound, PyAny};

use crate::crossing::_helpers::parser::start_parsers;
use crate::crossing::_helpers::producer::produce;
use crate::crossing::models::{ByteBudget, CrossingOptions, CrossingStats, TableBatches};

/// Read `iterable` (a table function's generator) on a background thread, batching dict yields, and parse what it yields into Arrow on others; at most `options.max_queued + options.parse_threads` batches wait between them and the consumer, so the generator pauses when the consumer falls behind.
pub fn table_batches(iterable: &Bound<'_, PyAny>, options: CrossingOptions) -> TableBatches {
    let (batch_sender, batch_receiver) = sync_channel(0);
    let stats = Arc::new(CrossingStats {
        budget: ByteBudget::new(options.max_queued_bytes),
        ..CrossingStats::default()
    });
    let dispatch = start_parsers(options, batch_sender, &stats);
    let source = iterable.clone().unbind();
    let producer_stats = stats.clone();
    std::thread::spawn(move || produce(source, options, dispatch, producer_stats));
    TableBatches {
        receiver: batch_receiver,
        schema: Schema::empty(),
        stats,
        finished: false,
    }
}
