//! The crossing settings every Python read uses.

use crate::bindings::constants::{
    DEFAULT_BATCH_ROWS, DEFAULT_DICT_CROSSING, DEFAULT_MAX_QUEUED, DEFAULT_MAX_QUEUED_BYTES,
};
use crate::crossing::main::default_parse_threads::default_parse_threads;
use crate::crossing::models::CrossingOptions;

/// How a table function's yields, or what a `transform` returned, cross into Arrow.
pub fn crossing_options() -> CrossingOptions {
    CrossingOptions {
        batch_rows: DEFAULT_BATCH_ROWS,
        max_queued: DEFAULT_MAX_QUEUED,
        dict_crossing: DEFAULT_DICT_CROSSING,
        parse_threads: default_parse_threads(),
        panic_at: None,
        max_queued_bytes: DEFAULT_MAX_QUEUED_BYTES,
    }
}
