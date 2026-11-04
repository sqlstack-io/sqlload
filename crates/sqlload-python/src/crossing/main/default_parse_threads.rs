//! How many threads parse one table's batches by default.

use crate::bindings::constants::MAX_DEFAULT_PARSE_THREADS;

/// One parser per core but one, which the generator's thread keeps, capped at the default.
pub fn default_parse_threads() -> usize {
    std::thread::available_parallelism()
        .map_or(1, |cores| cores.get().saturating_sub(1))
        .clamp(1, MAX_DEFAULT_PARSE_THREADS)
}
