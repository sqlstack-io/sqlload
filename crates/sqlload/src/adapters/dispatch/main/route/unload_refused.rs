//! Whether a destination reads a warehouse's unloaded files itself.

use crate::adapters::dispatch::models::Destination;
use crate::routing::models::Store;

/// Why `destination` doesn't read the files a warehouse source unloads to `unload_to` itself, so SQLLoad reads them: MotherDuck reads only a files source's Parquet on its servers so far; `None` otherwise.
pub fn unload_refused(destination: &Destination, unload_to: &[Store]) -> Option<String> {
    if unload_to.is_empty() {
        return None;
    }
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(duckdb) if duckdb.motherduck => Some(
            "MotherDuck reads only a files source's Parquet in S3 itself, not a warehouse's unload"
                .to_owned(),
        ),
        _ => None,
    }
}
