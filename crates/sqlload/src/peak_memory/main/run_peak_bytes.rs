//! The run's peak memory.

use crate::peak_memory::_helpers::os_peak::os_peak_bytes;

/// The most memory the whole process has held at once, in bytes: every table, driver, DuckDB and Python included (`getrusage`'s `ru_maxrss` on Linux and macOS, `PeakWorkingSetSize` on Windows), or any of the `measured` peaks if higher (what `--memory-per-table` read, which the OS's figure may have lost when it reset Linux's high-water mark); `None` when neither gives one.
pub fn run_peak_bytes(measured: &[Option<u64>]) -> Option<u64> {
    let os = match os_peak_bytes() {
        Ok(bytes) => Some(bytes),
        Err(error) => {
            tracing::debug!("no peak memory figure: {error}");
            None
        }
    };
    measured.iter().copied().chain([os]).flatten().max()
}
