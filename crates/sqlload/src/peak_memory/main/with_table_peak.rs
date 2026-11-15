//! One table's peak memory, for `load --memory-per-table` (Linux).

/// `work`'s output, and when `measure` (Linux only) the most memory the process held while it ran, in bytes: the high-water mark is reset to what the process holds when `work` starts, and read when it ends.
pub async fn with_table_peak<T>(
    measure: bool,
    work: impl std::future::Future<Output = T>,
) -> (T, Option<u64>) {
    #[cfg(target_os = "linux")]
    if measure {
        use crate::peak_memory::_helpers::linux::{high_water_mark, reset_high_water_mark};
        let reset = reset_high_water_mark();
        let output = work.await;
        return match reset.and_then(|()| high_water_mark()) {
            Ok(bytes) => (output, Some(bytes)),
            Err(error) => {
                tracing::warn!("could not measure the table's peak memory: {error}");
                (output, None)
            }
        };
    }
    #[cfg(not(target_os = "linux"))]
    let _ = measure;
    (work.await, None)
}
