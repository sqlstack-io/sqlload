//! The process's peak memory as the OS keeps it.

/// The most memory the process has held resident at once, from `getrusage`'s `ru_maxrss`: kibibytes on Linux, bytes on macOS.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn os_peak_bytes() -> Result<u64, String> {
    use nix::sys::resource::{UsageWho, getrusage};
    let usage = getrusage(UsageWho::RUSAGE_SELF).map_err(|error| error.to_string())?;
    let max_rss = u64::try_from(usage.max_rss()).map_err(|error| error.to_string())?;
    match cfg!(target_os = "macos") {
        true => Ok(max_rss),
        false => Ok(max_rss.saturating_mul(crate::peak_memory::constants::KIB)),
    }
}

/// The most memory the process has held in its working set at once.
#[cfg(windows)]
pub(crate) fn os_peak_bytes() -> Result<u64, String> {
    crate::peak_memory::_helpers::windows::peak_working_set()
}

/// Elsewhere the OS gives no figure.
#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
pub(crate) fn os_peak_bytes() -> Result<u64, String> {
    Err(crate::peak_memory::constants::NO_PEAK_MEMORY.to_owned())
}
