//! Linux's resident high-water mark: reading it, and resetting it so the next read covers only what came after.

use crate::peak_memory::constants::{
    CLEAR_REFS, HIGH_WATER_MARK_FIELD, KIB, PROCESS_STATUS, RESET_HIGH_WATER_MARK, RESIDENT_FIELD,
};

/// The process's resident high-water mark (`VmHWM` in `/proc/self/status`), in bytes.
pub(crate) fn high_water_mark() -> Result<u64, String> {
    status_bytes(PROCESS_STATUS, HIGH_WATER_MARK_FIELD)
}

/// What process `pid` (this one when `None`) holds resident now (`VmRSS` in its `/proc/<pid>/status`), in bytes.
pub(crate) fn resident_bytes(pid: Option<u32>) -> Result<u64, String> {
    match pid {
        None => status_bytes(PROCESS_STATUS, RESIDENT_FIELD),
        Some(pid) => status_bytes(&format!("/proc/{pid}/status"), RESIDENT_FIELD),
    }
}

/// The bytes of the `field` line (in kB) of the process status file at `path`.
fn status_bytes(path: &str, field: &str) -> Result<u64, String> {
    let status = std::fs::read_to_string(path).map_err(|error| format!("{path}: {error}"))?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .and_then(|value| value.split_whitespace().next())
        .ok_or_else(|| format!("{path} has no {field} line"))?;
    let kib: u64 = kib
        .parse()
        .map_err(|error| format!("{path} {field} {kib}: {error}"))?;
    Ok(kib.saturating_mul(KIB))
}

/// Reset the high-water mark to what the process holds now.
pub(crate) fn reset_high_water_mark() -> Result<(), String> {
    std::fs::write(CLEAR_REFS, RESET_HIGH_WATER_MARK)
        .map_err(|error| format!("{CLEAR_REFS}: {error}"))
}
