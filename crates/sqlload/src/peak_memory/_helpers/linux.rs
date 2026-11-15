//! Linux's resident high-water mark: reading it, and resetting it so the next read covers only what came after.

use crate::peak_memory::constants::{
    CLEAR_REFS, HIGH_WATER_MARK_FIELD, KIB, PROCESS_STATUS, RESET_HIGH_WATER_MARK,
};

/// The process's resident high-water mark (`VmHWM` in `/proc/self/status`), in bytes.
pub(crate) fn high_water_mark() -> Result<u64, String> {
    let status = std::fs::read_to_string(PROCESS_STATUS)
        .map_err(|error| format!("{PROCESS_STATUS}: {error}"))?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix(HIGH_WATER_MARK_FIELD))
        .and_then(|value| value.split_whitespace().next())
        .ok_or_else(|| format!("{PROCESS_STATUS} has no {HIGH_WATER_MARK_FIELD} line"))?;
    let kib: u64 = kib
        .parse()
        .map_err(|error| format!("{PROCESS_STATUS} {HIGH_WATER_MARK_FIELD} {kib}: {error}"))?;
    Ok(kib.saturating_mul(KIB))
}

/// Reset the high-water mark to what the process holds now.
pub(crate) fn reset_high_water_mark() -> Result<(), String> {
    std::fs::write(CLEAR_REFS, RESET_HIGH_WATER_MARK)
        .map_err(|error| format!("{CLEAR_REFS}: {error}"))
}
