/// Linux's `ru_maxrss` counts kibibytes (macOS's counts bytes), as `/proc/self/status` does.
pub const KIB: u64 = 1024;

/// Writing `5` to this file resets the process's resident high-water mark (`VmHWM`) to what it holds now (Linux).
pub const CLEAR_REFS: &str = "/proc/self/clear_refs";
pub const RESET_HIGH_WATER_MARK: &str = "5";

/// The file whose `VmHWM:` line holds the process's resident high-water mark, in kB (Linux).
pub const PROCESS_STATUS: &str = "/proc/self/status";
pub const HIGH_WATER_MARK_FIELD: &str = "VmHWM:";

/// The status line of what a process holds resident now, in kB (Linux).
pub const RESIDENT_FIELD: &str = "VmRSS:";

/// Why `--memory-per-table` is refused off Linux.
pub const MEMORY_PER_TABLE_NEEDS_LINUX: &str =
    "--memory-per-table needs Linux; the run's peak is in every report";

/// Why there is no peak memory figure on an OS other than Linux, macOS and Windows.
pub const NO_PEAK_MEMORY: &str = "this OS gives no peak memory figure";

/// Why there is no figure for what a process holds now on an OS other than Linux, macOS and Windows.
pub const NO_HELD_MEMORY: &str = "this OS gives no figure for the memory a process holds";
