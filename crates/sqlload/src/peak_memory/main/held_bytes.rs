//! What a process holds now.

/// What process `pid` (this one when `None`, such as a bridge child otherwise) holds now, in bytes: resident on Linux and macOS, its working set or committed memory, whichever is more, on Windows.
pub fn held_bytes(pid: Option<u32>) -> Result<u64, String> {
    #[cfg(target_os = "linux")]
    return crate::peak_memory::_helpers::linux::resident_bytes(pid);
    #[cfg(target_os = "macos")]
    return crate::peak_memory::_helpers::macos::resident_bytes(pid.unwrap_or(std::process::id()));
    #[cfg(windows)]
    return crate::peak_memory::_helpers::windows::held_bytes(pid);
    #[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
    {
        let _ = pid;
        Err(crate::peak_memory::constants::NO_HELD_MEMORY.to_owned())
    }
}
