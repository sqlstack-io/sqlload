//! The process's peak working set on Windows. Windows gives it only through `GetProcessMemoryInfo`, an FFI call no maintained crate wraps safely, so this module alone allows `unsafe` (the crate denies it everywhere else), for that one call.

#![allow(unsafe_code)]

use windows_sys::Win32::System::ProcessStatus::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
use windows_sys::Win32::System::Threading::GetCurrentProcess;

/// The most memory the process has held in its working set at once (`PeakWorkingSetSize`), in bytes. The call is sound: `GetCurrentProcess` returns a pseudo-handle that needs no closing, and `counters` is a writable `PROCESS_MEMORY_COUNTERS` of the `size` bytes passed, as `GetProcessMemoryInfo` requires.
pub(crate) fn peak_working_set() -> Result<u64, String> {
    let mut counters = PROCESS_MEMORY_COUNTERS::default();
    let size = u32::try_from(std::mem::size_of::<PROCESS_MEMORY_COUNTERS>())
        .map_err(|error| error.to_string())?;
    let read = unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, size) };
    match read {
        0 => Err(format!(
            "GetProcessMemoryInfo: {}",
            std::io::Error::last_os_error()
        )),
        _ => u64::try_from(counters.PeakWorkingSetSize).map_err(|error| error.to_string()),
    }
}
