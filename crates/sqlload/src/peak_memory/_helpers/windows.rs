//! The process's memory and the limits on it, on Windows. Windows gives them only through FFI calls no maintained crate wraps safely (`GetProcessMemoryInfo`, `IsProcessInJob`, `QueryInformationJobObject`, `GlobalMemoryStatusEx`), so this module alone allows `unsafe` among the crate's Windows code, for those calls.

#![allow(unsafe_code)]

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
use windows_sys::Win32::System::JobObjects::{
    IsProcessInJob, JOB_OBJECT_LIMIT_JOB_MEMORY, JOB_OBJECT_LIMIT_PROCESS_MEMORY,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
    QueryInformationJobObject,
};
use windows_sys::Win32::System::ProcessStatus::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
};

/// The most memory the process has held in its working set at once (`PeakWorkingSetSize`), in bytes.
pub(crate) fn peak_working_set() -> Result<u64, String> {
    let counters = memory_counters(current_process())?;
    u64::try_from(counters.PeakWorkingSetSize).map_err(|error| error.to_string())
}

/// What process `pid` (this one when `None`) holds now, in bytes: its working set or its committed memory, whichever is more, as a job object's limit counts the committed memory. The calls are sound: `OpenProcess` returns a handle or null, checked, which is closed once read.
pub(crate) fn held_bytes(pid: Option<u32>) -> Result<u64, String> {
    let counters = match pid {
        None => memory_counters(current_process())?,
        Some(pid) => {
            let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
            if handle.is_null() {
                return Err(format!(
                    "OpenProcess {pid}: {}",
                    std::io::Error::last_os_error()
                ));
            }
            let counters = memory_counters(handle);
            unsafe { CloseHandle(handle) };
            counters?
        }
    };
    let held = counters.WorkingSetSize.max(counters.PagefileUsage);
    u64::try_from(held).map_err(|error| error.to_string())
}

/// This process's pseudo-handle, which needs no closing.
fn current_process() -> HANDLE {
    unsafe { GetCurrentProcess() }
}

/// The lowest memory limit of the job object this process runs in (its per-process or its whole-job limit), in bytes; `None` outside a job or when the job sets neither. Both calls are sound: `in_job` and `limits` are writable values of the sizes passed, and a null job handle names the calling process's own job, as the API documents.
pub(crate) fn job_memory_limit() -> Result<Option<u64>, String> {
    let mut in_job = 0;
    let read = unsafe { IsProcessInJob(current_process(), std::ptr::null_mut(), &mut in_job) };
    if read == 0 {
        return Err(format!(
            "IsProcessInJob: {}",
            std::io::Error::last_os_error()
        ));
    }
    if in_job == 0 {
        return Ok(None);
    }
    let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    let size = u32::try_from(std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
        .map_err(|error| error.to_string())?;
    let read = unsafe {
        QueryInformationJobObject(
            std::ptr::null_mut(),
            JobObjectExtendedLimitInformation,
            std::ptr::from_mut(&mut limits).cast(),
            size,
            std::ptr::null_mut(),
        )
    };
    if read == 0 {
        return Err(format!(
            "QueryInformationJobObject: {}",
            std::io::Error::last_os_error()
        ));
    }
    let flags = limits.BasicLimitInformation.LimitFlags;
    let process =
        (flags & JOB_OBJECT_LIMIT_PROCESS_MEMORY != 0).then_some(limits.ProcessMemoryLimit);
    let job = (flags & JOB_OBJECT_LIMIT_JOB_MEMORY != 0).then_some(limits.JobMemoryLimit);
    let lowest = match (process, job) {
        (Some(process), Some(job)) => Some(process.min(job)),
        (process, job) => process.or(job),
    };
    lowest
        .map(|bytes| u64::try_from(bytes).map_err(|error| error.to_string()))
        .transpose()
}

/// The machine's physical memory, in bytes. The call is sound: `status` is a writable `MEMORYSTATUSEX` whose `dwLength` holds its size, as `GlobalMemoryStatusEx` requires.
pub(crate) fn physical_memory() -> Result<u64, String> {
    let mut status = MEMORYSTATUSEX {
        dwLength: u32::try_from(std::mem::size_of::<MEMORYSTATUSEX>())
            .map_err(|error| error.to_string())?,
        ..MEMORYSTATUSEX::default()
    };
    match unsafe { GlobalMemoryStatusEx(&mut status) } {
        0 => Err(format!(
            "GlobalMemoryStatusEx: {}",
            std::io::Error::last_os_error()
        )),
        _ => Ok(status.ullTotalPhys),
    }
}

/// The memory counters of the process `handle`. The call is sound: `handle` is this process's pseudo-handle or one `OpenProcess` returned and not yet closed, and `counters` is a writable `PROCESS_MEMORY_COUNTERS` of the `size` bytes passed, as `GetProcessMemoryInfo` requires.
fn memory_counters(handle: HANDLE) -> Result<PROCESS_MEMORY_COUNTERS, String> {
    let mut counters = PROCESS_MEMORY_COUNTERS::default();
    let size = u32::try_from(std::mem::size_of::<PROCESS_MEMORY_COUNTERS>())
        .map_err(|error| error.to_string())?;
    match unsafe { GetProcessMemoryInfo(handle, &mut counters, size) } {
        0 => Err(format!(
            "GetProcessMemoryInfo: {}",
            std::io::Error::last_os_error()
        )),
        _ => Ok(counters),
    }
}
