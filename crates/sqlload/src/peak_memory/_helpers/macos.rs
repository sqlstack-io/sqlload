//! What a process holds resident now, on macOS. macOS gives it only through `proc_pidinfo`, an FFI call no maintained crate wraps safely without generating bindings at build time, so this module alone allows `unsafe` among the crate's macOS code, for that one call.

#![allow(unsafe_code)]

/// What process `pid` holds resident now (`pti_resident_size`), in bytes. The call is sound: `info` is a writable `proc_taskinfo` of the `size` bytes passed, as `proc_pidinfo` requires, and it writes at most that many.
pub(crate) fn resident_bytes(pid: u32) -> Result<u64, String> {
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = i32::try_from(std::mem::size_of::<libc::proc_taskinfo>())
        .map_err(|error| error.to_string())?;
    let pid = i32::try_from(pid).map_err(|error| error.to_string())?;
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTASKINFO,
            0,
            std::ptr::from_mut(&mut info).cast(),
            size,
        )
    };
    match written == size {
        true => Ok(info.pti_resident_size),
        false => Err(format!(
            "proc_pidinfo {pid}: {}",
            std::io::Error::last_os_error()
        )),
    }
}
