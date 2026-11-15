//! The space left where a spool holds its rows.

use std::path::Path;

/// The bytes free to an unprivileged user in the file system holding `folder`; `None` where that can't be read, as on Windows.
pub fn free_bytes(folder: &Path) -> Option<u64> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        match rustix::fs::statvfs(folder) {
            Ok(stats) => Some(stats.f_bavail.saturating_mul(stats.f_frsize)),
            Err(_) => None,
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = folder;
        None
    }
}
