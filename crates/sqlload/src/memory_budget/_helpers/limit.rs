//! The memory limit the OS sets on this process: a container's cgroup, a Windows job object, or the machine's memory.

use std::path::{Path, PathBuf};

use crate::memory_budget::constants::{
    CGROUP_V1_LIMIT, CGROUP_V1_ROOT, CGROUP_V2_LIMIT, CGROUP_V2_PREFIX, CGROUP_V2_ROOT,
    MEMINFO_TOTAL, MEMINFO_UNIT, MEMORY_CONTROLLER, PROC_CGROUP, PROC_MEMINFO, UNLIMITED,
};
use crate::memory_budget::models::{LimitOrigin, MemoryLimit};
use crate::number_text::constants::BINARY_UNIT;

/// The memory this process may use: the Windows job object's limit, else the lowest cgroup limit over it, when below the machine's memory, else the machine's memory; `None` when none is known. Fails, naming the file or call, when a limit exists but can't be read.
pub(crate) fn memory_limit() -> Result<Option<MemoryLimit>, String> {
    let physical = physical_memory()?;
    let set = match job_limit()? {
        Some(bytes) => Some((bytes, LimitOrigin::JobObject)),
        None => container_limit()?.map(|bytes| (bytes, LimitOrigin::Container)),
    };
    Ok(lower_limit(set, physical))
}

/// The lower of `limit` (a cgroup's or job object's) and the machine's memory `physical`, and which it is; `None` when neither is known.
pub(crate) fn lower_limit(
    limit: Option<(u64, LimitOrigin)>,
    physical: Option<u64>,
) -> Option<MemoryLimit> {
    match (limit, physical) {
        (Some((bytes, origin)), Some(physical)) if bytes < physical => {
            Some(MemoryLimit { bytes, origin })
        }
        (Some((bytes, origin)), None) => Some(MemoryLimit { bytes, origin }),
        (_, physical) => physical.map(|bytes| MemoryLimit {
            bytes,
            origin: LimitOrigin::Machine,
        }),
    }
}

/// The lowest cgroup limit over this process (v2's `memory.max` from its group up, else v1's), when any is set. Fails, naming the file, when a limit file exists but can't be read or holds no number.
pub(crate) fn container_limit() -> Result<Option<u64>, String> {
    match read_text(Path::new(PROC_CGROUP))? {
        Some(groups) => match cgroup_limit(&groups, Path::new(CGROUP_V2_ROOT))? {
            Some(limit) => Ok(Some(limit)),
            None => cgroup_v1_limit(&groups),
        },
        None => Ok(None),
    }
}

/// The lowest cgroup v2 `memory.max` from the process's group (its `0::/path` line in `groups`, `/proc/self/cgroup`) up to `root`; `None` when there is no such line or every one is `max` or missing.
pub(crate) fn cgroup_limit(groups: &str, root: &Path) -> Result<Option<u64>, String> {
    let Some(path) = groups
        .lines()
        .find_map(|line| line.strip_prefix(CGROUP_V2_PREFIX))
    else {
        return Ok(None);
    };
    let mut folder: PathBuf = root.join(path.trim().trim_start_matches('/'));
    let mut lowest: Option<u64> = None;
    loop {
        lowest = match (lowest, limit_file(&folder.join(CGROUP_V2_LIMIT))?) {
            (Some(lowest), Some(limit)) => Some(lowest.min(limit)),
            (lowest, limit) => lowest.or(limit),
        };
        if folder.as_path() == root || !folder.pop() || !folder.starts_with(root) {
            return Ok(lowest);
        }
    }
}

/// The machine's memory: `GlobalMemoryStatusEx` on Windows, `MemTotal` on Linux, `hw.memsize` on macOS; `None` elsewhere.
pub(crate) fn physical_memory() -> Result<Option<u64>, String> {
    if cfg!(windows) {
        return windows_memory();
    }
    if let Some(meminfo) = read_text(Path::new(PROC_MEMINFO))? {
        return meminfo_total(&meminfo);
    }
    if !cfg!(target_os = "macos") {
        return Ok(None);
    }
    let output = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .map_err(|error| format!("sysctl hw.memsize: {error}"))?;
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .map(Some)
        .map_err(|error| format!("sysctl hw.memsize: {error}"))
}

/// The lowest memory limit of the Windows job object this process runs in, when it sets one; `None` elsewhere.
pub(crate) fn job_limit() -> Result<Option<u64>, String> {
    #[cfg(windows)]
    return crate::peak_memory::_helpers::windows::job_memory_limit();
    #[cfg(not(windows))]
    Ok(None)
}

/// The machine's memory on Windows.
#[cfg(windows)]
fn windows_memory() -> Result<Option<u64>, String> {
    crate::peak_memory::_helpers::windows::physical_memory().map(Some)
}

/// Not Windows: never asked.
#[cfg(not(windows))]
fn windows_memory() -> Result<Option<u64>, String> {
    Ok(None)
}

/// The cgroup v1 memory limit of the process's group (its `memory` line in `groups`), else of the hierarchy's root, as a container sees it; unlimited reads as a huge number, which the machine's memory then undercuts.
fn cgroup_v1_limit(groups: &str) -> Result<Option<u64>, String> {
    let Some(path) = groups.lines().find_map(memory_group) else {
        return Ok(None);
    };
    let root = Path::new(CGROUP_V1_ROOT);
    match limit_file(
        &root
            .join(path.trim().trim_start_matches('/'))
            .join(CGROUP_V1_LIMIT),
    )? {
        Some(limit) => Ok(Some(limit)),
        None => limit_file(&root.join(CGROUP_V1_LIMIT)),
    }
}

/// The group path of a `/proc/self/cgroup` line of cgroup v1's memory controller, `N:memory:/path`.
fn memory_group(line: &str) -> Option<&str> {
    let mut parts = line.splitn(3, ':');
    let (_, controllers, path) = (parts.next()?, parts.next()?, parts.next()?);
    controllers
        .split(',')
        .any(|name| name == MEMORY_CONTROLLER)
        .then_some(path)
}

/// The bytes of `/proc/meminfo`'s `MemTotal:  16314228 kB` line; `None` without one.
fn meminfo_total(meminfo: &str) -> Result<Option<u64>, String> {
    let Some(total) = meminfo
        .lines()
        .find_map(|line| line.strip_prefix(MEMINFO_TOTAL))
    else {
        return Ok(None);
    };
    let kilobytes = total.trim().trim_end_matches(MEMINFO_UNIT).trim();
    kilobytes
        .parse::<u64>()
        .map(|kilobytes| Some(kilobytes.saturating_mul(BINARY_UNIT)))
        .map_err(|error| format!("{PROC_MEMINFO}: MemTotal {kilobytes:?}: {error}"))
}

/// A cgroup limit file's number; `None` when it is `max` or the file doesn't exist.
fn limit_file(path: &Path) -> Result<Option<u64>, String> {
    let Some(text) = read_text(path)? else {
        return Ok(None);
    };
    match text.trim() {
        UNLIMITED => Ok(None),
        number => number
            .parse()
            .map(Some)
            .map_err(|error| format!("{}: {number:?}: {error}", path.display())),
    }
}

/// A file's text; `None` when it doesn't exist.
fn read_text(path: &Path) -> Result<Option<String>, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("{}: {error}", path.display())),
    }
}
