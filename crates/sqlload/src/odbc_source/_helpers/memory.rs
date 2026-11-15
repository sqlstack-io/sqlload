//! The memory a machine gives SQLLoad, the budget one fetch of a table takes of it, and how `plan` shows them.

use std::path::{Path, PathBuf};

use crate::number_text::constants::BINARY_UNIT;
use crate::number_text::main::memory_text::memory_text;
use crate::odbc_source::constants::{
    CGROUP_V1_LIMIT, CGROUP_V1_ROOT, CGROUP_V2_LIMIT, CGROUP_V2_PREFIX, CGROUP_V2_ROOT,
    DEFAULT_BATCH_MEMORY, FETCH_SHARE_OF_LIMIT, MEMINFO_TOTAL, MEMINFO_UNIT, MEMORY_CONTROLLER,
    NAMED_LONG_COLUMNS, PROC_CGROUP, PROC_MEMINFO, UNLIMITED,
};
use crate::odbc_source::models::{BudgetReason, FetchShape, FetchSize, MemoryBudget, MemoryLimit};

/// The budget of one fetch: `set` (the source's `max_batch_memory`) as it is; else 512 MiB, lowered to half of `limit` shared between `concurrency` tables when that is less (never raised); 512 MiB when the limit is unknown.
pub(crate) fn fetch_budget(
    set: Option<usize>,
    limit: Option<MemoryLimit>,
    concurrency: usize,
) -> MemoryBudget {
    let concurrency = concurrency.max(1);
    let share = limit.map(|limit| {
        let share = limit.bytes / FETCH_SHARE_OF_LIMIT as u64 / concurrency as u64;
        usize::try_from(share).unwrap_or(usize::MAX).max(1)
    });
    let (bytes, reason) = match (set, share) {
        (Some(bytes), _) => (bytes.max(1), BudgetReason::Set),
        (None, Some(share)) if share < DEFAULT_BATCH_MEMORY => (share, BudgetReason::Capped),
        (None, _) => (DEFAULT_BATCH_MEMORY, BudgetReason::Default),
    };
    MemoryBudget {
        bytes,
        reason,
        limit,
        concurrency,
    }
}

/// The memory this process may use: the lowest cgroup limit over it, when below the machine's memory, else the machine's memory; `None` when neither is known (Windows). Fails, naming the file, when a limit file exists but can't be read or holds no number.
pub(crate) fn memory_limit() -> Result<Option<MemoryLimit>, String> {
    let physical = physical_memory()?;
    let cgroup = match read_text(Path::new(PROC_CGROUP))? {
        Some(groups) => match cgroup_limit(&groups, Path::new(CGROUP_V2_ROOT))? {
            Some(limit) => Some(limit),
            None => cgroup_v1_limit(&groups)?,
        },
        None => None,
    };
    Ok(match (cgroup, physical) {
        (Some(cgroup), Some(physical)) if cgroup >= physical => Some(MemoryLimit {
            bytes: physical,
            container: false,
        }),
        (Some(bytes), _) => Some(MemoryLimit {
            bytes,
            container: true,
        }),
        (None, physical) => physical.map(|bytes| MemoryLimit {
            bytes,
            container: false,
        }),
    })
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

/// `plan`'s `buffer` line: the fetch buffer (the budget) per table, and why; `unreadable` is why the machine's memory couldn't be read.
pub(crate) fn budget_note(budget: &MemoryBudget, unreadable: Option<&str>) -> String {
    let bytes = memory_text(budget.bytes as u64);
    let limit = budget.limit.map(|limit| match limit.container {
        true => format!("{} container limit", memory_text(limit.bytes)),
        false => format!("{} of memory", memory_text(limit.bytes)),
    });
    let concurrency = budget.concurrency;
    match (budget.reason, limit, unreadable) {
        (BudgetReason::Set, _, _) => format!("{bytes} fetch buffer per table (max_batch_memory)"),
        (BudgetReason::Capped, Some(limit), _) => {
            format!("{bytes} fetch buffer per table (capped: {limit}, concurrency {concurrency})")
        }
        (_, Some(limit), _) => {
            format!("{bytes} fetch buffer per table (default; {limit}, concurrency {concurrency})")
        }
        (_, None, Some(reason)) => {
            format!(
                "{bytes} fetch buffer per table (default; this machine's memory can't be read: {reason})"
            )
        }
        (_, None, None) => {
            format!("{bytes} fetch buffer per table (default; this machine's memory is unknown)")
        }
    }
}

/// `plan`'s `fetch` line for a table read as `shape` in fetches of `size`: in bulk, the bytes and rows of one fetch; value by value, the budget and rows that end a fetch, and the long columns, the first few named.
pub(crate) fn memory_line(shape: &FetchShape, size: &FetchSize) -> String {
    let rows = thousands(size.rows);
    if shape.long_columns.is_empty() {
        return format!("~{} per fetch, {rows} rows", memory_text(size.bytes as u64));
    }
    let count = shape.long_columns.len();
    let mut names = shape.long_columns[..count.min(NAMED_LONG_COLUMNS)].join(", ");
    if count > NAMED_LONG_COLUMNS {
        names.push_str(", …");
    }
    let columns = match count {
        1 => format!("1 long column ({names})"),
        _ => format!("{count} long columns ({names})"),
    };
    format!(
        "up to {} or {rows} rows per fetch; {columns}, so all {} columns are read value by value",
        memory_text(size.bytes as u64),
        shape.columns
    )
}

/// `count` with thousands separators, as `65,536`.
fn thousands(count: usize) -> String {
    let digits = count.to_string();
    let mut grouped = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
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

/// The machine's memory: `MemTotal` on Linux, `hw.memsize` on macOS; `None` elsewhere.
fn physical_memory() -> Result<Option<u64>, String> {
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
