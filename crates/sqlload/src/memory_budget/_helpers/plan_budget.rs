//! A run's memory budget and how it is shared.

use crate::memory_budget::constants::{
    DUCKDB_CHECKPOINT_PERCENT, DUCKDB_COMMIT_PERCENT, DUCKDB_FOOTPRINT, DUCKDB_ROW_GROUP_PERCENT,
    DUCKDB_SHARE_PERCENT, DUCKDB_THREAD_MEMORY, LIMIT_SHARE_PERCENT, MACHINE_SHARE_PERCENT,
    MIN_DUCKDB_MEMORY_LIMIT, MIN_DUCKDB_THREADS, MIN_TABLE_SHARE,
};
use crate::memory_budget::models::{
    BudgetOrigin, BudgetRequest, DuckDbShare, LimitOrigin, MemoryBudget,
};
use crate::number_text::constants::{BINARY_UNIT, TENTHS};
use crate::number_text::main::memory_text::memory_text;

/// The run's budget: `memory_limit` (or `--memory-limit`) as set, else 80% of a container's or job object's limit, else 50% of the machine's memory; `None` when none of them is known. What the process already holds (`overhead`) comes off first; the DuckDB and MotherDuck targets then take 30% of the rest together, each one's `memory_limit` half its part (DuckDB holds up to twice it) and at least 256MB, and the tables share what remains, as many at once as get at least 64 MiB each, up to `concurrency`. Fails, naming the budget a run needs, when the overhead alone doesn't fit or what remains gives no table 64 MiB.
pub(crate) fn plan_budget(request: &BudgetRequest) -> Result<Option<MemoryBudget>, String> {
    let (bytes, origin) = match (request.set, request.limit) {
        (Some(set), _) => set,
        (None, Some(limit)) => {
            let percent = match limit.origin {
                LimitOrigin::Container | LimitOrigin::JobObject => LIMIT_SHARE_PERCENT,
                LimitOrigin::Machine => MACHINE_SHARE_PERCENT,
            };
            (percent_of(limit.bytes, percent), BudgetOrigin::Detected)
        }
        (None, None) => return Ok(None),
    };
    let least = least_budget(request.overhead, request.duckdb);
    let raise = |what: String| {
        format!(
            "{what}; raise memory_limit to at least {}",
            setting_text(least)
        )
    };
    if request.overhead >= bytes {
        return Err(raise(format!(
            "the {} memory budget is no more than the {} SQLLoad holds before loading anything",
            memory_text(bytes),
            memory_text(request.overhead)
        )));
    }
    let available = bytes - request.overhead;
    let duckdb = match request.duckdb {
        0 => None,
        targets => Some(duckdb_share(available, (targets as u64, request.cores))),
    };
    let duckdb_bytes = duckdb.map_or(0, |share| share.bytes * request.duckdb as u64);
    let tables = available.saturating_sub(duckdb_bytes);
    if tables < MIN_TABLE_SHARE {
        let after = match duckdb_bytes {
            0 => String::new(),
            bytes => format!(" after DuckDB's {}", memory_text(bytes)),
        };
        return Err(raise(format!(
            "the {} memory budget leaves the tables {}{after}, under the {} one table loads in",
            memory_text(bytes),
            memory_text(tables),
            memory_text(MIN_TABLE_SHARE)
        )));
    }
    let fitting = usize::try_from(tables / MIN_TABLE_SHARE).unwrap_or(usize::MAX);
    let tables_at_once = request.concurrency.max(1).min(fitting);
    Ok(Some(MemoryBudget {
        bytes,
        origin,
        limit: request.limit,
        overhead: request.overhead,
        duckdb,
        duckdb_targets: request.duckdb,
        table_share: tables / tables_at_once as u64,
        tables_at_once,
    }))
}

/// Each of `targets` DuckDB databases' part of `available`, the budget after the overhead, on a machine of `cores`: 30% of it shared between them, but never a `memory_limit` under 256MB.
fn duckdb_share(available: u64, (targets, cores): (u64, u64)) -> DuckDbShare {
    let bytes = (percent_of(available, DUCKDB_SHARE_PERCENT) / targets.max(1))
        .max(MIN_DUCKDB_MEMORY_LIMIT * DUCKDB_FOOTPRINT);
    let memory_limit = bytes / DUCKDB_FOOTPRINT;
    DuckDbShare {
        bytes,
        memory_limit,
        threads: (memory_limit / DUCKDB_THREAD_MEMORY)
            .clamp(MIN_DUCKDB_THREADS, cores.max(MIN_DUCKDB_THREADS)),
        commit_bytes: percent_of(memory_limit, DUCKDB_COMMIT_PERCENT),
        checkpoint_bytes: percent_of(memory_limit, DUCKDB_CHECKPOINT_PERCENT),
        row_group_bytes: percent_of(memory_limit, DUCKDB_ROW_GROUP_PERCENT),
    }
}

/// The least budget a run holding `overhead` fits in: one table's 64 MiB, and each of `duckdb` targets' 256MB `memory_limit` with what DuckDB holds past it.
fn least_budget(overhead: u64, duckdb: usize) -> u64 {
    overhead + MIN_TABLE_SHARE + MIN_DUCKDB_MEMORY_LIMIT * DUCKDB_FOOTPRINT * duckdb as u64
}

/// `bytes` as a `memory_limit` to write: rounded up to whole MiB, or to tenths of a GiB from 1 GiB, as `640MiB` or `1.8GiB`.
pub(in crate::memory_budget) fn setting_text(bytes: u64) -> String {
    let mib = bytes.div_ceil(BINARY_UNIT * BINARY_UNIT);
    match mib < BINARY_UNIT {
        true => format!("{mib}MiB"),
        false => {
            let tenths = (mib * TENTHS).div_ceil(BINARY_UNIT);
            match tenths % TENTHS {
                0 => format!("{}GiB", tenths / TENTHS),
                rest => format!("{}.{rest}GiB", tenths / TENTHS),
            }
        }
    }
}

/// `percent` of `bytes`, rounded down.
fn percent_of(bytes: u64, percent: u64) -> u64 {
    u64::try_from(u128::from(bytes) * u128::from(percent) / 100).unwrap_or(u64::MAX)
}
