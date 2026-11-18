/// The memory the OS lets this process use, and what limits it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryLimit {
    pub bytes: u64,
    pub origin: LimitOrigin,
}

/// What limits the process's memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitOrigin {
    /// A cgroup's `memory.max` (or v1's `memory.limit_in_bytes`) below the machine's memory.
    Container,
    /// A Windows job object's process or job memory limit, below the machine's memory.
    JobObject,
    /// Nothing but the machine's memory.
    Machine,
}

/// `memory_limit` (or `--memory-limit`) as set: the memory a run stays under, or `off`, for no budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemorySetting {
    /// The budget, in bytes.
    Limit(u64),
    /// No budget: no watchdog, no pieces, and DuckDB's own `memory_limit` and threads.
    Off,
}

impl std::str::FromStr for MemorySetting {
    type Err = String;

    /// `off`, or a size such as `4GiB`.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.trim().eq_ignore_ascii_case("off") {
            true => Ok(Self::Off),
            false => crate::config::_helpers::units::parse_byte_size(text)
                .map(Self::Limit)
                .map_err(|error| format!("{error}, or off")),
        }
    }
}

impl<'de> serde::Deserialize<'de> for MemorySetting {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Number(u64),
            Text(String),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Number(bytes) => Ok(Self::Limit(bytes)),
            Raw::Text(text) => text.parse().map_err(serde::de::Error::custom),
        }
    }
}

/// Where the run's budget came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetOrigin {
    /// The project's `memory_limit`.
    Setting,
    /// `--memory-limit`.
    Flag,
    /// A share of the limit the OS sets.
    Detected,
}

/// What a run's budget is worked out from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetRequest {
    /// `--memory-limit`, else the project's `memory_limit`, and which.
    pub set: Option<(u64, BudgetOrigin)>,
    /// The limit the OS sets, when it can be read.
    pub limit: Option<MemoryLimit>,
    /// What the process holds already: the interpreter, drivers and libraries loaded.
    pub overhead: u64,
    /// The run's DuckDB and MotherDuck targets, each its own database.
    pub duckdb: usize,
    /// The most tables loading at once.
    pub concurrency: usize,
    /// The machine's cores.
    pub cores: u64,
}

/// The memory a run plans to stay under, and how it is shared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryBudget {
    pub bytes: u64,
    pub origin: BudgetOrigin,
    /// The limit the OS sets, when it can be read.
    pub limit: Option<MemoryLimit>,
    pub overhead: u64,
    /// Each DuckDB and MotherDuck target's share, when the run has one.
    pub duckdb: Option<DuckDbShare>,
    /// How many DuckDB and MotherDuck targets share `duckdb` each.
    pub duckdb_targets: usize,
    /// Each table's share: its read's buffers, the pages it requests ahead and the batches waiting to be written.
    pub table_share: u64,
    /// How many tables load at once: the run's concurrency, or fewer when that many shares don't fit.
    pub tables_at_once: usize,
}

/// One DuckDB or MotherDuck target's part of the budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DuckDbShare {
    /// All DuckDB holds, which is more than its `memory_limit`.
    pub bytes: u64,
    /// The `memory_limit` DuckDB is given.
    pub memory_limit: u64,
    pub threads: u64,
    /// The most one commit's large values hold.
    pub commit_bytes: u64,
    /// The large values committed since DuckDB's last checkpoint at which the next commit checkpoints.
    pub checkpoint_bytes: u64,
    /// The most a table's last row group's large values hold before its load stops.
    pub row_group_bytes: u64,
}

/// A run's memory budget as worked out before its targets connect, and what `plan` says of it.
#[derive(Debug, Clone, Default)]
pub struct RunMemory {
    /// `None` when the run has no budget: its limit is unknown, or it wasn't worked out (a run not from the command line).
    pub budget: Option<MemoryBudget>,
    /// `plan`'s `memory` line; `None` when it wasn't worked out.
    pub line: Option<String>,
    /// The line asks for something, so `plan` shows it without `-v` too: the run has no budget, or it lowers `concurrency`.
    pub asks: bool,
    /// Why the budget doesn't fit, which fails the plan.
    pub problem: Option<String>,
    /// The watchdog keeping a load under the budget, started when the load starts.
    pub guard: Option<std::sync::Arc<MemoryGuard>>,
}

/// A load's watch over its memory: the budget, the tables loading and what each holds, the bridge processes counted with the process, and how close to the budget it is.
#[derive(Debug)]
pub struct MemoryGuard {
    pub budget: MemoryBudget,
    pub tables: std::sync::Mutex<Vec<std::sync::Arc<TableWatch>>>,
    /// The process ids of the MotherDuck bridges, whose memory counts too.
    pub children: std::sync::Mutex<Vec<u32>>,
    /// At or past [`HOLD_PERCENT`](crate::memory_budget::constants::HOLD_PERCENT) of the budget at the last reading: no table starts, and pieces are cut smaller.
    pub holding: std::sync::atomic::AtomicBool,
    /// Wakes what waits for `holding` to end.
    pub released: tokio::sync::Notify,
    /// The most the process (and its bridges) held at a reading.
    pub peak: std::sync::atomic::AtomicU64,
    /// Why the watchdog stopped the whole run, once it did (see [`RUN_STOP_PERCENT`](crate::memory_budget::constants::RUN_STOP_PERCENT)).
    pub run_stop: std::sync::Mutex<Option<String>>,
    /// Cancelled when the watchdog stops the whole run.
    pub run_stopped: tokio_util::sync::CancellationToken,
}

/// One loading table, as the watchdog sees it.
#[derive(Debug)]
pub struct TableWatch {
    /// The table as its outcome names it.
    pub name: String,
    /// The bytes the table holds itself: its batch being written and its open commit.
    pub held: std::sync::atomic::AtomicU64,
    /// Each of its DuckDB connections' interrupt: a message stops the statement running on it.
    pub interrupts: std::sync::Mutex<Vec<std::sync::mpsc::Sender<()>>>,
    /// Why the watchdog stopped it, once it did.
    pub tripped: std::sync::Mutex<Option<String>>,
}

/// The table the running task loads, with its run's guard.
#[derive(Debug, Clone)]
pub struct WatchedTable {
    pub guard: std::sync::Arc<MemoryGuard>,
    pub table: std::sync::Arc<TableWatch>,
}
