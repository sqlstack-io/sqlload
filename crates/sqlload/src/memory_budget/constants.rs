/// What a cgroup's limit file holds when it sets none.
pub const UNLIMITED: &str = "max";

/// The cgroup v1 controller that limits memory, as `/proc/self/cgroup` names it.
pub const MEMORY_CONTROLLER: &str = "memory";

/// The cgroup v2 line of `/proc/self/cgroup`, before the group's path.
pub const CGROUP_V2_PREFIX: &str = "0::";

/// Each cgroup v2 and v1 group's memory limit file.
pub const CGROUP_V2_LIMIT: &str = "memory.max";
pub const CGROUP_V1_LIMIT: &str = "memory.limit_in_bytes";

/// The `/proc/meminfo` line of the machine's memory, and its unit.
pub const MEMINFO_TOTAL: &str = "MemTotal:";
pub const MEMINFO_UNIT: &str = "kB";

/// Where Linux lists the process's cgroups: `0::/path` under cgroup v2, `N:memory:/path` under v1.
pub const PROC_CGROUP: &str = "/proc/self/cgroup";

/// Where cgroup v2 is mounted; each group's limit is `memory.max` in its folder.
pub const CGROUP_V2_ROOT: &str = "/sys/fs/cgroup";

/// Where cgroup v1's memory controller is mounted; each group's limit is `memory.limit_in_bytes` in its folder.
pub const CGROUP_V1_ROOT: &str = "/sys/fs/cgroup/memory";

/// Where Linux reports the machine's memory (`MemTotal:  16314228 kB`).
pub const PROC_MEMINFO: &str = "/proc/meminfo";

/// The percent of a container's or Windows job object's limit the budget takes when `memory_limit` is unset: the process is alone under it, but the OS stops it at the limit.
pub const LIMIT_SHARE_PERCENT: u64 = 80;

/// The percent of the machine's memory the budget takes when `memory_limit` is unset and nothing else limits the process: the machine is likely shared.
pub const MACHINE_SHARE_PERCENT: u64 = 50;

/// The percent of the budget, after the overhead, that a DuckDB or MotherDuck target takes.
pub const DUCKDB_SHARE_PERCENT: u64 = 30;

/// DuckDB holds up to about 1.9 times its `memory_limit` (its buffer pool plus what it allocates outside it, such as its copies of the Arrow batches it reads; measured on DuckDB 2.0 with rows of 100 bytes to 1 MiB), so its `memory_limit` is half its share.
pub const DUCKDB_FOOTPRINT: u64 = 2;

/// DuckDB runs a statement on one thread per this much of its `memory_limit`, between two and the machine's cores: fewer threads hold less memory past the limit, more merge faster.
pub const DUCKDB_THREAD_MEMORY: u64 = 128 * 1024 * 1024;

/// The fewest threads DuckDB runs a statement on.
pub const MIN_DUCKDB_THREADS: u64 = 2;

/// The least `memory_limit` DuckDB is given: 256MB, below which rows of a few hundred KiB failed even in small commits; a budget without room for it and one table fails `plan`.
pub const MIN_DUCKDB_MEMORY_LIMIT: u64 = 256_000_000;

/// The percent of DuckDB's `memory_limit` one commit's large values (see [`LARGE_VALUE_BYTES`]) may take.
pub const DUCKDB_COMMIT_PERCENT: u64 = 25;

/// The percent of DuckDB's `memory_limit` its large values may take, committed since its last checkpoint, before the next commit checkpoints: they stay in memory until then.
pub const DUCKDB_CHECKPOINT_PERCENT: u64 = 40;

/// The percent of DuckDB's `memory_limit` the large values of one table's last row group may take: a checkpoint rewrites that row group whole, and one of 1.4 times the limit was the most that checkpointed at 256MB.
pub const DUCKDB_ROW_GROUP_PERCENT: u64 = 125;

/// The size from which DuckDB stores a value in blocks that stay in memory until a checkpoint: a third of its 256 KiB block (values of 87,000 bytes committed 1.6 GB in one transaction at 256MB; of 87,500 bytes, not 244 MiB), less a little.
pub const LARGE_VALUE_BYTES: usize = 85 * 1024;

/// The rows of a DuckDB row group.
pub const DUCKDB_ROW_GROUP_ROWS: u64 = 122_880;

/// The least memory one table loads in: 64 MiB.
pub const MIN_TABLE_SHARE: u64 = 64 * 1024 * 1024;

/// How many parts a table's share is cut into: the source's buffer (an ODBC fetch, the HTTP pages requested ahead), the batches read ahead of the one being written, and that one.
pub const TABLE_SHARE_PARTS: u64 = 4;

/// The percent of the budget at which no table or piece starts, and pieces are cut smaller.
pub const HOLD_PERCENT: u64 = 80;

/// The percent of the budget at which the largest table's running statement is interrupted and the table fails.
pub const STOP_PERCENT: u64 = 90;

/// How often the watchdog reads the process's memory: rows of 1 MiB grew it by up to 10 MiB per 10 ms.
pub const WATCH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

/// How many times a piece's large values count when it goes through a staging table: written there, read back, and written to the table (a merge of 24 rows of 1 MiB from a staging table committed at 256MB; of 32, not).
pub const STAGED_COPIES: u64 = 3;

/// How much smaller pieces are cut while the watchdog holds the load at 80% of its budget.
pub const HOLDING_DIVISOR: u64 = 4;
