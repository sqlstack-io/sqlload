use std::collections::BTreeMap;

use crate::adapters::contract::models::BoundParameter;
use crate::auth::models::Secret;
use crate::postgres_source::models::{ReadColumn, SourceTable};

/// What a table's primary key is read for, so the catalog asks the driver (`SQLPrimaryKeys`) only for tables that use it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyUse {
    /// Nothing reads it.
    Unused,
    /// `plan` reads it: beside a merge's own `unique_key`, in the `write_strategy` it suggests, or as a ClickHouse table's sorting key.
    Read,
    /// The table merges on it (`unique_key = "{primary_key}"`): when the driver reports none, the keys `INFORMATION_SCHEMA` lists explain the error.
    Merged,
}

/// What each table of a source reads its primary key for: by `schema.table` as its `[sources.*.tables]` entry names it, else the source's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyUses {
    pub source: KeyUse,
    pub tables: BTreeMap<String, KeyUse>,
}

/// A table or view as `SQLTables` lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedTable {
    pub catalog: String,
    /// Empty for a driver without schemas.
    pub schema: String,
    pub name: String,
}

/// An ODBC source: the connection string its driver manager connects with, and the buffer caps for long values.
#[derive(Debug, Clone)]
pub struct OdbcReader {
    pub(crate) connection_string: Secret,
    pub(crate) limits: ValueLimits,
}

/// The largest text and binary value a read holds, in bytes; `None` lets the driver say, which for unbounded columns (`TEXT`, `VARCHAR(MAX)`) can be far more than any value. A column whose buffer per value would pass `long_column_threshold` (or that has no size) is read value by value instead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ValueLimits {
    pub max_text_size: Option<usize>,
    pub max_binary_size: Option<usize>,
    /// `None` for the default, 4 KiB.
    pub long_column_threshold: Option<usize>,
}

/// How a read fetches a table's rows: the bytes its bound buffers take per row, and the columns too long to bind, which make it read every value on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchShape {
    /// The bytes each row takes in the buffers bound to the columns; 0 when the table is read value by value.
    pub bound_bytes_per_row: usize,
    /// The columns whose buffer per value would pass the threshold, or that have no size, in order.
    pub long_columns: Vec<String>,
    pub columns: usize,
}

/// One fetch under a memory budget: its rows, and the bytes its buffers take (for a table read value by value, the budget, which its values fill at most).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchSize {
    pub rows: usize,
    pub bytes: usize,
}

/// The memory a machine gives SQLLoad: a container's cgroup limit, or else its physical memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryLimit {
    pub bytes: u64,
    /// The limit is a cgroup's `memory.max` (or v1's `memory.limit_in_bytes`) below the machine's memory.
    pub container: bool,
}

/// The memory one fetch of a table may hold, why, and what it was worked out from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryBudget {
    pub bytes: usize,
    pub reason: BudgetReason,
    /// The machine's memory limit; `None` when it can't be read (Windows).
    pub limit: Option<MemoryLimit>,
    /// The most tables loading at once.
    pub concurrency: usize,
}

/// Why a fetch's budget is what it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetReason {
    /// The source's `max_batch_memory`.
    Set,
    /// 512 MiB, which half the limit shared between the tables loading at once doesn't lower.
    Default,
    /// Half the limit, shared between the tables loading at once, below 512 MiB.
    Capped,
}

/// How a table's rows would be fetched, as `plan` shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedFetch {
    /// The bytes one fetch may hold.
    pub budget: usize,
    /// The bytes one fetch holds at most: its bound buffers, or the budget for a table read value by value; `None` when the driver couldn't describe the columns.
    pub estimate: Option<usize>,
    /// `plan`'s memory line.
    pub memory: String,
    /// `plan`'s budget line: the budget and why.
    pub budget_note: String,
    /// The machine's memory limit the budget was worked out from, in bytes.
    pub limit: Option<u64>,
}

/// What one read selects: a table or a query, its columns, and the rows' filter.
#[derive(Debug, Clone, Copy)]
pub struct OdbcReadRequest<'a> {
    pub table: &'a SourceTable,
    /// The query whose rows are the table; `None` reads the table itself.
    pub sql: Option<&'a str>,
    pub columns: &'a [ReadColumn],
    /// A condition on the source's columns, already rendered.
    pub filter: Option<&'a str>,
    /// The values bound to `filter`'s `?` placeholders, in order, each as its type.
    pub parameters: &'a [BoundParameter],
    pub batch_rows: usize,
    /// The bytes one fetch may hold: its bound buffers, or the values it reads value by value; a fetch has at least one row.
    pub memory_budget: usize,
}
