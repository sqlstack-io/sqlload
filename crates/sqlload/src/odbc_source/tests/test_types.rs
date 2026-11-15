use crate::odbc_source::models::{BudgetReason, FetchShape, FetchSize, MemoryLimit};
use crate::postgres_source::models::SourceTable;

pub(crate) struct SelectQueryTestCase {
    pub(crate) description: &'static str,
    pub(crate) table: SourceTable,
    pub(crate) sql: Option<&'static str>,
    pub(crate) filter: Option<&'static str>,
    pub(crate) expected_query: &'static str,
}

pub(crate) struct ConvertedDecimalTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    /// The value loaded as decimal(28,6), in millionths; `None` when the read must fail.
    pub(crate) expected_value: Option<i128>,
}

pub(crate) struct OdbcReaderTestCase {
    pub(crate) description: &'static str,
    pub(crate) connection_string: Option<&'static str>,
    pub(crate) dsn: Option<&'static str>,
    pub(crate) user: Option<&'static str>,
    pub(crate) password: Option<&'static str>,
    pub(crate) expected_connection_string: &'static str,
}

pub(crate) struct TimestampTextTestCase {
    pub(crate) description: &'static str,
    pub(crate) time: &'static str,
    pub(crate) digits: u16,
    pub(crate) expected_text: &'static str,
}

pub(crate) struct ConnectErrorTestCase {
    pub(crate) description: &'static str,
    /// The file written in the test's folder, by its path there, and what it holds.
    pub(crate) file: (&'static str, Vec<u8>),
    /// The path the connection string registers as the driver library, after the folder and a `/`.
    pub(crate) library: &'static str,
    /// What the error adds under unixODBC's, `{path}` standing for the library's path and `{folder}` for the folder's.
    pub(crate) expected_reason: &'static str,
}

pub(crate) struct FetchBudgetTestCase {
    pub(crate) description: &'static str,
    /// The source's `max_batch_memory`.
    pub(crate) set: Option<usize>,
    pub(crate) limit: Option<MemoryLimit>,
    pub(crate) concurrency: usize,
    pub(crate) expected_bytes: usize,
    pub(crate) expected_reason: BudgetReason,
}

pub(crate) struct FetchSizeTestCase {
    pub(crate) description: &'static str,
    pub(crate) shape: FetchShape,
    pub(crate) budget: usize,
    pub(crate) expected_size: FetchSize,
}

pub(crate) struct CgroupLimitTestCase {
    pub(crate) description: &'static str,
    /// `/proc/self/cgroup` as read.
    pub(crate) groups: &'static str,
    /// Each folder under the cgroup root with its `memory.max`.
    pub(crate) limits: &'static [(&'static str, &'static str)],
    pub(crate) expected_limit: Option<u64>,
}

pub(crate) struct BudgetNoteTestCase {
    pub(crate) description: &'static str,
    pub(crate) set: Option<usize>,
    pub(crate) limit: Option<MemoryLimit>,
    /// Why the machine's memory couldn't be read.
    pub(crate) unreadable: Option<&'static str>,
    pub(crate) concurrency: usize,
    pub(crate) expected_note: &'static str,
}

pub(crate) struct MemoryLineTestCase {
    pub(crate) description: &'static str,
    pub(crate) shape: FetchShape,
    pub(crate) budget: usize,
    pub(crate) expected_line: &'static str,
}

pub(crate) struct MemoryWarningTestCase {
    pub(crate) description: &'static str,
    /// Each table's bytes per fetch and its source.
    pub(crate) fetches: &'static [(usize, &'static str)],
    pub(crate) limit: Option<u64>,
    pub(crate) concurrency: usize,
    pub(crate) expected_warning: Option<&'static str>,
}

pub(crate) struct SearchPatternsTestCase {
    pub(crate) description: &'static str,
    /// An `include` pattern.
    pub(crate) pattern: &'static str,
    /// The schema and table search patterns `SQLTables` is asked with; `None` for a pattern the whole catalog is listed for.
    pub(crate) expected_patterns: Option<(&'static str, &'static str)>,
}
