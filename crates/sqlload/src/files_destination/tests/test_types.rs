use std::time::Duration;

use crate::column_types::models::ColumnType;
use crate::files_destination::models::PartitionBy;
use crate::project::models::WriteStrategy;
use crate::store_paths::models::{Location, StoreKind};

pub(crate) struct TablePathTestCase {
    pub(crate) description: &'static str,
    pub(crate) template: &'static str,
    pub(crate) store: StoreKind,
    /// The table folder as it prints, or the error text.
    pub(crate) expected_folder: Result<&'static str, &'static str>,
    /// The partition folder pattern as it prints.
    pub(crate) expected_partition: Option<&'static str>,
}

pub(crate) struct GroupPartitionsTestCase {
    pub(crate) description: &'static str,
    pub(crate) keys: &'static [&'static str],
    /// The partition folder's prefix and how it takes the cursor; `None` for a table without partitions.
    pub(crate) partition: Option<(&'static str, PartitionBy)>,
    pub(crate) cursor_type: ColumnType,
    /// Each partition's value (empty without partitions) and its file count, oldest first.
    pub(crate) expected_partitions: &'static [(&'static str, usize)],
    /// Text the problems must contain, joined; empty when there are none.
    pub(crate) expected_problems: &'static str,
}

pub(crate) struct PartitionWindowTestCase {
    pub(crate) description: &'static str,
    pub(crate) by: PartitionBy,
    pub(crate) cursor_type: ColumnType,
    pub(crate) newest: Option<&'static str>,
    pub(crate) lookback: Option<Duration>,
    /// `--start-cursor-*` and `--end-cursor-*`, normalised.
    pub(crate) flags: (Option<&'static str>, Option<&'static str>),
    pub(crate) cursor_start: Option<&'static str>,
    pub(crate) reload: bool,
    /// The window's lower bound, or the error text.
    pub(crate) expected_from: Result<Option<&'static str>, &'static str>,
    pub(crate) expected_to: Option<&'static str>,
    /// Text the note must contain.
    pub(crate) expected_note: &'static str,
}

pub(crate) struct PartitionInWindowTestCase {
    pub(crate) description: &'static str,
    pub(crate) value: &'static str,
    pub(crate) by: PartitionBy,
    pub(crate) cursor_type: ColumnType,
    pub(crate) bounds: (Option<&'static str>, Option<&'static str>),
    pub(crate) expected_inside: Result<bool, &'static str>,
}

pub(crate) struct SplitBatchTestCase {
    pub(crate) description: &'static str,
    /// The cursor column's values: microseconds since 1970 (UTC), `None` for NULL.
    pub(crate) instants: &'static [Option<i64>],
    pub(crate) by: PartitionBy,
    /// Each partition value and its row count, or the error text.
    pub(crate) expected_parts: Result<&'static [(&'static str, usize)], &'static str>,
}

pub(crate) struct OutputProblemsTestCase {
    pub(crate) description: &'static str,
    pub(crate) strategy: WriteStrategy,
    pub(crate) cursor_type: Option<ColumnType>,
    pub(crate) partition: Option<PartitionBy>,
    /// Text the problems must contain, joined; empty when there are none.
    pub(crate) expected_problems: &'static str,
}

pub(crate) struct OverlappingFoldersTestCase {
    pub(crate) description: &'static str,
    /// Each table's name and folder; a warehouse table is added after them.
    pub(crate) tables: Vec<(&'static str, Location)>,
    /// Per table, text its problem must contain; empty when its folder is its own.
    pub(crate) expected_problems: &'static [&'static str],
}
