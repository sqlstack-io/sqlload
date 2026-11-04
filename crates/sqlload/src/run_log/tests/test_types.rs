use crate::column_types::models::{ColumnType, Warehouse};

pub(crate) struct StatementTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    /// Builds the statements under test, joined by `;\n`.
    pub(crate) build: fn(Warehouse) -> Result<String, String>,
    pub(crate) expected_sql: &'static str,
}

pub(crate) struct CurrentRunTestCase {
    pub(crate) description: &'static str,
    /// Records as (run id, event, recorded at), in any order.
    pub(crate) records: &'static [(&'static str, &'static str, &'static str)],
    /// Further record texts, as read.
    pub(crate) raw: &'static [&'static str],
    /// The current record's run id, or the error text.
    pub(crate) expected_run: Result<Option<&'static str>, &'static str>,
}

pub(crate) struct LaterCursorTestCase {
    pub(crate) description: &'static str,
    pub(crate) left: Option<&'static str>,
    pub(crate) right: Option<&'static str>,
    pub(crate) column_type: ColumnType,
    pub(crate) expected_cursor: Option<&'static str>,
}

pub(crate) struct FinishedRecordTestCase {
    pub(crate) description: &'static str,
    pub(crate) before: Option<&'static str>,
    pub(crate) loaded: Option<&'static str>,
    pub(crate) cursor_type: Option<ColumnType>,
    /// The record's cursor after, then its object name.
    pub(crate) expected_record: (Option<&'static str>, &'static str),
}
