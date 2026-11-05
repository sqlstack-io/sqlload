use crate::column_types::models::Warehouse;
use std::time::Duration;

use crate::column_types::models::ColumnType;
use crate::write_sql::models::{CursorValue, WriteMode};

pub(crate) struct WriteStatementsTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) mode: WriteMode,
    /// Columns the write hashes in SQL.
    pub(crate) hashed: &'static [&'static str],
    pub(crate) expected_statements: &'static [&'static str],
}

pub(crate) struct CursorBoundTestCase {
    pub(crate) description: &'static str,
    pub(crate) raw: &'static str,
    pub(crate) value: CursorValue,
    pub(crate) lookback: Option<Duration>,
    pub(crate) expected_bound: Result<&'static str, &'static str>,
}

pub(crate) struct SqlLiteralTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) expected_literal: &'static str,
}

pub(crate) struct CursorTextTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) column_type: ColumnType,
    pub(crate) expected_text: Result<&'static str, &'static str>,
}
