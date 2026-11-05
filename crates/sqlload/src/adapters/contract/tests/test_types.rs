use crate::column_types::models::Warehouse;
use std::time::Duration;

use crate::column_types::models::ColumnType;
use crate::adapters::contract::models::{CursorValue, WriteMode};

pub(crate) struct WriteStatementsTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) mode: WriteMode,
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

pub(crate) struct CommitStatementsTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) mode: WriteMode,
    pub(crate) run_insert: Option<&'static str>,
    /// The staging table is swapped in or rebuilt from at the end, so it is not emptied.
    pub(crate) keep_staging: bool,
    pub(crate) expected_statements: &'static [&'static str],
}

pub(crate) struct StagingChangesTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    /// The new columns' types: `id`, then `score`, then (when given) `note`.
    pub(crate) new_types: &'static [ColumnType],
    pub(crate) expected_statements: &'static [&'static str],
}

pub(crate) struct StaleStagingTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) found: &'static [&'static str],
    pub(crate) expected_statements: &'static [&'static str],
}

pub(crate) struct StagingIdTestCase {
    pub(crate) description: &'static str,
    pub(crate) load_id: &'static str,
    pub(crate) expected_name: &'static str,
}

pub(crate) struct InsertRowsTestCase {
    pub(crate) description: &'static str,
    pub(crate) rows: usize,
    pub(crate) value_bytes: usize,
    pub(crate) expected_statements: usize,
    /// The start of the first statement.
    pub(crate) expected_start: &'static str,
}

pub(crate) struct BuildTableTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) live: Option<Vec<crate::schema_plan::models::LiveColumn>>,
    pub(crate) definition: Option<&'static str>,
    pub(crate) expected_statements: Vec<String>,
}

pub(crate) struct PromoteTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) expected_statements: Vec<String>,
}

pub(crate) struct KeptColumnsTestCase {
    pub(crate) description: &'static str,
    pub(crate) new: Vec<crate::schema_plan::models::DesiredColumn>,
    pub(crate) expected_statements: Vec<String>,
}
