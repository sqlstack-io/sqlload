use crate::column_types::models::Warehouse;
use std::time::Duration;

use crate::adapters::contract::models::{
    BoundParameter, CheckQuery, CursorValue, NowShift, WriteMode,
};
use crate::column_types::models::{ColumnType, LiveColumnType, TypeFit};

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
    /// The table's `timezone`, which text without an offset on a `timestamp_tz` cursor is read in.
    pub(crate) zone: chrono_tz::Tz,
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

pub(crate) struct TombstonesTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) expected_statements: [String; 2],
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

pub(crate) struct WarehouseTypeTestCase {
    pub(crate) description: &'static str,
    pub(crate) column_type: ColumnType,
    pub(crate) warehouse: Warehouse,
    pub(crate) expected_sql: &'static str,
    pub(crate) expected_lossy: bool,
}

pub(crate) struct ArrayLossTestCase {
    pub(crate) description: &'static str,
    pub(crate) element: ColumnType,
    pub(crate) warehouse: Warehouse,
    /// The array's note; `None` when it loses nothing.
    pub(crate) expected_loss: Option<&'static str>,
}

pub(crate) struct LiveTypeTestCase {
    pub(crate) description: &'static str,
    pub(crate) live: LiveColumnType,
    pub(crate) warehouse: Warehouse,
    pub(crate) expected_type: ColumnType,
}

pub(crate) struct TypeFitTestCase {
    pub(crate) description: &'static str,
    pub(crate) desired: ColumnType,
    pub(crate) live: ColumnType,
    pub(crate) warehouse: Warehouse,
    pub(crate) expected_fit: TypeFit,
}

pub(crate) struct ContractTestCase {
    pub(crate) description: &'static str,
    pub(crate) mode: WriteMode,
    /// The staging table is a build table, swapped in at the end.
    pub(crate) built: bool,
    /// How many statements copy rows out of the staging table.
    pub(crate) expected_copies: usize,
}

pub(crate) struct CheckQueryTestCase<'a> {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) check: CheckQuery<'a>,
    pub(crate) expected_query: &'static str,
}

pub(crate) struct CursorFromNowTestCase {
    pub(crate) description: &'static str,
    pub(crate) column_type: ColumnType,
    pub(crate) shift: NowShift,
    /// The cursor text, or text the error contains.
    pub(crate) expected_text: Result<&'static str, &'static str>,
}

pub(crate) struct BoundParameterTestCase {
    pub(crate) description: &'static str,
    pub(crate) value: &'static str,
    pub(crate) column_type: ColumnType,
    pub(crate) expected_parameter: Option<BoundParameter>,
}

pub(crate) struct MaxCursorTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    /// The table's constant columns: name, type and value as text.
    pub(crate) scope: Vec<(String, ColumnType, String)>,
    /// The further columns of a coalesced cursor.
    pub(crate) coalesce: Vec<String>,
    pub(crate) expected_query: &'static str,
}

/// A merge or change apply on keys that may be NULL, and how the warehouse's statement matches them.
pub(crate) struct KeyMatchTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    pub(crate) mode: WriteMode,
    /// The text around the condition the write matches rows on, such as a `MERGE`'s ` AS __source ON ` and ` WHEN `.
    pub(crate) markers: (&'static str, &'static str),
    /// The condition the write matches rows on, between `markers`.
    pub(crate) expected_merge: &'static str,
    /// The condition the query of rekeyed row ids joins the table and the staged rows on.
    pub(crate) expected_rekeyed: &'static str,
}

pub(crate) struct NewestByTestCase {
    pub(crate) description: &'static str,
    pub(crate) warehouse: Warehouse,
    /// The `newest_by` column's type, as the table writes it.
    pub(crate) column_type: ColumnType,
    /// The plan's problem with it, or empty when it orders rows.
    pub(crate) expected_problem: &'static str,
}
