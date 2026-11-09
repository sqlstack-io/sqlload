use arrow_array::ArrayRef;

use crate::column_types::models::ColumnType;

pub(crate) struct SourceColumnTestCase {
    pub(crate) description: &'static str,
    /// The schema field as BigQuery's REST API gives it, as JSON text.
    pub(crate) field: &'static str,
    /// The portable type; `None` when a type must be pinned.
    pub(crate) expected_type: Option<ColumnType>,
    pub(crate) expected_shown: &'static str,
    pub(crate) expected_nullable: bool,
}

pub(crate) struct ConvertBatchTestCase {
    pub(crate) description: &'static str,
    /// The column as the Storage Read API sends it.
    pub(crate) read: fn() -> ArrayRef,
    pub(crate) output: ColumnType,
    /// The converted value as text; `None` when the conversion fails.
    pub(crate) expected_value: Option<&'static str>,
}

pub(crate) struct ReadColumnsTestCase {
    pub(crate) description: &'static str,
    pub(crate) pinned: Vec<(&'static str, ColumnType)>,
    /// Each read column's expression and type, or the problems.
    pub(crate) expected_columns: Result<Vec<(&'static str, ColumnType)>, Vec<&'static str>>,
}

pub(crate) struct SelectQueryTestCase {
    pub(crate) description: &'static str,
    /// The query table's `sql`.
    pub(crate) sql: &'static str,
    pub(crate) expected_query: &'static str,
}
