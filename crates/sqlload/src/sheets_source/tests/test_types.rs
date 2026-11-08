use crate::column_types::models::ColumnType;
use crate::sheets_source::models::HeaderRule;

pub(crate) struct SheetTableTestCase {
    pub(crate) description: &'static str,
    /// The tab as the Sheets API answers for it.
    pub(crate) sheet: serde_json::Value,
    pub(crate) rule: HeaderRule,
    /// Each column as `name:type`, and the row count; or the problems, joined by `; `.
    pub(crate) expected: Result<(&'static str, usize), &'static str>,
    pub(crate) expected_warnings: &'static [&'static str],
}

pub(crate) struct SheetBatchTestCase {
    pub(crate) description: &'static str,
    pub(crate) sheet: serde_json::Value,
    pub(crate) rule: HeaderRule,
    pub(crate) columns: Vec<(&'static str, ColumnType)>,
    /// The rows, each as its values joined by `|`, rows joined by `;`; or the error.
    pub(crate) expected_result: Result<&'static str, &'static str>,
}

pub(crate) struct KeyTestCase {
    pub(crate) description: &'static str,
    pub(crate) source: crate::sheets_source::models::KeySource,
    /// The account the client reads as, or why the key does not read.
    pub(crate) expected_result: Result<&'static str, &'static str>,
}
