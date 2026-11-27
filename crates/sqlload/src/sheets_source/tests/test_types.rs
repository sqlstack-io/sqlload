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

pub(crate) struct SheetInputTestCase {
    pub(crate) description: &'static str,
    pub(crate) sheet: serde_json::Value,
    pub(crate) rule: HeaderRule,
    /// Each column as `name:type` joined by `,`, and the rows, each as its values joined by `|`, rows joined by `;`; or the error.
    pub(crate) expected_result: Result<(&'static str, &'static str), &'static str>,
}

pub(crate) struct KeyTestCase {
    pub(crate) description: &'static str,
    pub(crate) source: crate::sheets_source::models::KeySource,
    /// The account the client reads as, or why the key does not read.
    pub(crate) expected_result: Result<&'static str, &'static str>,
}

pub(crate) struct A1RangeTestCase {
    pub(crate) description: &'static str,
    pub(crate) range: &'static str,
    /// The sheet, columns and rows read, or why the range is refused.
    pub(crate) expected_range: Result<&'static str, &'static str>,
}

/// A credentials file `GOOGLE_APPLICATION_CREDENTIALS` names, for a google_sheets connection.
pub(crate) struct SheetsCredentialsTestCase {
    pub(crate) description: &'static str,
    pub(crate) file: serde_json::Value,
    /// Text the refusal contains; empty when the credentials can read Sheets.
    pub(crate) expected_error: &'static str,
}
