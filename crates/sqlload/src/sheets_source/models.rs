use std::sync::Arc;

use crate::auth::models::Secret;
use crate::bigquery_api::models::ServiceAccountKey;
use crate::column_types::models::ColumnType;

/// What the Sheets API needs: the shared HTTP pool, the API root, the service-account key and the token it last gave.
#[derive(Debug, Clone)]
pub struct SheetsClient {
    pub(crate) http: reqwest::Client,
    /// The spreadsheets root, with no trailing `/`.
    pub(crate) base_url: String,
    pub(crate) key: ServiceAccountKey,
    /// The current access token and when it expires, in milliseconds since the epoch.
    pub(crate) token: Arc<tokio::sync::Mutex<Option<(Secret, i64)>>>,
}

impl SheetsClient {
    /// The service account the client reads as.
    pub fn account(&self) -> &str {
        &self.key.client_email
    }
}

/// Where a connection's service-account key comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeySource {
    /// The key file's JSON text.
    Json(Secret),
    /// The key file's JSON text, base64-encoded.
    Base64(Secret),
    /// A key file.
    File(std::path::PathBuf),
    /// The key file `GOOGLE_APPLICATION_CREDENTIALS` names.
    Environment,
}

/// How a number cell is formatted, which decides the type it loads as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumberKind {
    /// A number in a plain or custom format.
    Plain,
    /// A currency, percentage or scientific number, which loads as `float` even when whole, so a column keeps one type as its values change.
    Fractional,
    Date,
    DateTime,
    /// A time of day (a fraction of a day).
    Time,
}

/// One cell's effective value.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Empty,
    Bool(bool),
    /// A number with its format, and the text the sheet shows for it.
    Number {
        value: f64,
        kind: NumberKind,
        shown: String,
    },
    Text(String),
    /// A formula error, as the sheet shows it (`#DIV/0!`).
    Error(String),
}

/// A tab's cells in one range: where the range starts (0-based) and its rows, each as long as its last non-empty cell.
#[derive(Debug, Clone, PartialEq)]
pub struct Grid {
    pub title: String,
    pub start_row: u32,
    pub start_column: u32,
    pub rows: Vec<Vec<Cell>>,
}

/// Where a range's header is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderRule {
    /// Whether the range has a header row; without one, columns are `column_1`, `column_2`, ….
    pub header: bool,
    /// The header's row within the range, counting from 1; the rows above it are skipped.
    pub header_row: u32,
}

impl Default for HeaderRule {
    fn default() -> Self {
        Self {
            header: true,
            header_row: 1,
        }
    }
}

/// What one table of a sheets source reads: its range (every tab when `None`, matched by name) and where its header is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SheetRead {
    pub range: Option<String>,
    pub rule: HeaderRule,
}

/// One column of a sheet's table: its name, its position in the range, and the type its cells suggest.
#[derive(Debug, Clone, PartialEq)]
pub struct SheetColumn {
    pub name: String,
    pub position: usize,
    /// Why the column cannot load, when a cell holds an error.
    pub column_type: Result<ColumnType, String>,
    /// Whether any cell below the header holds a value; a column without one is typed `text` until the live table says otherwise.
    pub has_values: bool,
}

/// A range read as a table: its columns, its row count, and warnings about how its cells were typed.
#[derive(Debug, Clone, PartialEq)]
pub struct SheetTable {
    pub columns: Vec<SheetColumn>,
    pub rows: usize,
    pub warnings: Vec<String>,
}
