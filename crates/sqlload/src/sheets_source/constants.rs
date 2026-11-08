/// The Sheets API's spreadsheets root.
pub const BASE_URL: &str = "https://sheets.googleapis.com/v4/spreadsheets";

/// The OAuth scope SQLLoad asks for: reading spreadsheets, nothing more.
pub const SCOPE: &str = "https://www.googleapis.com/auth/spreadsheets.readonly";

/// The parts of a spreadsheet read: each tab's title and type, and each cell's value, displayed text and number format type.
pub const GRID_FIELDS: &str = "properties.title,sheets(properties(title,sheetType),data(startRow,startColumn,rowData(values(effectiveValue,formattedValue,effectiveFormat/numberFormat/type))))";

/// The query parameter naming the range a read asks for.
pub const RANGES_PARAMETER: &str = "ranges";

/// The type of a tab of cells, as opposed to a chart.
pub const GRID_SHEET_TYPE: &str = "GRID";

/// The type of a service-account key file.
pub const SERVICE_ACCOUNT_TYPE: &str = "service_account";

/// Whole numbers below this print without a fraction or exponent.
pub const WHOLE_TEXT_LIMIT: f64 = 1e15;

/// The parts of a spreadsheet read to list its tabs: its title and each tab's title and type, without cells.
pub const TAB_FIELDS: &str = "properties.title,sheets.properties(title,sheetType)";

/// The first wait before a read is retried; doubling (up to 30 s) over eight attempts, the retries outlast Sheets' per-minute read quota.
pub const RETRY_FIRST_DELAY: std::time::Duration = std::time::Duration::from_secs(1);

/// The column type of a source column read from a sheet, as `plan` shows it.
pub const SHEET_COLUMN_KIND: &str = "google_sheets";

/// The same, for a column none of whose cells holds a value, which takes the live table's type.
pub const EMPTY_SHEET_COLUMN_KIND: &str = "google_sheets (no values)";

/// The days from Sheets' epoch (1899-12-30, serial 0) to 1970-01-01.
pub const UNIX_EPOCH_SERIAL: i64 = 25_569;

/// Milliseconds in a day; Sheets keeps dates and times to the millisecond.
pub const DAY_MS: f64 = 86_400_000.0;

/// The largest whole number a sheet holds exactly (2^53); a larger number loads as `float`.
pub const MAX_EXACT_INTEGER: f64 = 9_007_199_254_740_992.0;

/// The environment variable that names a key file when the connection gives no key.
pub const CREDENTIALS_VARIABLE: &str = "GOOGLE_APPLICATION_CREDENTIALS";

/// The HTTP status of a read of a spreadsheet the account may not see.
pub const FORBIDDEN: u16 = 403;

/// The HTTP status of a read of a spreadsheet the API does not find.
pub const NOT_FOUND: u16 = 404;

/// The HTTP status of a request whose token the server refused.
pub const UNAUTHORIZED: u16 = 401;
