/// Where the fake hands out OAuth tokens.
pub const TOKEN_PATH: &str = "/token";
/// How every read the fake accepts is authorized; the token's own text follows.
pub const TOKEN_PREFIX: &str = "Bearer fake-sheets-token-";
/// The spreadsheets root under the fake's address, as a connection's `base_url` names it.
pub const SPREADSHEETS_PATH: &str = "/v4/spreadsheets";
/// The query parameter naming each range a read asks for.
pub const RANGES_PARAMETER: &str = "ranges";
/// The title every fake spreadsheet has.
pub const FAKE_TITLE: &str = "Fake spreadsheet";
