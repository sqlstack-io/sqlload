//! Helpers for the Google Sheets loader tests: cells as the Sheets API writes them, and the fake spreadsheet's tabs.

use serde_json::{Value, json};
use sqlload_testkit::fake_sheets::models::FakeTab;

/// A text cell.
pub(crate) fn text(value: &str) -> Value {
    json!({"effectiveValue": {"stringValue": value}, "formattedValue": value})
}

/// A number cell shown as `shown`, with a number format of `format` (`""` for none).
pub(crate) fn number(value: f64, shown: &str, format: &str) -> Value {
    json!({"effectiveValue": {"numberValue": value}, "formattedValue": shown, "effectiveFormat": {"numberFormat": {"type": format}}})
}

/// A TRUE/FALSE cell.
pub(crate) fn flag(value: bool) -> Value {
    json!({"effectiveValue": {"boolValue": value}, "formattedValue": if value { "TRUE" } else { "FALSE" }})
}

/// An empty cell.
pub(crate) fn blank() -> Value {
    json!({})
}

/// A tab.
pub(crate) fn tab(title: &str, rows: Vec<Vec<Value>>) -> FakeTab {
    FakeTab {
        title: title.to_owned(),
        rows,
    }
}

/// The spreadsheet's first state: typed locations (a ragged row, a blank row), a report with title rows above its header, a header-only tab and an empty one.
pub(crate) fn first_tabs() -> Vec<FakeTab> {
    vec![
        tab(
            "Locations",
            vec![
                vec![
                    text("Code"),
                    text("Name"),
                    text("Opened"),
                    text("Active"),
                    text("Capacity"),
                    text("Updated At"),
                    text("Ref"),
                ],
                vec![
                    text("L1"),
                    text("North"),
                    number(46037.0, "2026-01-15", "DATE"),
                    flag(true),
                    number(120.0, "120", ""),
                    number(46334.395833333336, "2026-11-08 09:30:00", "DATE_TIME"),
                    text("00123"),
                ],
                vec![blank()],
                vec![
                    text("L2"),
                    text("South, Upper"),
                    number(46022.0, "2025-12-31", "DATE"),
                    flag(false),
                ],
            ],
        ),
        tab(
            "Report",
            vec![
                vec![text("A generic report")],
                vec![text("Generated for tests")],
                vec![text("Item"), text("Qty"), text("Note")],
                vec![text("alpha"), number(1.0, "1", ""), text("first")],
                vec![text("beta"), number(2.5, "2.5", "")],
            ],
        ),
        tab("Header Only", vec![vec![text("id"), text("name")]]),
        tab("Empty", Vec::new()),
    ]
}

/// The spreadsheet with a tab of codes whose key repeats (`a`, rows 2 and 4).
pub(crate) fn duplicated_tabs() -> Vec<FakeTab> {
    let mut tabs = first_tabs();
    tabs.push(tab(
        "Codes",
        vec![
            vec![text("code"), text("label")],
            vec![text("a"), text("first")],
            vec![text("b"), text("second")],
            vec![text("a"), text("again")],
        ],
    ));
    tabs
}

/// The spreadsheet after values change type: the report's quantities all whole now, rows typed into the header-only tab, and a date in the timestamp column.
pub(crate) fn retyped_tabs() -> Vec<FakeTab> {
    let mut tabs = edited_tabs();
    tabs[1].rows.truncate(3);
    tabs[1].rows.push(vec![text("alpha"), number(3.0, "3", "")]);
    tabs[1].rows.push(vec![text("beta"), number(4.0, "4", "")]);
    tabs[2].rows.push(vec![number(7.0, "7", ""), text("seven")]);
    tabs[0].rows[1].push(number(46037.0, "2026-01-15", "DATE"));
    tabs
}

/// The spreadsheet after an edit: a location renamed, one removed and one added.
pub(crate) fn edited_tabs() -> Vec<FakeTab> {
    let mut tabs = first_tabs();
    tabs[0].rows = vec![
        tabs[0].rows[0].clone(),
        vec![
            text("L1"),
            text("North Gate"),
            number(46037.0, "2026-01-15", "DATE"),
            flag(true),
            number(150.0, "150", ""),
        ],
        vec![
            text("L3"),
            text("East"),
            number(46100.0, "2026-03-19", "DATE"),
            flag(true),
            number(90.0, "90", ""),
        ],
    ];
    tabs
}

/// The test project: a DuckDB target, and sheets sources reading the fake through its key.
pub(crate) fn project_text(key_json: &str, base_url: &str, database: &std::path::Path) -> String {
    format!(
        r#"
[connections.sheets]
kind = "google_sheets"
service_account_json = '''{key}'''
base_url = "{base_url}"

[connections.local]
kind = "duckdb"
database = "{database}"

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.reference]
kind = "google_sheets"
description = "Reference tables kept in a spreadsheet."
connection = "sheets"
spreadsheet_id = "fake-sheet"

[sources.report]
kind = "google_sheets"
description = "A report below its title rows."
connection = "sheets"
spreadsheet_id = "fake-sheet"
ranges = {{ items = "'Report'!A2:B" }}
header_row = 2
"#,
        key = key_json,
        base_url = base_url,
        database = database.display(),
    )
}

/// A project reading the fake spreadsheet's `Locations` tab as a Google user of an installed app's OAuth client, with the connection's `refresh` token if any, into DuckDB at `database`.
pub(crate) fn user_project_text(
    (base_url, token_url): (&str, &str),
    refresh: Option<&str>,
    database: &std::path::Path,
) -> String {
    let refresh = refresh.map_or_else(String::new, |token| format!("refresh_token = '{token}'\n"));
    format!(
        r#"
[connections.sheets]
kind = "google_sheets"
client_id = "client.apps.googleusercontent.com"
client_secret = "secret"
{refresh}base_url = '{base_url}'
token_url = '{token_url}'

[connections.local]
kind = "duckdb"
database = '{database}'

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.reference]
kind = "google_sheets"
description = "Reference tables kept in a spreadsheet."
connection = "sheets"
spreadsheet_id = "fake-sheet"
ranges = {{ locations = "Locations" }}
"#,
        database = database.display()
    )
}

/// Run `sqlload` with `args` on `config`, its sign-in cache below `cache`: the exit code and the output.
pub(crate) fn run_with_cache(
    config: &std::path::Path,
    args: &[&str],
    cache: &std::path::Path,
) -> Result<(Option<i32>, String), String> {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sqlload"))
        .arg("--config")
        .arg(config)
        .arg("--no-color")
        .args(args)
        .env("XDG_CACHE_HOME", cache)
        .env("LOCALAPPDATA", cache)
        .output()
        .map_err(|error| error.to_string())?;
    Ok((
        output.status.code(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    ))
}
