use serde_json::Value;

use crate::stage_api::models::{FileLoad, LoadReport, LoadStatus};

/// Read an `insertReport` or `loadHistoryScan` response; `completeResult` is a boolean in one and text in the other.
pub(crate) fn parse_load_report(body: &Value) -> LoadReport {
    let files = body["files"]
        .as_array()
        .map(|files| files.iter().map(file_load).collect())
        .unwrap_or_default();
    let complete = match &body["completeResult"] {
        Value::Bool(complete) => *complete,
        Value::String(text) => !text.eq_ignore_ascii_case("false"),
        _ => true,
    };
    LoadReport {
        files,
        next_begin_mark: text(&body["nextBeginMark"]),
        complete,
        range_end: text(&body["rangeEndTime"]),
    }
}

fn file_load(file: &Value) -> FileLoad {
    let status = match file["status"].as_str().unwrap_or_default() {
        "LOADED" => LoadStatus::Loaded,
        "LOAD_FAILED" => LoadStatus::Failed,
        "PARTIALLY_LOADED" => LoadStatus::PartiallyLoaded,
        _ => LoadStatus::InProgress,
    };
    FileLoad {
        path: file["path"].as_str().unwrap_or_default().to_owned(),
        status,
        rows_inserted: file["rowsInserted"].as_u64().unwrap_or_default(),
        rows_parsed: file["rowsParsed"].as_u64().unwrap_or_default(),
        first_error: text(&file["firstError"]).or_else(|| text(&file["systemError"])),
    }
}

fn text(value: &Value) -> Option<String> {
    value
        .as_str()
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}
