//! The Files API over the fake volume.

use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::fake_databricks::models::SharedState;

pub(crate) type Outcome = Result<Value, (StatusCode, Value)>;

fn missing(path: &str) -> (StatusCode, Value) {
    (
        StatusCode::NOT_FOUND,
        json!({ "error_code": "NOT_FOUND", "message": format!("{path} does not exist") }),
    )
}

/// Write a file, replacing one already there.
pub(crate) fn put_file(state: &SharedState, path: &str, body: &[u8]) -> Outcome {
    let mut state = state.lock();
    state.puts.push(path.to_owned());
    state.write_file(path, body.to_vec(), now_ms());
    Ok(Value::String(String::new()))
}

/// Delete a file; a missing one is 404.
pub(crate) fn delete_file(state: &SharedState, path: &str) -> Outcome {
    match state.lock().files.remove(path) {
        Some(_) => Ok(Value::String(String::new())),
        None => Err(missing(path)),
    }
}

/// Delete an empty directory; a missing one is 404, one still holding entries is refused.
pub(crate) fn delete_directory(state: &SharedState, path: &str) -> Outcome {
    let path = path.trim_end_matches('/');
    let mut state = state.lock();
    if !state.directories.contains(path) {
        return Err(missing(path));
    }
    let prefix = format!("{path}/");
    let holds = state.files.keys().any(|file| file.starts_with(&prefix))
        || state.directories.iter().any(|dir| dir.starts_with(&prefix));
    if holds {
        return Err((
            StatusCode::CONFLICT,
            json!({ "error_code": "DIRECTORY_NOT_EMPTY", "message": format!("{path} is not empty") }),
        ));
    }
    state.directories.remove(path);
    Ok(Value::String(String::new()))
}

/// The files and directories directly in `path`.
pub(crate) fn list_directory(state: &SharedState, path: &str, page_token: Option<&str>) -> Outcome {
    let path = path.trim_end_matches('/');
    let mut state = state.lock();
    if !state.directories.contains(path) {
        return Err(missing(path));
    }
    let prefix = format!("{path}/");
    let direct = |entry: &String| {
        entry
            .strip_prefix(&prefix)
            .is_some_and(|rest| !rest.is_empty() && !rest.contains('/'))
    };
    let mut contents: Vec<Value> = state
        .directories
        .iter()
        .filter(|dir| direct(dir))
        .map(|dir| json!({ "path": format!("{dir}/"), "is_directory": true, "name": dir.rsplit('/').next() }))
        .collect();
    contents.extend(
        state
            .files
            .iter()
            .filter(|(file, _)| direct(file))
            .map(|(file, found)| {
                json!({
                    "path": file,
                    "is_directory": false,
                    "file_size": found.body.len(),
                    "last_modified": found.modified_ms,
                    "name": file.rsplit('/').next(),
                })
            }),
    );
    let start: usize = match page_token.map(str::parse) {
        Some(Ok(start)) => start,
        _ => 0,
    };
    let size = state
        .page_entries
        .filter(|size| *size > 0)
        .unwrap_or(usize::MAX);
    let end = start.saturating_add(size).min(contents.len());
    let page: Vec<Value> = contents
        .get(start..end)
        .map(<[Value]>::to_vec)
        .unwrap_or_default();
    let next = (end < contents.len()).then(|| end.to_string());
    if let Some((folder, file)) = state.write_after_listing.clone()
        && folder == path
    {
        state.write_after_listing = None;
        state.write_file(&file, b"PAR1".to_vec(), now_ms());
    }
    Ok(json!({ "contents": page, "next_page_token": next }))
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}
