use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::fake_snowflake::models::{FakePipe, LoadOutcome, SharedState, StalledFile};

type Outcome = Result<Value, (StatusCode, Value)>;

const EXPIRED: &str =
    "<Error><Code>ExpiredToken</Code><Message>The provided token has expired.</Message></Error>";

/// Delete one path-style object, `/bucket/key`; a missing one is no error, as in S3.
pub(crate) fn delete_object(state: &SharedState, path: &str) -> Outcome {
    let key = percent_decode(path.trim_start_matches('/'));
    state.lock().objects.remove(&key);
    Ok(Value::String(String::new()))
}

/// Store one path-style upload, `/bucket/key`, unless a failure is queued.
pub(crate) fn put_object(state: &SharedState, path: &str, body: &[u8]) -> Outcome {
    let key = percent_decode(path.trim_start_matches('/'));
    let mut guard = state.lock();
    guard.puts.push(key.clone());
    if let Some(status) = guard.put_failures.pop_front() {
        let code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let text = match status {
            400 => EXPIRED.to_owned(),
            _ => "<Error><Code>InternalError</Code></Error>".to_owned(),
        };
        return Err((code, Value::String(text)));
    }
    guard.objects.insert(key, body.to_vec());
    Ok(Value::String(String::new()))
}

/// `HEAD /bucket/key`: whether the object exists.
pub(crate) fn head_object(state: &SharedState, path: &str) -> Outcome {
    let key = percent_decode(path.trim_start_matches('/'));
    match state.lock().objects.contains_key(&key) {
        true => Ok(Value::String(String::new())),
        false => Err((StatusCode::NOT_FOUND, Value::String(String::new()))),
    }
}

/// `/v1/data/pipes/DB.SCHEMA.PIPE/insertFiles`: load each path under the pipe's location and record a load event; paths the pipe received before are skipped.
pub(crate) fn insert_files(state: &SharedState, path: &str, query: &str, body: &[u8]) -> Outcome {
    let pipe = pipe_name(path, "/insertFiles");
    let request: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(error) => {
            return Err((
                StatusCode::BAD_REQUEST,
                json!({ "message": error.to_string() }),
            ));
        }
    };
    let mut guard = state.lock();
    let Some(found) = guard.pipes.get(&pipe).cloned() else {
        return Err((
            StatusCode::NOT_FOUND,
            json!({ "code": "390404", "message": format!("pipe {pipe} not found") }),
        ));
    };
    let files = request["files"].as_array().cloned().unwrap_or_default();
    let mut skipped: Vec<String> = Vec::new();
    for file in files {
        let relative = file["path"].as_str().unwrap_or_default().to_owned();
        guard.inserted.push(relative.clone());
        if !guard.received.insert((pipe.clone(), relative.clone())) {
            skipped.push(relative);
            continue;
        }
        let key = format!("{}{relative}", found.location);
        let outcome = guard.load_script.pop_front().unwrap_or(LoadOutcome::Load);
        match outcome {
            LoadOutcome::Load => guard.load_file(&pipe, &found.table, &relative, &key),
            LoadOutcome::Fail(error) => {
                let event = load_event(&relative, "LOAD_FAILED", 0, Some(error));
                guard
                    .load_events
                    .entry(pipe.clone())
                    .or_default()
                    .push(event);
            }
            LoadOutcome::Stall => guard.stalled.push(StalledFile {
                pipe: pipe.clone(),
                table: found.table.clone(),
                path: relative,
                key,
            }),
            LoadOutcome::Lose => {
                guard.objects.remove(&key);
            }
        }
    }
    let mut response = json!({ "responseCode": "SUCCESS" });
    if query.contains("showSkippedFiles=true") {
        response["skippedFiles"] = json!(skipped);
    }
    Ok(response)
}

/// `/v1/data/pipes/DB.SCHEMA.PIPE/insertReport`: load events after `beginMark`, none while reports are withheld.
pub(crate) fn insert_report(state: &SharedState, path: &str, query: &str) -> Outcome {
    let pipe = pipe_name(path, "/insertReport");
    let guard = state.lock();
    let events = guard.load_events.get(&pipe).cloned().unwrap_or_default();
    let mark: usize = query
        .split('&')
        .filter_map(|pair| pair.strip_prefix("beginMark="))
        .find_map(parse_mark)
        .unwrap_or(0)
        .min(events.len());
    let (files, next): (Vec<Value>, usize) = match guard.withhold_reports {
        true => (Vec::new(), mark),
        false => (events[mark..].to_vec(), events.len()),
    };
    Ok(json!({
        "pipe": pipe,
        "completeResult": true,
        "nextBeginMark": next.to_string(),
        "files": files,
    }))
}

/// `/v1/data/pipes/DB.SCHEMA.PIPE/loadHistoryScan`: every load event, none while history is withheld, or a queued failure.
pub(crate) fn load_history_scan(state: &SharedState, path: &str) -> Outcome {
    let pipe = pipe_name(path, "/loadHistoryScan");
    let mut guard = state.lock();
    guard.history_scans += 1;
    if let Some(status) = guard.history_failures.pop_front() {
        let code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        return Err((
            code,
            json!({ "code": "FAKE", "message": "history scan failed" }),
        ));
    }
    let files = match guard.withhold_history {
        true => Vec::new(),
        false => guard.load_events.get(&pipe).cloned().unwrap_or_default(),
    };
    Ok(json!({ "pipe": pipe, "completeResult": "true", "files": files }))
}

fn parse_mark(mark: &str) -> Option<usize> {
    match mark.parse() {
        Ok(mark) => Some(mark),
        Err(_) => None,
    }
}

/// One `insertReport` file entry.
pub(crate) fn load_event(path: &str, status: &str, rows: usize, error: Option<&str>) -> Value {
    let mut event = json!({
        "path": path,
        "status": status,
        "rowsInserted": rows,
        "rowsParsed": rows,
        "errorsSeen": usize::from(error.is_some()),
        "complete": error.is_none(),
    });
    if let Some(error) = error {
        event["firstError"] = json!(error);
    }
    event
}

/// The unqualified pipe name of a `/v1/data/pipes/DB.SCHEMA.PIPE/<endpoint>` path.
fn pipe_name(path: &str, endpoint: &str) -> String {
    path.trim_end_matches(endpoint)
        .rsplit('/')
        .next()
        .and_then(|name| name.rsplit('.').next())
        .unwrap_or_default()
        .trim_matches('"')
        .to_owned()
}

/// `CREATE PIPE IF NOT EXISTS P AS COPY INTO T ... FROM @STAGE/location ...` → the pipe; the source may sit inside a Parquet `SELECT`.
pub(crate) fn parse_stage_pipe(statement: &str) -> Option<(String, FakePipe)> {
    let rest = statement.strip_prefix("CREATE PIPE IF NOT EXISTS ")?;
    let (pipe, rest) = rest.split_once(' ')?;
    let (_, copy) = rest.split_once("COPY INTO ")?;
    let (table, location) = copy_target(copy)?;
    Some((
        last_name(pipe),
        FakePipe {
            table: last_name(table),
            location: stage_relative(location),
        },
    ))
}

/// `COPY INTO T ... FROM @STAGE/folder/ ... FILES = ('a', 'b') ...` → the table and the object keys to load.
pub(crate) fn copy_files(statement: &str) -> Option<(String, Vec<String>)> {
    let copy = statement.strip_prefix("COPY INTO ")?;
    let (table, location) = copy_target(copy)?;
    let (_, rest) = copy.split_once(" FILES = (")?;
    let (names, _) = rest.split_once(')')?;
    let folder = stage_relative(location);
    let keys: Vec<String> = names
        .split(", ")
        .map(|name| format!("{folder}{}", name.trim_matches('\'')))
        .collect();
    Some((last_name(table), keys))
}

/// The table and the `STAGE/path/` after the last `FROM @` of a COPY body.
fn copy_target(copy: &str) -> Option<(&str, &str)> {
    let table = copy.split_whitespace().next()?;
    let (_, source) = copy.rsplit_once(" FROM @")?;
    let end = source
        .find(|c: char| c.is_whitespace() || c == ')')
        .unwrap_or(source.len());
    Some((table, &source[..end]))
}

/// The bucket-relative part of `STAGE/path/`, with the fake's single bucket in front.
fn stage_relative(location: &str) -> String {
    let path = location.split_once('/').map_or("", |(_, path)| path);
    format!("{}/{path}", crate::fake_snowflake::constants::FAKE_BUCKET)
}

fn last_name(qualified: &str) -> String {
    qualified
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .trim_matches('"')
        .to_owned()
}

/// Undo SigV4 URI encoding, as S3 does for object keys.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut decoded: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let escaped = match bytes.get(index..index + 3) {
            Some([b'%', high, low]) => hex_digit(*high)
                .zip(hex_digit(*low))
                .map(|(high, low)| high * 16 + low),
            _ => None,
        };
        match escaped {
            Some(byte) => {
                decoded.push(byte);
                index += 3;
            }
            None => {
                decoded.push(bytes[index]);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
