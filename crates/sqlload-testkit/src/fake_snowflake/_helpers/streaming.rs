use std::io::Read;

use axum::http::StatusCode;
use flate2::read::GzDecoder;
use serde_json::{Value, json};

use crate::fake_snowflake::models::{PendingAppend, SharedState};

type Outcome = Result<Value, (StatusCode, Value)>;

fn segment(path: &str, after: &str) -> String {
    let parts: Vec<&str> = path.split('/').collect();
    let index = parts
        .iter()
        .position(|part| *part == after)
        .map_or(parts.len(), |found| found + 1);
    parts
        .get(index)
        .map(|part| (*part).to_owned())
        .unwrap_or_default()
}

fn channel_key(path: &str) -> String {
    format!(
        "{}/{}",
        segment(path, "pipes"),
        segment(path, "channels").to_ascii_uppercase()
    )
}

fn table_of(pipe: &str) -> String {
    pipe.trim_end_matches("-STREAMING").to_owned()
}

fn query_value(query: &str, name: &str) -> String {
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| *key == name)
        .map(|(_, value)| value.to_owned())
        .unwrap_or_default()
}

fn parse_line(line: &str) -> Option<Value> {
    match serde_json::from_str(line) {
        Ok(value) => Some(value),
        Err(_) => None,
    }
}

/// The ingest host, which is the fake itself.
pub(crate) fn host(state: &SharedState) -> Outcome {
    Ok(Value::String(state.lock().host.clone()))
}

/// Open or reopen a channel, discarding uncommitted rows like the real service.
pub(crate) fn open(state: &SharedState, path: &str) -> Outcome {
    let mut guard = state.lock();
    let channel = guard.channels.entry(channel_key(path)).or_default();
    channel.uncommitted.clear();
    channel.continuation += 1;
    Ok(json!({
        "next_continuation_token": channel.continuation.to_string(),
        "channel_status": { "channel_status_code": "SUCCESS", "last_committed_offset_token": channel.committed },
    }))
}

/// Accept one gzip NDJSON body after the expected continuation token.
pub(crate) fn append(state: &SharedState, path: &str, query: &str, body: &[u8]) -> Outcome {
    let mut guard = state.lock();
    if let Some(status) = guard.append_failures.pop_front() {
        let code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        return Err((
            code,
            json!({ "code": "INJECTED", "message": "injected failure" }),
        ));
    }
    let mut text = String::new();
    GzDecoder::new(body)
        .read_to_string(&mut text)
        .map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                json!({ "code": "BAD_GZIP", "message": error.to_string() }),
            )
        })?;
    let channel = guard.channels.get_mut(&channel_key(path)).ok_or((
        StatusCode::NOT_FOUND,
        json!({ "code": "ERR_CHANNEL_DOES_NOT_EXIST" }),
    ))?;
    if query_value(query, "continuationToken") != channel.continuation.to_string() {
        return Err((
            StatusCode::BAD_REQUEST,
            json!({ "code": "STALE_CONTINUATION_TOKEN_SEQUENCER" }),
        ));
    }
    let rows: Vec<Value> = text.lines().filter_map(parse_line).collect();
    channel.error_rows += (text.lines().count() - rows.len()) as i64;
    channel.uncommitted.push(PendingAppend {
        offset_token: query_value(query, "offsetToken"),
        rows,
    });
    channel.continuation += 1;
    let next = channel.continuation.to_string();
    guard.appends += 1;
    Ok(json!({ "next_continuation_token": next }))
}

fn channel_names(body: &[u8]) -> Vec<String> {
    let request: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(_) => Value::Null,
    };
    let names = request["channel_names"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    names
        .iter()
        .filter_map(Value::as_str)
        .map(str::to_ascii_uppercase)
        .collect()
}

fn commit(appended: Vec<PendingAppend>) -> (Option<String>, Vec<Value>) {
    let committed = appended.last().map(|last| last.offset_token.clone());
    let rows: Vec<Value> = appended
        .into_iter()
        .flat_map(|pending| pending.rows)
        .collect();
    (committed, rows)
}

/// Commit everything appended to the requested channels, then report their status.
pub(crate) fn bulk_status(state: &SharedState, path: &str, body: &[u8]) -> Outcome {
    let pipe = segment(path, "pipes")
        .trim_end_matches(":bulk-channel-status")
        .to_owned();
    let mut guard = state.lock();
    let mut statuses = serde_json::Map::new();
    for name in channel_names(body) {
        let channel = guard.channels.entry(format!("{pipe}/{name}")).or_default();
        let (committed, committed_rows) = commit(std::mem::take(&mut channel.uncommitted));
        if committed.is_some() {
            channel.committed = committed;
        }
        let status = json!({
            "channel_status_code": "SUCCESS",
            "last_committed_offset_token": channel.committed,
            "rows_error_count": channel.error_rows,
        });
        guard
            .tables
            .entry(table_of(&pipe))
            .or_default()
            .rows
            .extend(committed_rows);
        statuses.insert(name, status);
    }
    Ok(json!({ "channel_statuses": statuses }))
}
