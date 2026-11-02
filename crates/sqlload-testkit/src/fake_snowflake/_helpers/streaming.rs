use std::io::Read;

use axum::http::StatusCode;
use flate2::read::GzDecoder;
use serde_json::{Value, json};

use crate::fake_snowflake::models::FakeState;

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

/// Open or reopen a channel, discarding uncommitted rows like the real service.
pub(crate) fn open(state: &mut FakeState, path: &str) -> Outcome {
    let key = format!(
        "{}/{}",
        segment(path, "pipes"),
        segment(path, "channels").to_ascii_uppercase()
    );
    let channel = state.channels.entry(key).or_default();
    channel.uncommitted.clear();
    channel.continuation += 1;
    Ok(json!({
        "next_continuation_token": channel.continuation.to_string(),
        "channel_status": { "channel_status_code": "SUCCESS", "last_committed_offset_token": channel.committed },
    }))
}

/// Accept one gzip NDJSON body after the expected continuation token.
pub(crate) fn append(state: &mut FakeState, path: &str, query: &str, body: &[u8]) -> Outcome {
    if let Some(status) = state.append_failures.pop_front() {
        let code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        return Err((
            code,
            json!({ "code": "INJECTED", "message": "injected failure" }),
        ));
    }
    let key = format!(
        "{}/{}",
        segment(path, "pipes"),
        segment(path, "channels").to_ascii_uppercase()
    );
    let mut text = String::new();
    GzDecoder::new(body)
        .read_to_string(&mut text)
        .map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                json!({ "code": "BAD_GZIP", "message": error.to_string() }),
            )
        })?;
    let channel = state.channels.get_mut(&key).ok_or((
        StatusCode::NOT_FOUND,
        json!({ "code": "ERR_CHANNEL_DOES_NOT_EXIST" }),
    ))?;
    if query_value(query, "continuationToken") != channel.continuation.to_string() {
        return Err((
            StatusCode::BAD_REQUEST,
            json!({ "code": "STALE_CONTINUATION_TOKEN_SEQUENCER" }),
        ));
    }
    let rows: Vec<Value> = text
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect();
    channel.error_rows += (text.lines().count() - rows.len()) as i64;
    channel
        .uncommitted
        .push((query_value(query, "offsetToken"), rows));
    channel.continuation += 1;
    state.appends += 1;
    Ok(json!({ "next_continuation_token": channel.continuation.to_string() }))
}

/// Commit everything appended to the requested channels, then report their status.
pub(crate) fn bulk_status(state: &mut FakeState, path: &str, body: &[u8]) -> Outcome {
    let pipe = segment(path, "pipes")
        .trim_end_matches(":bulk-channel-status")
        .to_owned();
    let request: Value = serde_json::from_slice(body).unwrap_or_default();
    let names: Vec<String> = request["channel_names"]
        .as_array()
        .map(|names| {
            names
                .iter()
                .filter_map(|name| name.as_str())
                .map(str::to_ascii_uppercase)
                .collect()
        })
        .unwrap_or_default();
    let mut statuses = serde_json::Map::new();
    for name in names {
        let key = format!("{pipe}/{name}");
        let channel = state.channels.entry(key).or_default();
        let appended = std::mem::take(&mut channel.uncommitted);
        let committed_rows: Vec<Value> =
            appended.iter().flat_map(|(_, rows)| rows.clone()).collect();
        if let Some((token, _)) = appended.last() {
            channel.committed = Some(token.clone());
        }
        let status = json!({
            "channel_status_code": "SUCCESS",
            "last_committed_offset_token": channel.committed,
            "rows_error_count": channel.error_rows,
        });
        state
            .tables
            .entry(table_of(&pipe))
            .or_default()
            .rows
            .extend(committed_rows);
        statuses.insert(name, status);
    }
    Ok(json!({ "channel_statuses": statuses }))
}
