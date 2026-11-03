use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::fake_snowflake::models::{FakePipe, SharedState};

type Outcome = Result<Value, (StatusCode, Value)>;

const EXPIRED: &str =
    "<Error><Code>ExpiredToken</Code><Message>The provided token has expired.</Message></Error>";

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

/// `/v1/data/pipes/DB.SCHEMA.PIPE/insertFiles`: load each path under the pipe's location.
pub(crate) fn insert_files(state: &SharedState, path: &str, body: &[u8]) -> Outcome {
    let pipe = path
        .trim_end_matches("/insertFiles")
        .rsplit('/')
        .next()
        .and_then(|name| name.rsplit('.').next())
        .unwrap_or_default()
        .trim_matches('"')
        .to_owned();
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
    for file in files {
        let relative = file["path"].as_str().unwrap_or_default();
        let key = format!("{}{relative}", found.location);
        guard.load_object(&found.table, &key);
    }
    Ok(json!({ "responseCode": "SUCCESS" }))
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
