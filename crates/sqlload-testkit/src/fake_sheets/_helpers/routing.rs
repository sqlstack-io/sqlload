//! The fake's routes: tokens, and spreadsheets with their grid data.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use crate::fake_sheets::_helpers::ranges::{query_pairs, split_range};
use crate::fake_sheets::constants::{
    FAKE_TITLE, RANGES_PARAMETER, SPREADSHEETS_PATH, TOKEN_PATH, TOKEN_PREFIX,
};
use crate::fake_sheets::models::{FakeTab, SharedSheets};

/// A Google error body.
fn google_error(code: u16, status: &str, message: &str) -> Value {
    json!({"error": {"code": code, "message": message, "status": status}})
}

/// Route one request: a token request gets a token unless its refresh token is revoked; every read needs a bearer token the fake handed out and has not refused.
pub(crate) async fn handle(
    State(state): State<SharedSheets>,
    parts: Parts,
    body: Bytes,
) -> Response {
    let path = parts.uri.path().to_owned();
    if parts.method == axum::http::Method::POST && path == TOKEN_PATH {
        let form = query_pairs(&String::from_utf8_lossy(&body));
        let field = |name: &str| {
            form.iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
                .unwrap_or_default()
        };
        let mut locked = state.lock();
        locked.grants.push(field("grant_type"));
        if locked
            .revoked_refresh_tokens
            .contains(&field("refresh_token"))
        {
            let body = json!({"error": "invalid_grant", "error_description": "Token has been expired or revoked."});
            return (StatusCode::BAD_REQUEST, body.to_string()).into_response();
        }
        locked.tokens += 1;
        let token = format!("{}{}", &TOKEN_PREFIX["Bearer ".len()..], locked.tokens);
        return (
            StatusCode::OK,
            json!({"access_token": token, "token_type": "Bearer", "expires_in": 3600}).to_string(),
        )
            .into_response();
    }
    let sent = parts
        .headers
        .get(axum::http::header::AUTHORIZATION)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .unwrap_or_default();
    let refused = {
        let mut locked = state.lock();
        if locked.refuse_requests > 0 {
            locked.refuse_requests -= 1;
            locked.refused_tokens.push(sent.clone());
        }
        locked.refused_tokens.contains(&sent)
    };
    if !sent.starts_with(TOKEN_PREFIX) || refused {
        let body = google_error(
            401,
            "UNAUTHENTICATED",
            "Request had invalid authentication credentials.",
        );
        return (StatusCode::UNAUTHORIZED, body.to_string()).into_response();
    }
    let id = path
        .strip_prefix(SPREADSHEETS_PATH)
        .and_then(|rest| rest.strip_prefix('/'))
        .unwrap_or_default()
        .to_owned();
    let ranges: Vec<String> = query_pairs(parts.uri.query().unwrap_or_default())
        .into_iter()
        .filter(|(name, _)| name == RANGES_PARAMETER)
        .map(|(_, value)| value)
        .collect();
    let (status, body) = read(&state, &id, &ranges);
    (status, body.to_string()).into_response()
}

/// Spreadsheet `id`'s grid data: every tab, or the tab and cells of each range.
fn read(state: &SharedSheets, id: &str, ranges: &[String]) -> (StatusCode, Value) {
    let mut locked = state.lock();
    locked.reads.push(match ranges.is_empty() {
        true => "*".to_owned(),
        false => ranges.join(","),
    });
    let Some(tabs) = locked.spreadsheets.get(id).cloned() else {
        let body = google_error(404, "NOT_FOUND", "Requested entity was not found.");
        return (StatusCode::NOT_FOUND, body);
    };
    let mut sheets: Vec<Value> = Vec::new();
    if ranges.is_empty() {
        sheets.extend(tabs.iter().map(|tab| sheet(tab, (0, 0, None))));
    }
    for range in ranges {
        let (title, cells) = split_range(range);
        let Some(tab) = tabs.iter().find(|tab| tab.title == title) else {
            let body = google_error(
                400,
                "INVALID_ARGUMENT",
                &format!("Unable to parse range: {range}"),
            );
            return (StatusCode::BAD_REQUEST, body);
        };
        sheets.push(sheet(tab, cells.unwrap_or((0, 0, None))));
    }
    (
        StatusCode::OK,
        json!({"properties": {"title": FAKE_TITLE}, "sheets": sheets}),
    )
}

/// One tab's entry, its cells from `start_row` and `start_column` to `end_column`; as the real API does, the start is left out when it is the first cell, and a tab without cells has no `rowData`.
fn sheet(
    tab: &FakeTab,
    (start_row, start_column, end_column): (usize, usize, Option<usize>),
) -> Value {
    let rows: Vec<Value> = tab
        .rows
        .iter()
        .skip(start_row)
        .map(|row| {
            let end = end_column.map_or(row.len(), |end| (end + 1).min(row.len()));
            let cells: Vec<Value> = row
                .get(start_column..end.max(start_column))
                .unwrap_or_default()
                .to_vec();
            json!({"values": cells})
        })
        .collect();
    let mut data = json!({});
    if start_row > 0 {
        data["startRow"] = json!(start_row);
    }
    if start_column > 0 {
        data["startColumn"] = json!(start_column);
    }
    if !rows.is_empty() {
        data["rowData"] = json!(rows);
    }
    json!({"properties": {"title": tab.title, "sheetType": "GRID"}, "data": [data]})
}

/// A service-account key file of a new RSA key, exchanging tokens at `root`.
pub(crate) fn fake_key(root: &str) -> Result<String, String> {
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    let key = rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048)
        .map_err(|error| error.to_string())?
        .to_pkcs8_pem(LineEnding::LF)
        .map_err(|error| error.to_string())?;
    Ok(json!({
        "type": "service_account",
        "project_id": "fake-project",
        "client_email": "reader@fake-project.iam.gserviceaccount.com",
        "private_key": key.as_str(),
        "token_uri": format!("{root}{TOKEN_PATH}"),
    })
    .to_string())
}
