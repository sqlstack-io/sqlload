use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::fake_snowflake::constants::FAKE_BUCKET;
use crate::fake_snowflake::models::SharedState;

type Outcome = Result<Value, (StatusCode, Value)>;

/// `/session/v1/login-request`: any login works.
pub(crate) fn login() -> Outcome {
    Ok(json!({
        "success": true,
        "data": { "token": "session", "masterToken": "master", "validityInSeconds": 3600 },
    }))
}

/// `/queries/v1/query-request`: record the statement; a `PUT` gets upload details back unless its stage is refused.
pub(crate) fn query_request(state: &SharedState, body: &[u8]) -> Outcome {
    let request: Value = serde_json::from_slice(body).unwrap_or_default();
    let statement = request["sqlText"].as_str().unwrap_or_default().to_owned();
    let mut guard = state.lock();
    guard.driver_queries.push((
        request["sequenceId"].as_u64().unwrap_or_default(),
        statement.clone(),
    ));
    if !statement.starts_with("PUT ") {
        return Ok(json!({ "success": true, "data": {} }));
    }
    let denied = guard
        .denied_stages
        .iter()
        .any(|stage| statement.contains(stage.as_str()));
    if denied {
        return Ok(json!({
            "success": false,
            "code": "003001",
            "message": "Insufficient privileges on the stage",
        }));
    }
    let host = guard.host.clone();
    Ok(json!({
        "success": true,
        "data": {
            "stageInfo": {
                "locationType": "S3",
                "location": format!("{FAKE_BUCKET}/internal"),
                "region": "us-west-2",
                "endPoint": format!("http://{host}"),
                "creds": { "AWS_KEY_ID": "K", "AWS_SECRET_KEY": "S", "AWS_TOKEN": "T" },
            },
            "encryptionMaterial": {
                "queryStageMasterKey": "AAAAAAAAAAAAAAAAAAAAAA==",
                "queryId": "q",
                "smkId": 42,
            },
        },
    }))
}
