//! The fake's sign-in endpoints: Google's token endpoint (key assertions and refresh tokens), the metadata server, Google STS, IAM Credentials `generateAccessToken`, and Azure's instance metadata service.

use axum::body::Bytes;
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use crate::fake_bigquery::_helpers::statements::google_error;
use crate::fake_bigquery::constants::{
    AWS_ROLE, AWS_SESSION, REFUSED_SUBJECT, TOKEN_PATH, TOKEN_PREFIX,
};
use crate::fake_bigquery::models::SharedState;

/// The answer to a sign-in request, or `None` when `parts` is not one; every sign-in is recorded in `sign_ins`.
pub(crate) fn sign_in(state: &SharedState, parts: &Parts, body: &Bytes) -> Option<Response> {
    let path = parts.uri.path();
    let method = parts.method.as_str();
    let form = || form_pairs(&String::from_utf8_lossy(body));
    let query = || form_pairs(parts.uri.query().unwrap_or_default());
    let header = |name: &str| {
        parts
            .headers
            .get(name)
            .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
    };
    if let Some(text) = aws_metadata(state, (method, path), &header) {
        return Some(text);
    }
    let answer = match (method, path) {
        ("POST", TOKEN_PATH) => {
            let form = form();
            match field(&form, "grant_type").as_str() {
                "refresh_token" => {
                    let refresh = field(&form, "refresh_token");
                    let refused = state.lock().refused_refresh_tokens.contains(&refresh);
                    record(
                        state,
                        format!("refresh client_id={}", field(&form, "client_id")),
                    );
                    match refused {
                        true => Err((
                            StatusCode::BAD_REQUEST,
                            json!({ "error": "invalid_grant", "error_description": "Token has been expired or revoked." }),
                        )),
                        false => Ok(token_body(state, "access_token")),
                    }
                }
                _ => {
                    record(state, "key".to_owned());
                    Ok(token_body(state, "access_token"))
                }
            }
        }
        ("POST", "/v1/token") => {
            let form = form();
            let subject = field(&form, "subject_token");
            record(
                state,
                format!(
                    "sts audience={} type={} scope={} subject={subject}",
                    field(&form, "audience"),
                    field(&form, "subject_token_type"),
                    field(&form, "scope")
                ),
            );
            if field(&form, "subject_token_type").ends_with(":aws4_request") {
                record(state, aws_subject(&subject));
            }
            match subject.is_empty() || subject == REFUSED_SUBJECT {
                true => Err((
                    StatusCode::BAD_REQUEST,
                    json!({ "error": "invalid_grant", "error_description": "The subject token is invalid." }),
                )),
                false => Ok(token_body(state, "access_token")),
            }
        }
        ("POST", path) if path.ends_with(":generateAccessToken") => {
            let bearer = header("authorization").unwrap_or_default();
            let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let account = path
                .rsplit_once("/serviceAccounts/")
                .map_or("", |(_, rest)| {
                    rest.trim_end_matches(":generateAccessToken")
                });
            record(
                state,
                format!(
                    "impersonate {account} lifetime={} delegates={} scope={}",
                    request["lifetime"].as_str().unwrap_or_default(),
                    request["delegates"],
                    request["scope"]
                ),
            );
            let denied = state
                .lock()
                .denied_accounts
                .iter()
                .any(|denied| denied == account);
            match (bearer.starts_with(TOKEN_PREFIX), denied) {
                (false, _) => Err((
                    StatusCode::UNAUTHORIZED,
                    google_error(
                        401,
                        "unauthorized",
                        "Request had invalid authentication credentials.",
                    ),
                )),
                (true, true) => Err((
                    StatusCode::FORBIDDEN,
                    json!({ "error": { "code": 403, "status": "PERMISSION_DENIED", "message": format!("Permission 'iam.serviceAccounts.getAccessToken' denied on resource {account}") } }),
                )),
                (true, false) => {
                    let mut body = token_body(state, "accessToken");
                    let lifetime = body["expires_in"].as_i64().unwrap_or(3600);
                    body["expireTime"] = json!(
                        (chrono::Utc::now() + chrono::Duration::seconds(lifetime))
                            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                    );
                    if let Some(object) = body.as_object_mut() {
                        object.remove("expires_in");
                    }
                    Ok(body)
                }
            }
        }
        ("GET", "/computeMetadata/v1/instance/service-accounts/default/token") => {
            record(
                state,
                format!("metadata scopes={}", field(&query(), "scopes")),
            );
            match header("metadata-flavor").as_deref() {
                Some("Google") => Ok(token_body(state, "access_token")),
                _ => Err((
                    StatusCode::FORBIDDEN,
                    json!({ "error": "missing Metadata-Flavor" }),
                )),
            }
        }
        ("GET", "/metadata/identity/oauth2/token") => {
            let query = query();
            record(
                state,
                format!(
                    "azure api-version={} resource={} metadata={}",
                    field(&query, "api-version"),
                    field(&query, "resource"),
                    header("metadata").unwrap_or_default()
                ),
            );
            match header("metadata").as_deref() {
                Some("True") => {
                    let mut locked = state.lock();
                    locked.tokens += 1;
                    Ok(
                        json!({ "access_token": format!("azure-subject-{}", locked.tokens), "expires_in": "3599", "token_type": "Bearer" }),
                    )
                }
                _ => Err((
                    StatusCode::BAD_REQUEST,
                    json!({ "error": "invalid_request", "error_description": "Required metadata header not specified" }),
                )),
            }
        }
        _ => return None,
    };
    Some(match answer {
        Ok(value) => (StatusCode::OK, value.to_string()).into_response(),
        Err((status, value)) => (status, value.to_string()).into_response(),
    })
}

/// AWS's instance metadata service, IMDSv2 only: a session token for a `PUT` naming its lifetime, then the zone, the instance role and its keys for `GET`s carrying that session; `None` for other paths.
fn aws_metadata(
    state: &SharedState,
    (method, path): (&str, &str),
    header: &dyn Fn(&str) -> Option<String>,
) -> Option<Response> {
    let session = header("x-aws-ec2-metadata-token");
    let answer = match (method, path) {
        ("PUT", "/latest/api/token") => {
            let ttl = header("x-aws-ec2-metadata-token-ttl-seconds").unwrap_or_default();
            record(state, format!("aws session ttl={ttl}"));
            return Some(match ttl.is_empty() {
                true => (StatusCode::BAD_REQUEST, String::new()).into_response(),
                false => (StatusCode::OK, AWS_SESSION.to_owned()).into_response(),
            });
        }
        ("GET", "/latest/meta-data/placement/availability-zone") => {
            record(state, "aws region".to_owned());
            Some("eu-north-1a".to_owned())
        }
        ("GET", "/latest/meta-data/iam/security-credentials") => {
            record(state, "aws role".to_owned());
            Some(format!("{AWS_ROLE}\n"))
        }
        ("GET", path)
            if path == format!("/latest/meta-data/iam/security-credentials/{AWS_ROLE}") =>
        {
            record(state, "aws keys".to_owned());
            Some(
                json!({ "Code": "Success", "AccessKeyId": "ASIAFAKEROLE", "SecretAccessKey": "fake-secret", "Token": "fake-role-session" })
                    .to_string(),
            )
        }
        _ => return None,
    };
    Some(match (answer, session.as_deref() == Some(AWS_SESSION)) {
        (Some(text), true) => (StatusCode::OK, text).into_response(),
        _ => (StatusCode::UNAUTHORIZED, String::new()).into_response(),
    })
}

/// What a subject token of AWS's kind (a signed `GetCallerIdentity` request, as URL-encoded JSON) asks: its URL, the session token it carries and the start of its credential scope.
fn aws_subject(subject: &str) -> String {
    let request: Value = serde_json::from_str(&decode(subject)).unwrap_or(Value::Null);
    let header = |name: &str| {
        request["headers"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|pair| {
                pair["key"]
                    .as_str()
                    .is_some_and(|key| key.eq_ignore_ascii_case(name))
            })
            .and_then(|pair| pair["value"].as_str())
            .unwrap_or_default()
            .to_owned()
    };
    let authorization = header("Authorization");
    let credential = authorization
        .split_once("Credential=")
        .and_then(|(_, rest)| rest.split_once('/'))
        .map_or("", |(key, _)| key);
    format!(
        "aws-subject method={} url={} target={} token={} key={credential} signed={}",
        request["method"].as_str().unwrap_or_default(),
        request["url"].as_str().unwrap_or_default(),
        header("x-goog-cloud-target-resource"),
        header("x-amz-security-token"),
        authorization.contains("SignedHeaders=host;x-amz-date;x-amz-security-token;x-goog-cloud-target-resource, Signature=")
    )
}

/// A new fake token under `field`, lasting the fake's token lifetime.
fn token_body(state: &SharedState, field: &str) -> Value {
    let mut locked = state.lock();
    locked.tokens += 1;
    let token = format!("{}{}", &TOKEN_PREFIX["Bearer ".len()..], locked.tokens);
    json!({ field: token, "token_type": "Bearer", "expires_in": locked.token_lifetime_secs.unwrap_or(3600) })
}

/// Note one sign-in request.
fn record(state: &SharedState, line: String) {
    state.lock().sign_ins.push(line);
}

/// One field's value, empty when it is not there.
fn field(pairs: &[(String, String)], name: &str) -> String {
    pairs
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
        .unwrap_or_default()
}

/// `a=1&b=%2F` as decoded pairs.
fn form_pairs(text: &str) -> Vec<(String, String)> {
    text.split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode(key), decode(value))
        })
        .collect()
}

/// Percent- and plus-decoding.
fn decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let hex = match bytes.get(index + 1..index + 3) {
            Some([high, low]) => hex_digit(*high).zip(hex_digit(*low)),
            _ => None,
        }
        .map(|(high, low)| high * 16 + low);
        match (bytes[index], hex) {
            (b'%', Some(byte)) => {
                out.push(byte);
                index += 3;
            }
            (b'+', _) => {
                out.push(b' ');
                index += 1;
            }
            (byte, _) => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A hex digit's value.
fn hex_digit(byte: u8) -> Option<u8> {
    char::from(byte).to_digit(16).map(|digit| digit as u8)
}
