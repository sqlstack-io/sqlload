use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};

use crate::auth::_helpers::oauth::{challenge, fresh_access_token, now_secs};
use crate::auth::_helpers::sign_in::SignInIo;
use crate::auth::_helpers::token_cache::{read_cached, write_cached};
use crate::auth::models::{OauthClient, OauthTokens, Secret};
use crate::auth::tests::test_types::{FakeOauth, LifecycleOutcome, TokenLifecycleTestCase};
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;

type Shared = Arc<Mutex<FakeOauth>>;

pub(crate) fn block_on<T>(
    body: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(body)
}

/// A client of the fake at `base` signing in `loader` as role `loader`, caching in `file`.
pub(crate) fn test_client(base: &str, file: &Path, user: &str) -> Result<OauthClient, String> {
    Ok(OauthClient {
        account_url: base.to_owned(),
        client_id: "LOCAL_APPLICATION".to_owned(),
        client_secret: None,
        redirect_uri: None,
        user: user.to_owned(),
        role: Some("loader".to_owned()),
        cache_file: file.to_owned(),
        http: build_http_client(&ConnectionCounter::default())
            .map_err(|error| error.to_string())?,
    })
}

/// Tokens whose access token has `left` seconds to go.
pub(crate) fn tokens(access: &str, left: i64, refresh: Option<&str>) -> OauthTokens {
    OauthTokens {
        access_token: Secret(access.to_owned()),
        access_expires_at: now_secs().saturating_add_signed(left),
        refresh_token: refresh.map(|token| Secret(token.to_owned())),
        refresh_expires_at: None,
    }
}

/// Ask for a token as `test_case` sets things up, against a fresh fake and cache.
pub(crate) async fn lifecycle(
    test_case: &TokenLifecycleTestCase,
) -> Result<LifecycleOutcome, String> {
    let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
    let fake: Shared = Arc::new(Mutex::new(FakeOauth {
        revoked: test_case
            .revoked
            .iter()
            .map(|token| (*token).to_owned())
            .collect(),
        username: test_case.username.to_owned(),
        refusal: test_case.refusal.map(str::to_owned),
        ..FakeOauth::default()
    }));
    let base = start_fake_oauth(fake.clone()).await?;
    let client = test_client(&base, &folder.path().join("tokens.json"), "loader")?;
    test_case
        .cached
        .map(|(access, left, refresh)| write_cached(&client, &tokens(access, left, refresh)))
        .transpose()?;
    let (io, opened) = fake_io(test_case.terminal, test_case.browser)?;
    let held = tokio::sync::Mutex::new(None);
    let outcome = fresh_access_token(&client, &held, &io)
        .await
        .unwrap_or_else(|error| error.to_string());
    let cached_refresh = read_cached(&client)
        .and_then(|cached| cached.refresh_token)
        .map(|token| token.expose().to_owned());
    let fake = fake.lock().map_err(|error| error.to_string())?;
    let opened = opened.lock().map(|url| url.clone()).unwrap_or_default();
    let sent = query_pairs(opened.split_once('?').map_or("", |(_, query)| query));
    let sent_challenge = sent.get("code_challenge").cloned().unwrap_or_default();
    Ok(LifecycleOutcome {
        outcome,
        grants: fake.grants.clone(),
        cached_refresh,
        pkce_matches: fake.verifier.is_empty() || challenge(&fake.verifier) == sent_challenge,
    })
}

/// A terminal that is there or not; a browser that follows the sign-in to its redirect, or none, in which case the read line is the address the fake would redirect to; and the last address the browser was asked to open.
fn fake_io(terminal: bool, browser: bool) -> Result<(SignInIo, Arc<Mutex<String>>), String> {
    let http =
        build_http_client(&ConnectionCounter::default()).map_err(|error| error.to_string())?;
    let seen = Arc::new(Mutex::new(String::new()));
    let (opened, pasted) = (seen.clone(), seen.clone());
    let io = SignInIo {
        terminal: Box::new(move || terminal),
        open_browser: Box::new(move |url| {
            opened
                .lock()
                .into_iter()
                .for_each(|mut last| url.clone_into(&mut last));
            let (http, url) = (http.clone(), url.to_owned());
            let _following =
                browser.then(|| tokio::spawn(async move { http.get(url).send().await }));
            browser
        }),
        read_line: Box::new(move || {
            let url = pasted.lock().map(|url| url.clone()).unwrap_or_default();
            let query = query_pairs(url.split_once('?').map_or("", |(_, query)| query));
            let redirect = query.get("redirect_uri").cloned().unwrap_or_default();
            let state = query.get("state").cloned().unwrap_or_default();
            Ok(format!("{redirect}?code=fake-code&state={state}\n"))
        }),
    };
    Ok((io, seen))
}

/// Serve the fake's authorization and token endpoints on a free local port.
async fn start_fake_oauth(state: Shared) -> Result<String, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    let router = Router::new()
        .route("/oauth/authorize", get(authorize))
        .route("/oauth/token-request", post(token_request))
        .with_state(state);
    tokio::spawn(async move { axum::serve(listener, router).await });
    Ok(format!("http://{address}"))
}

/// Redirect back with a code, or with the refusal.
async fn authorize(State(state): State<Shared>, uri: Uri) -> Response {
    let query = query_pairs(uri.query().unwrap_or_default());
    let Ok(fake) = state.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let redirect = query.get("redirect_uri").cloned().unwrap_or_default();
    let returned = query.get("state").cloned().unwrap_or_default();
    let answer = fake.refusal.as_ref().map_or_else(
        || format!("code=fake-code&state={returned}"),
        |error| format!("error={error}&error_description=refused&state={returned}"),
    );
    (
        StatusCode::FOUND,
        [(header::LOCATION, format!("{redirect}?{answer}"))],
    )
        .into_response()
}

/// Exchange the fake code or a refresh token that is not revoked for new tokens.
async fn token_request(State(state): State<Shared>, body: String) -> Response {
    let form = query_pairs(&body);
    let Ok(mut fake) = state.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let grant = form.get("grant_type").cloned().unwrap_or_default();
    fake.grants.push(grant.clone());
    let refused = form
        .get("refresh_token")
        .is_some_and(|token| fake.revoked.contains(token))
        || form.get("code").is_some_and(|code| code != "fake-code");
    form.get("code_verifier")
        .into_iter()
        .for_each(|verifier| verifier.clone_into(&mut fake.verifier));
    fake.issued += u32::from(!refused);
    let issued = fake.issued;
    let username = fake.username.clone();
    (!refused)
        .then(|| {
            serde_json::json!({
                "access_token": format!("access-{issued}"),
                "expires_in": 600,
                "refresh_token": format!("refresh-{issued}"),
                "token_type": "Bearer",
                "username": username,
            })
        })
        .map_or_else(
            || {
                let body = r#"{"error":"invalid_grant","message":"refused"}"#;
                (StatusCode::BAD_REQUEST, body.to_owned()).into_response()
            },
            |body| (StatusCode::OK, body.to_string()).into_response(),
        )
}

/// The pairs of a URL-encoded query or form.
fn query_pairs(text: &str) -> BTreeMap<String, String> {
    reqwest::Url::parse(&format!("http://fake/?{text}"))
        .map(|url| url.query_pairs().into_owned().collect())
        .unwrap_or_default()
}
