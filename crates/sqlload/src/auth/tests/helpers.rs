use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde_json::{Value, json};

use crate::auth::_helpers::google_oauth::{cache_key, google_access_token};
use crate::auth::_helpers::oauth::{challenge, fresh_access_token, now_secs};
use crate::auth::_helpers::sign_in::SignInIo;
use crate::auth::_helpers::token_cache::{read_cached, read_entry, write_cached, write_entry};
use crate::auth::models::{GoogleUser, OauthClient, OauthState, OauthTokens, Secret};
use crate::auth::tests::test_types::{
    FakeGoogle, FakeOauth, LifecycleOutcome, TokenLifecycleTestCase, UserTokenOutcome,
    UserTokenTestCase,
};
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::loader::errors::LoaderError;

type Shared = Arc<Mutex<FakeOauth>>;

/// The scope the Google tests ask for.
const GOOGLE_TEST_SCOPE: &str = "https://www.googleapis.com/auth/spreadsheets.readonly";

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

/// Tokens whose access token is due for renewal `left` seconds after `now`; a refresh token named `expired-…` expired a second before `now`.
pub(crate) fn tokens(now: u64, access: &str, left: i64, refresh: Option<&str>) -> OauthTokens {
    OauthTokens {
        access_token: Secret(access.to_owned()),
        renew_after: now.saturating_add_signed(left),
        refresh_token: refresh.map(|token| Secret(token.to_owned())),
        refresh_expires_at: refresh
            .filter(|token| token.starts_with("expired-"))
            .map(|_| now - 1),
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
    let now = now_secs();
    test_case
        .cached
        .map(|(access, left, refresh)| write_cached(&client, &tokens(now, access, left, refresh)))
        .transpose()?;
    let io = fake_io(test_case.terminal, test_case.browser, fake.clone())?;
    let state = tokio::sync::Mutex::new(OauthState {
        tokens: test_case
            .held
            .map(|(access, left, refresh)| tokens(now, access, left, refresh)),
        failure: None,
    });
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        fresh_access_token(&client, &state, &io),
    )
    .await
    .map_err(|_| "no token within 5 s, under the listener's read limit".to_owned())?
    .unwrap_or_else(|error| error.to_string());
    let grants = fake
        .lock()
        .map(|fake| fake.grants.clone())
        .map_err(|error| error.to_string())?;
    let cached_refresh = read_cached(&client)
        .and_then(|cached| cached.refresh_token)
        .map(|token| token.expose().to_owned());
    let repeated = fresh_access_token(&client, &state, &io)
        .await
        .unwrap_or_else(|error| error.to_string());
    let requests = fake
        .lock()
        .map(|fake| fake.grants.len())
        .map_err(|error| error.to_string())?;
    Ok(LifecycleOutcome {
        outcome,
        grants,
        cached_refresh,
        repeated: (repeated, requests),
    })
}

/// A terminal that is there or not; a browser that, after opening a connection that sends nothing and two forged redirects as another local process could send (another sign-in's state, and an error with no state), follows the sign-in to its redirect; or none, in which case the read line is the address the fake would redirect to, after the person opened the sign-in elsewhere.
fn fake_io(terminal: bool, browser: bool, fake: Shared) -> Result<SignInIo, String> {
    let http =
        build_http_client(&ConnectionCounter::default()).map_err(|error| error.to_string())?;
    let seen = Arc::new(Mutex::new(String::new()));
    let (opened, pasted) = (seen.clone(), seen);
    let io = SignInIo {
        terminal: Box::new(move || terminal),
        open_browser: Box::new(move |url| {
            opened
                .lock()
                .into_iter()
                .for_each(|mut last| url.clone_into(&mut last));
            let (http, url) = (http.clone(), url.to_owned());
            let _following = browser.then(|| {
                tokio::spawn(async move {
                    let query = query_pairs(url.split_once('?').map_or("", |(_, query)| query));
                    let redirect = query.get("redirect_uri").cloned().unwrap_or_default();
                    let port = reqwest::Url::parse(&redirect)
                        .map(|redirect| redirect.port().unwrap_or(80))
                        .unwrap_or_default();
                    let _idle = tokio::net::TcpStream::connect(("127.0.0.1", port)).await;
                    let _forged = http
                        .get(format!("{redirect}?code=evil&state=other"))
                        .send()
                        .await;
                    let _unstated = http
                        .get(format!("{redirect}?error=access_denied"))
                        .send()
                        .await;
                    http.get(url).send().await
                })
            });
            browser
        }),
        read_line: Box::new(move || {
            let url = pasted.lock().map(|url| url.clone()).unwrap_or_default();
            let query = query_pairs(url.split_once('?').map_or("", |(_, query)| query));
            let redirect = query.get("redirect_uri").cloned().unwrap_or_default();
            let state = query.get("state").cloned().unwrap_or_default();
            let challenge = query.get("code_challenge").cloned().unwrap_or_default();
            fake.lock()
                .into_iter()
                .for_each(|mut fake| fake.challenge.clone_from(&challenge));
            Ok(format!("{redirect}?code=fake-code&state={state}\n"))
        }),
    };
    Ok(io)
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

/// Record the PKCE challenge and redirect back with a code, or with the refusal.
async fn authorize(State(state): State<Shared>, uri: Uri) -> Response {
    let query = query_pairs(uri.query().unwrap_or_default());
    let Ok(mut fake) = state.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    fake.challenge = query.get("code_challenge").cloned().unwrap_or_default();
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

/// Exchange the fake code with the verifier of the last challenge, or a refresh token that is not revoked, for new tokens; a refresh token starting `fixed-` is not rotated, so the answer carries none.
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
        || form.get("code").is_some_and(|code| {
            let verified = form
                .get("code_verifier")
                .is_some_and(|verifier| challenge(verifier) == fake.challenge);
            code != "fake-code" || !verified
        });
    let rotates = !form
        .get("refresh_token")
        .is_some_and(|token| token.starts_with("fixed-"));
    fake.issued += u32::from(!refused);
    let issued = fake.issued;
    let username = fake.username.clone();
    (!refused)
        .then(|| {
            serde_json::json!({
                "access_token": format!("access-{issued}"),
                "expires_in": 600,
                "refresh_token": rotates.then(|| format!("refresh-{issued}")),
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

type SharedGoogle = Arc<Mutex<FakeGoogle>>;

/// Ask for an access token as `test_case` sets things up, against a fresh fake Google and cache, then ask again.
pub(crate) async fn user_token_outcome(
    test_case: &UserTokenTestCase,
) -> Result<UserTokenOutcome, String> {
    let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
    let fake: SharedGoogle = Arc::new(Mutex::new(FakeGoogle {
        revoked: test_case
            .revoked
            .iter()
            .map(|token| (*token).to_owned())
            .collect(),
        ..FakeGoogle::default()
    }));
    let base = start_fake_google(fake.clone()).await?;
    let user = GoogleUser {
        client_id: "client.apps.googleusercontent.com".to_owned(),
        client_secret: Secret(test_case.client_secret.to_owned()),
        refresh_token: test_case.configured.map(|token| Secret(token.to_owned())),
        scope: GOOGLE_TEST_SCOPE.to_owned(),
        connection: "sheets".to_owned(),
        authorize_url: format!("{base}/auth"),
        token_url: format!("{base}/token"),
        cache_file: folder.path().join("google-tokens.json"),
    };
    let now = now_secs();
    test_case
        .cached
        .map(|(access, left, refresh)| {
            let tokens = OauthTokens {
                access_token: Secret(access.to_owned()),
                renew_after: now.saturating_add_signed(left),
                refresh_token: Some(Secret(refresh.to_owned())),
                refresh_expires_at: None,
            };
            write_entry(&user.cache_file, &cache_key(&user), &tokens)
        })
        .transpose()?;
    let refused: Option<Secret> = test_case.refused.map(|token| Secret(token.to_owned()));
    let held: tokio::sync::Mutex<Option<(Secret, i64)>> =
        tokio::sync::Mutex::new(refused.clone().map(|token| (token, i64::MAX)));
    let http =
        build_http_client(&ConnectionCounter::default()).map_err(|error| error.to_string())?;
    let io = fake_browser(test_case.terminal, http.clone());
    let first = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        google_access_token(&http, &user, &held, (refused.as_ref(), &io)),
    )
    .await
    .map_err(|_| "no token within 5 s".to_owned())?;
    let retryable = first.as_ref().err().is_some_and(|error| {
        error.is_retryable() && LoaderError::from(error.clone()).is_retryable()
    });
    let outcome = first.map_or_else(|error| error.to_string(), |token| token.expose().to_owned());
    let grants = fake
        .lock()
        .map(|fake| fake.grants.clone())
        .map_err(|error| error.to_string())?;
    let cached_refresh = read_entry(&user.cache_file, &cache_key(&user))
        .and_then(|cached| cached.refresh_token)
        .map(|token| token.expose().to_owned());
    let repeated = google_access_token(&http, &user, &held, (None, &io))
        .await
        .map_or_else(|error| error.to_string(), |token| token.expose().to_owned());
    let requests = fake
        .lock()
        .map(|fake| fake.grants.len())
        .map_err(|error| error.to_string())?;
    Ok(UserTokenOutcome {
        outcome,
        retryable,
        grants,
        cached_refresh,
        repeated: (repeated, requests),
    })
}

/// A terminal that is there or not, with a browser that follows the consent page to its redirect; nothing is ever pasted.
fn fake_browser(terminal: bool, http: reqwest::Client) -> SignInIo {
    SignInIo {
        terminal: Box::new(move || terminal),
        open_browser: Box::new(move |url| {
            let (http, url) = (http.clone(), url.to_owned());
            tokio::spawn(async move { http.get(url).send().await });
            true
        }),
        read_line: Box::new(|| Err(std::io::Error::other("nothing is pasted"))),
    }
}

/// Serve the fake's consent page and token endpoint on a free local port.
async fn start_fake_google(state: SharedGoogle) -> Result<String, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    let router = Router::new()
        .route("/auth", get(google_consent))
        .route("/token", post(google_token))
        .with_state(state);
    tokio::spawn(async move { axum::serve(listener, router).await });
    Ok(format!("http://{address}"))
}

/// Record the PKCE challenge and redirect back with a code, when the consent asks for offline read-only access to spreadsheets with the consent screen shown; else with an error.
async fn google_consent(State(state): State<SharedGoogle>, uri: Uri) -> Response {
    let query = query_pairs(uri.query().unwrap_or_default());
    let Ok(mut fake) = state.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let field = |name: &str| query.get(name).cloned().unwrap_or_default();
    fake.challenge = field("code_challenge");
    let asked = field("scope") == GOOGLE_TEST_SCOPE
        && field("access_type") == "offline"
        && field("prompt") == "consent"
        && field("code_challenge_method") == "S256";
    let answer = ["error=invalid_request", "code=fake-code"][usize::from(asked)];
    let location = format!(
        "{}?{answer}&state={}",
        field("redirect_uri"),
        field("state")
    );
    (StatusCode::FOUND, [(header::LOCATION, location)]).into_response()
}

/// Answer a token request as Google does: `invalid_client` for a wrong secret, 503 for the refresh token `unavailable`, `invalid_grant` for a revoked refresh token or a code whose verifier does not match, else an access token (and a refresh token for a code).
async fn google_token(State(state): State<SharedGoogle>, body: String) -> Response {
    let form = query_pairs(&body);
    let Ok(mut fake) = state.lock() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let field = |name: &str| form.get(name).cloned().unwrap_or_default();
    fake.grants.push(field("grant_type"));
    fake.issued += 1;
    let issued = fake.issued;
    let refresh = field("refresh_token");
    let exchanged = form.contains_key("code");
    let verified =
        field("code") == "fake-code" && challenge(&field("code_verifier")) == fake.challenge;
    let answers: [(bool, StatusCode, Value); 5] = [
        (
            field("client_secret") != "secret",
            StatusCode::UNAUTHORIZED,
            json!({"error": "invalid_client", "error_description": "The OAuth client was not found."}),
        ),
        (
            refresh == "unavailable",
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error": "backend_error", "error_description": "try later"}),
        ),
        (
            refresh == "broken",
            StatusCode::INTERNAL_SERVER_ERROR,
            json!(format!("<html>{}</html>", "x".repeat(400))),
        ),
        (
            fake.revoked.contains(&refresh) || (exchanged && !verified),
            StatusCode::BAD_REQUEST,
            json!({"error": "invalid_grant", "error_description": "Token has been expired or revoked."}),
        ),
        (
            true,
            StatusCode::OK,
            json!({"access_token": format!("access-{issued}"), "expires_in": 3599, "token_type": "Bearer", "scope": GOOGLE_TEST_SCOPE, "refresh_token": exchanged.then(|| format!("refresh-{issued}"))}),
        ),
    ];
    answers
        .into_iter()
        .find(|(applies, _, _)| *applies)
        .map_or_else(
            || StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            |(_, status, body)| (status, body.to_string()).into_response(),
        )
}
