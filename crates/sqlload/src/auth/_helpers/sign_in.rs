//! Sign a person in to Snowflake in the browser: by a redirect to a loopback port, or by pasting the address the browser ended on when it cannot reach this machine.

use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::auth::_helpers::oauth::{
    Pkce, authorize_url, new_pkce, redirect_code, redirect_state, token_request,
};
use crate::auth::constants::{
    BROWSER_CHECK_MILLIS, BROWSER_CHECKS, LOOPBACK_HOST, LOOPBACK_SCHEME, NOT_THIS_SIGN_IN_PAGE,
    PROVIDER_PLACEHOLDER, REDIRECT_READ_TIMEOUT_SECS, REDIRECT_REQUEST_LIMIT, SIGN_IN_FAILED_PAGE,
    SIGN_IN_TIMEOUT_SECS, SIGNED_IN_PAGE, SNOWFLAKE_PROVIDER,
};
use crate::auth::errors::TokenError;
use crate::auth::models::{OauthClient, OauthTokens};
use crate::http::errors::ApiError;

/// How a sign-in reaches the person: whether a terminal is there to sign in from, how the browser is opened (false when it cannot be), and how a pasted line is read.
pub(crate) struct SignInIo {
    pub(crate) terminal: Box<dyn Fn() -> bool + Send + Sync>,
    pub(crate) open_browser: Box<dyn Fn(&str) -> bool + Send + Sync>,
    pub(crate) read_line: Box<dyn Fn() -> std::io::Result<String> + Send + Sync>,
}

/// Whether this run may ask a person to sign in; a command whose output a program reads (`--json`) turns it off.
pub(crate) static SIGN_IN_ALLOWED: AtomicBool = AtomicBool::new(true);

/// The real terminal and browser.
pub(crate) fn terminal_io() -> SignInIo {
    SignInIo {
        terminal: Box::new(|| {
            SIGN_IN_ALLOWED.load(Ordering::Relaxed)
                && std::io::stdin().is_terminal()
                && std::io::stderr().is_terminal()
        }),
        open_browser: Box::new(open_browser),
        read_line: Box::new(|| {
            let mut line = String::new();
            std::io::stdin().read_line(&mut line).map(|_| line)
        }),
    }
}

/// A browser sign-in to one provider: who signs in, the redirect URI an OAuth client registered (a free loopback port when `None`), and what to use instead when no terminal is there.
pub(crate) struct Consent<'a> {
    pub(crate) provider: &'a str,
    pub(crate) who: &'a str,
    pub(crate) redirect_uri: Option<&'a str>,
    pub(crate) unattended: &'a str,
}

/// What a browser sign-in returned: the authorization code, the redirect URI it was sent to, and the PKCE verifier that goes with it.
pub(crate) struct SignedCode {
    pub(crate) code: String,
    pub(crate) redirect_uri: String,
    pub(crate) verifier: String,
}

/// Sign in through the browser and exchange the code for tokens. Without a terminal this fails at once, as nobody is there to sign in.
pub(crate) async fn sign_in(client: &OauthClient, io: &SignInIo) -> Result<OauthTokens, ApiError> {
    let unattended = "run `sqlload debug` from a terminal once, or use auth = { method = \"key_pair\" } or { method = \"pat\" } for unattended runs";
    let consent = Consent {
        provider: SNOWFLAKE_PROVIDER,
        who: &client.user,
        redirect_uri: client.redirect_uri.as_deref(),
        unattended,
    };
    let signed = browser_code(&consent, io, |redirect_uri, pkce| {
        authorize_url(client, redirect_uri, pkce)
    })
    .await?;
    let grant = [
        ("grant_type", "authorization_code"),
        ("code", signed.code.as_str()),
        ("redirect_uri", signed.redirect_uri.as_str()),
        ("code_verifier", signed.verifier.as_str()),
    ];
    let tokens = token_request(client, &grant)
        .await
        .map_err(|error| match error {
            TokenError::Refused(reason) => {
                ApiError::Auth(format!("Snowflake refused the sign-in code: {reason}"))
            }
            TokenError::Failed(error) => error,
        })?;
    say(&format!("Signed in to Snowflake as {}.", client.user));
    Ok(tokens)
}

/// Have the person sign in to `consent.provider` in the browser, at the address `authorize` builds for a redirect URI and PKCE pair, and return the code it redirected back with: by a redirect to a loopback port, or by the person pasting the address the browser ended on. Without a terminal this fails at once.
pub(crate) async fn browser_code(
    consent: &Consent<'_>,
    io: &SignInIo,
    authorize: impl FnOnce(&str, &Pkce) -> Result<String, ApiError>,
) -> Result<SignedCode, ApiError> {
    let Consent { provider, who, .. } = consent;
    if !(io.terminal)() {
        return Err(ApiError::Auth(format!(
            "no usable {provider} sign-in is cached for {who}, and signing in in the browser needs a terminal; {}",
            consent.unattended
        )));
    }
    let pkce = new_pkce()?;
    let (listener, redirect_uri) = loopback(consent.redirect_uri).await?;
    let url = authorize(&redirect_uri, &pkce)?;
    say(&format!(
        "Sign in to {provider} as {who} in your browser:\n  {url}"
    ));
    let redirected = match (io.open_browser)(&url) {
        true => {
            say("Waiting for the browser to finish signing in...");
            tokio::time::timeout(
                Duration::from_secs(SIGN_IN_TIMEOUT_SECS),
                receive_redirect(&listener, (&pkce.state, provider)),
            )
            .await
            .map_err(|_| {
                ApiError::Auth(format!(
                    "the browser sign-in did not finish within {SIGN_IN_TIMEOUT_SECS} seconds"
                ))
            })??
        }
        false => {
            drop(listener);
            say(&format!(
                "Open that address in a browser and sign in. The browser then fails to load a page starting with {redirect_uri}; paste that page's whole address here:"
            ));
            (io.read_line)().map_err(|error| {
                ApiError::Auth(format!("cannot read the pasted address: {error}"))
            })?
        }
    };
    Ok(SignedCode {
        code: redirect_code(&redirected, (&pkce.state, provider))?,
        redirect_uri,
        verifier: pkce.verifier,
    })
}

/// A listener on the loopback address and the redirect URI that reaches it: the configured one (which must be `http://127.0.0.1[:port][/path]`), else a free port.
async fn loopback(configured: Option<&str>) -> Result<(TcpListener, String), ApiError> {
    let (port, path) = match configured {
        Some(uri) => {
            let url = match reqwest::Url::parse(uri) {
                Ok(url)
                    if url.scheme() == LOOPBACK_SCHEME && url.host_str() == Some(LOOPBACK_HOST) =>
                {
                    url
                }
                _ => {
                    return Err(ApiError::Auth(format!(
                        "redirect_uri {uri:?} must be http://127.0.0.1[:port][/path], where SQLLoad listens"
                    )));
                }
            };
            (url.port().unwrap_or(80), url.path().to_owned())
        }
        None => (0, "/".to_owned()),
    };
    let listener = TcpListener::bind((LOOPBACK_HOST, port))
        .await
        .map_err(|error| {
            ApiError::Auth(format!(
                "cannot listen on 127.0.0.1:{port} for the sign-in: {error}"
            ))
        })?;
    let bound = listener
        .local_addr()
        .map_err(|error| ApiError::Auth(error.to_string()))?;
    let uri = match configured {
        Some(uri) => uri.to_owned(),
        None => format!("http://127.0.0.1:{}{path}", bound.port()),
    };
    Ok((listener, uri))
}

/// The path and query of the first request to `listener` that carries this sign-in's `state`. Each connection is read on its own task with a time limit, so one that sends nothing or garbage holds nothing up; a request for another sign-in or with no state (another local process, or a web page probing the port) gets a 400 and is otherwise ignored.
async fn receive_redirect(
    listener: &TcpListener,
    (state, provider): (&str, &str),
) -> Result<String, ApiError> {
    let (found, mut received) = mpsc::channel::<String>(1);
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    tokio::spawn(answer_redirect(
                        stream,
                        (state.to_owned(), provider.to_owned()),
                        found.clone(),
                    ));
                }
                Err(error) => {
                    tracing::debug!(%error, "a connection to the sign-in listener failed");
                    tokio::time::sleep(Duration::from_millis(BROWSER_CHECK_MILLIS)).await;
                }
            },
            Some(target) = received.recv() => return Ok(target),
        }
    }
}

/// Answer one connection to the sign-in listener, and pass its request target on when it carries this sign-in's `state`.
async fn answer_redirect(
    mut stream: TcpStream,
    (state, provider): (String, String),
    found: mpsc::Sender<String>,
) {
    let read = tokio::time::timeout(
        Duration::from_secs(REDIRECT_READ_TIMEOUT_SECS),
        request_target(&mut stream),
    )
    .await;
    let Ok(Ok(target)) = read else {
        return;
    };
    let returned = redirect_state(&target);
    let ours = returned.as_deref() == Some(state.as_str());
    let unstated_error = returned.is_none() && target.contains("error=");
    if let (true, Err(error)) = (unstated_error, redirect_code(&target, (&state, &provider))) {
        say(&format!(
            "An error came back without the sign-in's state, so SQLLoad keeps waiting: {error}. If it came from your sign-in, press Ctrl-C and fix that."
        ));
    }
    let (status, page) = match (ours, target.contains("error=")) {
        (false, _) if unstated_error => ("400 Bad Request", SIGN_IN_FAILED_PAGE),
        (false, _) => ("400 Bad Request", NOT_THIS_SIGN_IN_PAGE),
        (true, true) => ("200 OK", SIGN_IN_FAILED_PAGE),
        (true, false) => ("200 OK", SIGNED_IN_PAGE),
    };
    let page = page.replace(PROVIDER_PLACEHOLDER, &provider);
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{page}",
        page.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
    if ours {
        let _ = found.send(target).await;
    }
}

/// The target (path and query) of the HTTP request on `stream`, read up to the end of its headers or `REDIRECT_REQUEST_LIMIT` bytes.
async fn request_target(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut request: Vec<u8> = Vec::new();
    let mut buffer = [0_u8; 2048];
    while !request.windows(4).any(|window| window == b"\r\n\r\n")
        && request.len() < REDIRECT_REQUEST_LIMIT
    {
        let read = stream.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
    }
    let text = String::from_utf8_lossy(&request);
    Ok(text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or_default()
        .to_owned())
}

/// Open `url` in the person's browser without waiting for the browser itself: true once the opener exits successfully or is still running after about two seconds; false where there is no browser to open (an SSH session without a forwarded display) or the opener failed.
fn open_browser(url: &str) -> bool {
    let has = |name: &str| std::env::var_os(name).is_some_and(|value| !value.is_empty());
    let remote = has("SSH_CONNECTION") || has("SSH_TTY");
    let (program, arguments): (&str, Vec<&str>) = match std::env::consts::OS {
        "macos" if !remote => ("open", vec![url]),
        "windows" if !remote => ("rundll32", vec!["url.dll,FileProtocolHandler", url]),
        "linux" | "freebsd" | "openbsd" | "netbsd" if has("DISPLAY") || has("WAYLAND_DISPLAY") => {
            ("xdg-open", vec![url])
        }
        _ => return false,
    };
    let Ok(mut opener) = std::process::Command::new(program)
        .args(arguments)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    else {
        return false;
    };
    for _ in 0..BROWSER_CHECKS {
        match opener.try_wait() {
            Ok(Some(status)) => return status.success(),
            Ok(None) => std::thread::sleep(Duration::from_millis(BROWSER_CHECK_MILLIS)),
            Err(_) => return false,
        }
    }
    true
}

/// Tell the person signing in what to do, on stderr so stdout stays the command's output.
fn say(text: &str) {
    let _ = writeln!(std::io::stderr(), "{text}");
}
