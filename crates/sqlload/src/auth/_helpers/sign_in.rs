//! Sign a person in to Snowflake in the browser: by a redirect to a loopback port, or by pasting the address the browser ended on when it cannot reach this machine.

use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::auth::_helpers::oauth::{authorize_url, new_pkce, redirect_code, token_request};
use crate::auth::constants::{
    LOOPBACK_HOST, LOOPBACK_SCHEME, REDIRECT_REQUEST_LIMIT, SIGN_IN_FAILED_PAGE,
    SIGN_IN_TIMEOUT_SECS, SIGNED_IN_PAGE,
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

/// Sign in through the browser and exchange the code for tokens. Without a terminal this fails at once, as nobody is there to sign in.
pub(crate) async fn sign_in(client: &OauthClient, io: &SignInIo) -> Result<OauthTokens, ApiError> {
    if !(io.terminal)() {
        return Err(ApiError::Auth(format!(
            "no usable Snowflake sign-in is cached for {}, and signing in in the browser needs a terminal; run `sqlload debug` from a terminal once, or use auth = {{ method = \"key_pair\" }} or {{ method = \"pat\" }} for unattended runs",
            client.user
        )));
    }
    let pkce = new_pkce()?;
    let (listener, redirect_uri) = loopback(client.redirect_uri.as_deref()).await?;
    let url = authorize_url(client, &redirect_uri, &pkce)?;
    say(&format!(
        "Sign in to Snowflake as {} in your browser:\n  {url}",
        client.user
    ));
    let redirected = match (io.open_browser)(&url) {
        true => {
            say("Waiting for the browser to finish signing in...");
            tokio::time::timeout(
                Duration::from_secs(SIGN_IN_TIMEOUT_SECS),
                receive_redirect(&listener),
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
    let code = redirect_code(&redirected, &pkce.state)?;
    let grant = [
        ("grant_type", "authorization_code"),
        ("code", code.as_str()),
        ("redirect_uri", redirect_uri.as_str()),
        ("code_verifier", pkce.verifier.as_str()),
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

/// The path and query of the first request to `listener` that carries a code or an error, answered with a page saying to return to the terminal; other requests (a favicon) get a 404.
async fn receive_redirect(listener: &TcpListener) -> Result<String, ApiError> {
    loop {
        let (mut stream, _) = listener
            .accept()
            .await
            .map_err(|error| ApiError::Auth(format!("the sign-in redirect failed: {error}")))?;
        let mut request: Vec<u8> = Vec::new();
        let mut buffer = [0_u8; 2048];
        while !request.windows(4).any(|window| window == b"\r\n\r\n")
            && request.len() < REDIRECT_REQUEST_LIMIT
        {
            let read = stream
                .read(&mut buffer)
                .await
                .map_err(|error| ApiError::Auth(format!("the sign-in redirect failed: {error}")))?;
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
        }
        let text = String::from_utf8_lossy(&request);
        let target = text
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or_default()
            .to_owned();
        let (status, page) = match (target.contains("code="), target.contains("error=")) {
            (_, true) => ("200 OK", SIGN_IN_FAILED_PAGE),
            (true, false) => ("200 OK", SIGNED_IN_PAGE),
            (false, false) => ("404 Not Found", ""),
        };
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{page}",
            page.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
        if !page.is_empty() {
            return Ok(target);
        }
    }
}

/// Open `url` in the person's browser; false where there is none to open (SSH without a display) or opening failed.
fn open_browser(url: &str) -> bool {
    let has_display = |name: &str| std::env::var_os(name).is_some_and(|value| !value.is_empty());
    let (program, arguments): (&str, Vec<&str>) = match std::env::consts::OS {
        "macos" if !has_display("SSH_CONNECTION") => ("open", vec![url]),
        "windows" => ("rundll32", vec!["url.dll,FileProtocolHandler", url]),
        "linux" | "freebsd" | "openbsd" | "netbsd"
            if has_display("DISPLAY") || has_display("WAYLAND_DISPLAY") =>
        {
            ("xdg-open", vec![url])
        }
        _ => return false,
    };
    std::process::Command::new(program)
        .args(arguments)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// Tell the person signing in what to do, on stderr so stdout stays the command's output.
fn say(text: &str) {
    let _ = writeln!(std::io::stderr(), "{text}");
}
