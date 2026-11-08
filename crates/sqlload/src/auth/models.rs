use std::path::PathBuf;
use std::sync::Mutex;

use crate::auth::_helpers::jwt::{KeyPair, load_key_pair, sign_jwt};
use crate::auth::_helpers::oauth::fresh_access_token;
use crate::auth::_helpers::sign_in::terminal_io;
use crate::auth::_helpers::token_cache::token_cache_file;
use crate::auth::constants::{
    JWT_LIFETIME_SECS, JWT_REFRESH_MARGIN_SECS, KEYPAIR_JWT, LOCAL_APPLICATION, OAUTH,
    PROGRAMMATIC_ACCESS_TOKEN,
};
use crate::auth::main::account_url::account_url;
use crate::config::models::{AuthConfig, DestinationConfig};
use crate::http::errors::ApiError;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::time_text::main::now_ms::now_ms;

/// A credential that never appears in logs or debug output.
#[derive(Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(transparent)]
pub struct Secret(pub String);

impl Secret {
    /// The secret value, for sending to the service that issued it.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("<redacted>")
    }
}

/// A bearer token and the `X-Snowflake-Authorization-Token-Type` it needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bearer {
    pub token: String,
    pub token_type: &'static str,
}

/// The `AUTHENTICATOR` and `TOKEN` fields of a driver-protocol login.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginToken {
    pub authenticator: &'static str,
    pub token: String,
}

/// A Snowflake OAuth client that signs a user in through the browser, and where its tokens are cached.
#[derive(Debug, Clone)]
pub struct OauthClient {
    pub account_url: String,
    pub client_id: String,
    /// Sent with token requests; `None` for a public client such as `LOCAL_APPLICATION`.
    pub client_secret: Option<Secret>,
    /// The redirect URI a custom integration registered, on `http://127.0.0.1`; `None` takes a free port.
    pub redirect_uri: Option<String>,
    pub user: String,
    pub role: Option<String>,
    pub cache_file: PathBuf,
    pub http: reqwest::Client,
}

/// A browser sign-in's tokens in one run, and why signing in failed, after which the run asks nobody again.
#[derive(Debug, Default)]
pub struct OauthState {
    pub tokens: Option<OauthTokens>,
    pub failure: Option<String>,
}

/// An access token with when to renew it, and the refresh token that renews it with when that expires (Unix seconds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OauthTokens {
    pub access_token: Secret,
    pub renew_after: u64,
    pub refresh_token: Option<Secret>,
    pub refresh_expires_at: Option<u64>,
}

/// One cached sign-in, as the token cache file holds it.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct CachedTokens {
    pub(crate) access_token: String,
    pub(crate) renew_after: u64,
    pub(crate) refresh_token: Option<String>,
    pub(crate) refresh_expires_at: Option<u64>,
}

#[derive(Debug)]
enum Method {
    Pat(Secret),
    KeyPair {
        key: Box<KeyPair>,
        cached: Mutex<Option<(Secret, u64)>>,
    },
    Oauth {
        client: Box<OauthClient>,
        tokens: tokio::sync::Mutex<OauthState>,
    },
}

/// Produces bearer and login tokens from a programmatic access token, a key pair, or a browser sign-in.
#[derive(Debug)]
pub struct Authenticator {
    qualified_user: String,
    method: Method,
}

impl Authenticator {
    pub fn from_config(destination: &DestinationConfig) -> Result<Self, ApiError> {
        let account = destination
            .account
            .split('.')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        let qualified_user = format!("{account}.{}", destination.user.to_ascii_uppercase());
        let method = match &destination.auth {
            AuthConfig::Pat { token } => Method::Pat(token.clone()),
            AuthConfig::KeyPair { private_key_file } => {
                let pem = std::fs::read_to_string(private_key_file).map_err(|error| {
                    ApiError::Auth(format!(
                        "cannot read {}: {error}",
                        private_key_file.display()
                    ))
                })?;
                Method::KeyPair {
                    key: Box::new(load_key_pair(&pem).map_err(ApiError::Auth)?),
                    cached: Mutex::new(None),
                }
            }
            AuthConfig::OauthAuthorizationCode {
                client_id,
                client_secret,
                redirect_uri,
            } => Method::Oauth {
                client: Box::new(OauthClient {
                    account_url: account_url(destination),
                    client_id: client_id
                        .clone()
                        .unwrap_or_else(|| LOCAL_APPLICATION.to_owned()),
                    client_secret: client_secret.clone(),
                    redirect_uri: redirect_uri.clone(),
                    user: destination.user.clone(),
                    role: destination.role.clone(),
                    cache_file: token_cache_file().ok_or_else(|| {
                        ApiError::Auth(
                            "no folder to cache the Snowflake sign-in in: set XDG_CACHE_HOME or HOME"
                                .to_owned(),
                        )
                    })?,
                    http: build_http_client(&ConnectionCounter::default())?,
                }),
                tokens: tokio::sync::Mutex::new(OauthState::default()),
            },
        };
        Ok(Self {
            qualified_user,
            method,
        })
    }

    /// True when a key pair is configured; Snowpipe `insertFiles` accepts only key-pair JWTs.
    pub fn uses_key_pair(&self) -> bool {
        matches!(self.method, Method::KeyPair { .. })
    }

    /// The bearer token for REST endpoints; a browser sign-in signs in or refreshes first when its token is missing or old.
    pub async fn bearer(&self) -> Result<Bearer, ApiError> {
        match &self.method {
            Method::Pat(token) => Ok(Bearer {
                token: token.expose().to_owned(),
                token_type: PROGRAMMATIC_ACCESS_TOKEN,
            }),
            Method::KeyPair { key, cached } => Ok(Bearer {
                token: self.cached_jwt(key, cached)?,
                token_type: KEYPAIR_JWT,
            }),
            Method::Oauth { client, tokens } => Ok(Bearer {
                token: fresh_access_token(client, tokens, &terminal_io()).await?,
                token_type: OAUTH,
            }),
        }
    }

    /// The token for a driver-protocol login.
    pub async fn login_token(&self) -> Result<LoginToken, ApiError> {
        match &self.method {
            Method::Pat(token) => Ok(LoginToken {
                authenticator: PROGRAMMATIC_ACCESS_TOKEN,
                token: token.expose().to_owned(),
            }),
            Method::KeyPair { key, cached } => Ok(LoginToken {
                authenticator: "SNOWFLAKE_JWT",
                token: self.cached_jwt(key, cached)?,
            }),
            Method::Oauth { client, tokens } => Ok(LoginToken {
                authenticator: OAUTH,
                token: fresh_access_token(client, tokens, &terminal_io()).await?,
            }),
        }
    }

    /// How this authenticator signs in, for `sqlload debug`: a browser sign-in names its token cache.
    pub fn describe(&self) -> String {
        match &self.method {
            Method::Pat(_) => "a programmatic access token".to_owned(),
            Method::KeyPair { .. } => "a key pair".to_owned(),
            Method::Oauth { client, .. } => format!(
                "browser sign-in (OAuth client {}), tokens cached in {}",
                client.client_id,
                client.cache_file.display()
            ),
        }
    }

    fn cached_jwt(
        &self,
        key: &KeyPair,
        cached: &Mutex<Option<(Secret, u64)>>,
    ) -> Result<String, ApiError> {
        let now = (now_ms() / 1000) as u64;
        let mut guard = cached
            .lock()
            .map_err(|_| ApiError::Auth("JWT cache lock poisoned".to_owned()))?;
        if let Some((token, expires)) = guard.as_ref()
            && *expires > now + JWT_REFRESH_MARGIN_SECS
        {
            return Ok(token.expose().to_owned());
        }
        let token =
            sign_jwt(key, &self.qualified_user, now, JWT_LIFETIME_SECS).map_err(ApiError::Auth)?;
        *guard = Some((Secret(token.clone()), now + JWT_LIFETIME_SECS));
        Ok(token)
    }
}
