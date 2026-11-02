use std::sync::Mutex;

use crate::auth::_helpers::jwt::{KeyPair, load_key_pair, sign_jwt};
use crate::auth::constants::{
    JWT_LIFETIME_SECS, JWT_REFRESH_MARGIN_SECS, KEYPAIR_JWT, PROGRAMMATIC_ACCESS_TOKEN,
};
use crate::config::models::{AuthConfig, DestinationConfig};
use crate::http::errors::ApiError;
use crate::time_text::main::now_ms::now_ms;

/// A credential that never appears in logs or debug output.
#[derive(Clone, PartialEq, Eq)]
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

#[derive(Debug)]
enum Method {
    Pat(Secret),
    KeyPair {
        key: Box<KeyPair>,
        cached: Mutex<Option<(Secret, u64)>>,
    },
}

/// Produces bearer and login tokens from a programmatic access token or a key pair.
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
            AuthConfig::Pat { token } => Method::Pat(Secret(token.clone())),
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

    /// The bearer token for REST endpoints.
    pub fn bearer(&self) -> Result<Bearer, ApiError> {
        match &self.method {
            Method::Pat(token) => Ok(Bearer {
                token: token.expose().to_owned(),
                token_type: PROGRAMMATIC_ACCESS_TOKEN,
            }),
            Method::KeyPair { key, cached } => Ok(Bearer {
                token: self.cached_jwt(key, cached)?,
                token_type: KEYPAIR_JWT,
            }),
        }
    }

    /// The token for a driver-protocol login.
    pub fn login_token(&self) -> Result<LoginToken, ApiError> {
        match &self.method {
            Method::Pat(token) => Ok(LoginToken {
                authenticator: PROGRAMMATIC_ACCESS_TOKEN,
                token: token.expose().to_owned(),
            }),
            Method::KeyPair { key, cached } => Ok(LoginToken {
                authenticator: "SNOWFLAKE_JWT",
                token: self.cached_jwt(key, cached)?,
            }),
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
