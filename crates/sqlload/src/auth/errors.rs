//! Why a token request failed.

use crate::http::errors::ApiError;

/// Why a token request failed: the server refused the grant (a revoked or expired refresh token), or the request itself failed.
#[derive(Debug)]
pub(crate) enum TokenError {
    Refused(String),
    Failed(ApiError),
}
