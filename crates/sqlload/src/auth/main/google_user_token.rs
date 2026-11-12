//! A Google user's access token, renewed, or from a consent in the browser when a terminal is there.

use crate::auth::_helpers::google_oauth::google_access_token;
use crate::auth::_helpers::sign_in::terminal_io;
use crate::auth::models::{GoogleUser, Secret};
use crate::http::errors::ApiError;

/// The access token to send as `user` now, other than `refused`: the one `held` while it has time left, else one renewed with the connection's or the cached refresh token, else one from a consent in the browser when a terminal is there to give it. A refresh token Google refuses is an error naming why and how to get a new one.
pub async fn google_user_token(
    http: &reqwest::Client,
    user: &GoogleUser,
    held: &tokio::sync::Mutex<Option<(Secret, i64)>>,
    refused: Option<&Secret>,
) -> Result<Secret, ApiError> {
    google_access_token(http, user, held, (refused, &terminal_io())).await
}
