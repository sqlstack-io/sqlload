//! An access token for a Google connection, kept until shortly before it expires.

use crate::auth::models::Secret;
use crate::google_identity::_helpers::impersonate::impersonated_token;
use crate::google_identity::_helpers::signers::signer_token;
use crate::google_identity::constants::{CLOUD_PLATFORM_SCOPE, TOKEN_REFRESH_MARGIN_MS};
use crate::google_identity::models::GoogleCredentials;
use crate::http::errors::ApiError;
use crate::time_text::main::now_ms::now_ms;

/// A token with `scope` for `credentials`: the one `kept`, or a new one when it is missing, within two minutes of its expiry, or the one the server `refused` (a request refused with a token another has already replaced gets the replacement). An impersonated token is asked for with a fresh token of the identity signing in, so a load longer than the impersonated token's hour keeps going.
pub async fn google_access_token(
    http: &reqwest::Client,
    credentials: &GoogleCredentials,
    kept: &tokio::sync::Mutex<Option<(Secret, i64)>>,
    (scope, refused): (&str, Option<&Secret>),
) -> Result<Secret, ApiError> {
    let mut kept = kept.lock().await;
    if let Some((token, expires_ms)) = kept.as_ref()
        && refused.is_none_or(|refused| refused.expose() != token.expose())
        && now_ms() + TOKEN_REFRESH_MARGIN_MS < *expires_ms
    {
        return Ok(token.clone());
    }
    let now = now_ms();
    let (token, expires_ms) = match &credentials.impersonate {
        None => signer_token(http, &credentials.signer, scope, now).await?,
        Some(target) => {
            let (source, _) =
                signer_token(http, &credentials.signer, CLOUD_PLATFORM_SCOPE, now).await?;
            impersonated_token(http, target, (&source, scope), now).await?
        }
    };
    *kept = Some((token.clone(), expires_ms));
    Ok(token)
}
