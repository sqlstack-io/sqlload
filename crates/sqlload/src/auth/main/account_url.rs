//! The account endpoint.

use crate::config::models::DestinationConfig;

/// `base_url` when configured, otherwise `https://<account>.snowflakecomputing.com` with `_` written as `-`.
pub fn account_url(destination: &DestinationConfig) -> String {
    match &destination.base_url {
        Some(url) => url.trim_end_matches('/').to_owned(),
        None => format!(
            "https://{}.snowflakecomputing.com",
            destination.account.to_ascii_lowercase().replace('_', "-")
        ),
    }
}
