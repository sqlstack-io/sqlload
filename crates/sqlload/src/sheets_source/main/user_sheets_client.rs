//! A Sheets API client reading as a Google user.

use crate::auth::models::GoogleUser;
use crate::sheets_source::constants::BASE_URL;
use crate::sheets_source::models::{SheetsClient, SheetsCredential};

/// A client reading as the Google user who consents to `user`'s OAuth client, against `base_url` (the public API when `None`); nothing is sent, and nobody asked to sign in, until the first read.
pub fn user_sheets_client(
    http: reqwest::Client,
    user: GoogleUser,
    base_url: Option<&str>,
) -> SheetsClient {
    SheetsClient {
        http,
        base_url: base_url
            .unwrap_or(BASE_URL)
            .trim_end_matches('/')
            .to_owned(),
        credential: SheetsCredential::User(Box::new(user)),
        token: std::sync::Arc::default(),
    }
}
