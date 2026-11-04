use std::sync::Arc;

use crate::auth::main::account_url::account_url;
use crate::auth::models::Authenticator;
use crate::config::models::DestinationConfig;
use crate::sql_api::constants::STATEMENT_TIMEOUT_SECS;

/// A statement result: column names and every row as text.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SqlRows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

impl SqlRows {
    /// The index of a column, compared case-insensitively.
    pub fn column(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|column| column.eq_ignore_ascii_case(name))
    }

    /// The value of `name` in `row`.
    pub fn value<'a>(&self, row: &'a [Option<String>], name: &str) -> Option<&'a str> {
        let index = self.column(name)?;
        row.get(index)?.as_deref()
    }
}

/// What `/api/v2/statements` needs: the shared HTTP pool, credentials and the session context.
#[derive(Debug, Clone)]
pub struct SqlClient {
    pub(crate) http: reqwest::Client,
    pub(crate) base_url: String,
    pub(crate) auth: Arc<Authenticator>,
    pub(crate) destination: DestinationConfig,
    /// Run sessions in UTC, so `TIMESTAMP_TZ` values are stored with a `+00:00` offset.
    pub(crate) utc_sessions: bool,
    /// Seconds a statement may run; 0 is Snowflake's maximum.
    pub(crate) timeout_secs: u64,
}

impl SqlClient {
    pub fn new(
        http: reqwest::Client,
        auth: Arc<Authenticator>,
        destination: &DestinationConfig,
    ) -> Self {
        Self {
            http,
            base_url: account_url(destination),
            auth,
            destination: destination.clone(),
            utc_sessions: false,
            timeout_secs: STATEMENT_TIMEOUT_SECS,
        }
    }

    /// The same client letting statements run for `seconds` (0 is Snowflake's maximum).
    pub fn with_timeout(self, seconds: u64) -> Self {
        Self {
            timeout_secs: seconds,
            ..self
        }
    }

    /// The same client with every session in UTC.
    pub fn in_utc(self) -> Self {
        Self {
            utc_sessions: true,
            ..self
        }
    }
}
