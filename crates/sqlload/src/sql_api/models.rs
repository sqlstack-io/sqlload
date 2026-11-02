use std::sync::Arc;

use crate::auth::main::account_url::account_url;
use crate::auth::models::Authenticator;
use crate::config::models::DestinationConfig;

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
        }
    }
}
