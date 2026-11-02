use crate::auth::main::account_url::account_url;
use crate::auth::models::Authenticator;
use crate::config::models::DestinationConfig;
use crate::http::errors::ApiError;
use crate::http::main::check_response::check_response;
use crate::sql_api::_helpers::results::{parse_columns, parse_rows};
use crate::sql_api::constants::{ASYNC_ACCEPTED, STATEMENT_TIMEOUT_SECS};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;

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
        self.column(name)
            .and_then(|index| row.get(index))
            .and_then(|value| value.as_deref())
    }
}

/// A client for `/api/v2/statements` on the shared HTTP pool.
#[derive(Debug, Clone)]
pub struct SqlClient {
    http: reqwest::Client,
    base_url: String,
    auth: Arc<Authenticator>,
    destination: DestinationConfig,
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

    /// Run one statement in the configured database, schema, warehouse and role.
    pub async fn execute(&self, statement: &str) -> Result<SqlRows, ApiError> {
        let mut body = json!({
            "statement": statement,
            "timeout": STATEMENT_TIMEOUT_SECS,
            "database": self.destination.database,
            "schema": self.destination.schema,
        });
        if let Some(warehouse) = &self.destination.warehouse {
            body["warehouse"] = json!(warehouse);
        }
        if let Some(role) = &self.destination.role {
            body["role"] = json!(role);
        }
        let response = self
            .request(reqwest::Method::POST, "/api/v2/statements")?
            .json(&body)
            .send()
            .await?;
        let mut result = check_response(response).await?;
        loop {
            let status = result.status().as_u16();
            let value: Value = result.json().await?;
            let handle = value["statementHandle"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            if status != ASYNC_ACCEPTED {
                return self.collect(&value, &handle).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
            let path = format!("/api/v2/statements/{handle}");
            result =
                check_response(self.request(reqwest::Method::GET, &path)?.send().await?).await?;
        }
    }

    async fn collect(&self, first: &Value, handle: &str) -> Result<SqlRows, ApiError> {
        let columns = parse_columns(first);
        let mut rows = parse_rows(first);
        let partitions = first["resultSetMetaData"]["partitionInfo"]
            .as_array()
            .map_or(1, Vec::len);
        for partition in 1..partitions {
            let path = format!("/api/v2/statements/{handle}?partition={partition}");
            let response =
                check_response(self.request(reqwest::Method::GET, &path)?.send().await?).await?;
            let page: Value = response.json().await?;
            rows.extend(parse_rows(&page));
        }
        Ok(SqlRows { columns, rows })
    }

    fn request(
        &self,
        method: reqwest::Method,
        path: &str,
    ) -> Result<reqwest::RequestBuilder, ApiError> {
        let bearer = self.auth.bearer()?;
        Ok(self
            .http
            .request(method, format!("{}{path}", self.base_url))
            .bearer_auth(bearer.token)
            .header("X-Snowflake-Authorization-Token-Type", bearer.token_type)
            .header("Accept", "application/json"))
    }
}
