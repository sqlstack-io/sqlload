//! A client for a ClickHouse destination.

use std::sync::Arc;

use crate::adapters::clickhouse::constants::{DEFAULT_HTTP_PORT, DEFAULT_HTTPS_PORT};
use crate::adapters::clickhouse::models::ClickHouseDestination;
use crate::http::errors::ApiError;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::project::models::ConnectionConfig;

/// A client for a `clickhouse` connection, on HTTPS with `secure`; nothing is sent until the first statement.
pub(in crate::adapters) fn connect_clickhouse(
    connection: &ConnectionConfig,
) -> Result<ClickHouseDestination, ApiError> {
    let ConnectionConfig::ClickHouse {
        host,
        port,
        user,
        password,
        secure,
    } = connection
    else {
        return Err(ApiError::Protocol(format!(
            "a {} connection is not a ClickHouse one",
            connection.kind()
        )));
    };
    let (scheme, default_port) = match secure {
        true => ("https", DEFAULT_HTTPS_PORT),
        false => ("http", DEFAULT_HTTP_PORT),
    };
    Ok(ClickHouseDestination {
        http: build_http_client(&ConnectionCounter::default())?,
        url: format!("{scheme}://{host}:{}/", port.unwrap_or(default_port)),
        user: user.clone(),
        password: password.clone(),
        ddl: Arc::default(),
    })
}
