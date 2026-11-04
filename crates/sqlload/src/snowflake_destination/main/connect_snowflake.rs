//! Authenticate to Snowflake for loading.

use std::sync::Arc;

use crate::auth::models::Authenticator;
use crate::config::models::{DestinationConfig, DestinationKind};
use crate::http::errors::ApiError;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::snowflake_destination::constants::SESSION_SCHEMA;
use crate::snowflake_destination::models::{SnowflakeAccount, SnowflakeDestination};
use crate::sql_api::models::SqlClient;
use crate::stage_api::models::DriverSession;

/// Clients for `account`; nothing is sent until the first statement.
pub fn connect_snowflake(account: SnowflakeAccount) -> Result<SnowflakeDestination, ApiError> {
    let destination = DestinationConfig {
        kind: DestinationKind::Snowflake,
        account: account.account,
        user: account.user,
        auth: account.auth,
        role: account.role,
        warehouse: account.warehouse,
        database: account.database.clone(),
        schema: SESSION_SCHEMA.to_owned(),
        base_url: account.base_url,
    };
    let http = build_http_client(&ConnectionCounter::default())?;
    let auth = Arc::new(Authenticator::from_config(&destination)?);
    Ok(SnowflakeDestination {
        sql: SqlClient::new(http.clone(), auth.clone(), &destination).in_utc(),
        driver: Arc::new(DriverSession::new(http.clone(), auth, &destination)),
        http,
        database: account.database,
    })
}
