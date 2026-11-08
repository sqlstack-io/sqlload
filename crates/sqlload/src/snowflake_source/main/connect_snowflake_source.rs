//! Open a Snowflake source.

use std::sync::Arc;

use crate::auth::models::Authenticator;
use crate::config::models::DestinationConfig;
use crate::http::errors::ApiError;
use crate::http::main::http_client::build_http_client;
use crate::http::models::ConnectionCounter;
use crate::project::models::UnloadStage;
use crate::snowflake_source::models::SnowflakeReader;
use crate::sql_api::models::SqlClient;
use crate::stage_api::models::DriverSession;

/// Clients for the account, database, role and warehouse in `config`, unloading to `stage` (the user stage when `None`); nothing is sent until the first statement. Sessions run in UTC and statements may run as long as Snowflake allows.
pub fn connect_snowflake_source(
    config: DestinationConfig,
    stage: Option<UnloadStage>,
) -> Result<SnowflakeReader, ApiError> {
    let http = build_http_client(&ConnectionCounter::default())?;
    let auth = Arc::new(Authenticator::from_config(&config)?);
    Ok(SnowflakeReader {
        sql: SqlClient::new(http.clone(), auth.clone(), &config)
            .in_utc()
            .with_timeout(0),
        driver: DriverSession::new(http.clone(), auth, &config),
        http,
        stage,
    })
}
