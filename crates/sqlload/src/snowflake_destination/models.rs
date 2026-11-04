use std::sync::Arc;

use crate::config::models::AuthConfig;

use crate::sql_api::models::SqlClient;
use crate::stage_api::models::DriverSession;

/// An authenticated Snowflake account, sharing one HTTP pool.
#[derive(Debug, Clone)]
pub struct SnowflakeDestination {
    pub sql: SqlClient,
    pub driver: Arc<DriverSession>,
    pub http: reqwest::Client,
    pub database: String,
}

/// One connection's settings.
#[derive(Debug, Clone)]
pub struct SnowflakeAccount {
    pub account: String,
    pub user: String,
    pub auth: AuthConfig,
    pub role: Option<String>,
    pub warehouse: Option<String>,
    pub database: String,
    pub base_url: Option<String>,
}
