use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use crate::config::models::AuthConfig;

use crate::sql_api::models::SqlClient;
use crate::stage_api::models::DriverSession;
use crate::store_paths::models::Location;

/// An authenticated Snowflake account, sharing one HTTP pool.
#[derive(Debug, Clone)]
pub struct SnowflakeDestination {
    pub sql: SqlClient,
    pub driver: Arc<DriverSession>,
    pub http: reqwest::Client,
    pub database: String,
    pub stage: LoadStage,
    /// Stages already created or checked in this run, by reference.
    pub ready_stages: Arc<Mutex<BTreeSet<String>>>,
}

/// Where loads stage their files.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum LoadStage {
    /// The user stage `@~`.
    #[default]
    Internal,
    External(ExternalStage),
}

/// An S3 external stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalStage {
    /// The stage URL as a folder, with the region and endpoint SQLLoad writes through.
    pub location: Location,
    pub object: StageObject,
}

/// Which Snowflake stage object reads the URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageObject {
    /// `_SQLLOAD_STAGE` in each loader schema, created with this storage integration.
    Managed { integration: String },
    /// An existing stage, `DATABASE.SCHEMA.STAGE`.
    Existing { name: String },
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
    pub stage: LoadStage,
}
