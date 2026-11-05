use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::auth::models::Authenticator;
use crate::config::models::{AuthConfig, DestinationConfig};
use crate::store_paths::models::Location;

use crate::sql_api::models::SqlClient;
use crate::stage_api::models::DriverSession;

/// An authenticated Snowflake account, sharing one HTTP pool.
#[derive(Debug, Clone)]
pub struct SnowflakeDestination {
    pub sql: SqlClient,
    pub driver: Arc<DriverSession>,
    pub http: reqwest::Client,
    pub database: String,
    /// Held while a load's tables change the schema, one at a time.
    pub ddl: Arc<tokio::sync::Mutex<()>>,
    /// The route the connection's `stage` forces, if any.
    pub stage: Option<ForcedStage>,
    /// What this run's plan already found out about stages, by what was probed: usable (and whether `apply` must create it), or why not.
    pub stage_checks: Arc<Mutex<BTreeMap<String, Result<bool, String>>>>,
    /// What a streaming client for this account needs.
    pub auth: Arc<Authenticator>,
    pub config: DestinationConfig,
}

/// The route a connection's `stage` forces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForcedStage {
    User,
    Table,
    Streaming,
    Insert,
    Internal { name: String },
    External(ExternalStage),
}

/// An S3 external stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalStage {
    /// The stage URL as a folder, with the region and endpoint SQLLoad writes through.
    pub location: Location,
    pub object: StageObject,
    /// Keep staged files after their batch commits, and leave the stage out of the cleanup of old ones.
    pub keep_staged_files: bool,
}

/// Which Snowflake stage object reads the URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageObject {
    /// `_SQLLOAD_STAGE` in each loader schema, created with this storage integration.
    Managed { integration: String },
    /// An existing stage, `DATABASE.SCHEMA.STAGE`.
    Existing { name: String },
}

/// How one table's rows reach Snowflake, as its plan chose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageRoute {
    External(ExternalStage),
    Internal {
        name: String,
    },
    /// `@~`.
    User,
    /// The target's `@%table`: an owned direct append.
    TargetTable,
    /// The staging table's `@%table`, which the load always owns.
    StagingTable,
    /// Snowpipe Streaming into the staging table; `may_fall_back` (the chain chose it, nothing forced it) lets a channel that will not open drop to `INSERT`.
    Streaming {
        may_fall_back: bool,
    },
    /// Batched multi-row `INSERT` into the staging table.
    Insert,
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
    pub stage: Option<ForcedStage>,
}
