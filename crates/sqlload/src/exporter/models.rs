use std::sync::Arc;
use std::time::Duration;

use crate::auth::models::Authenticator;
use crate::http::models::{ConnectionCounter, InFlight};
use crate::sql_api::models::SqlClient;
use crate::stage_api::models::DriverSession;
use crate::streaming_api::models::StreamingClient;

/// One HTTP pool and the Snowflake clients built on it.
#[derive(Debug, Clone)]
pub struct Clients {
    pub http: reqwest::Client,
    pub connections: ConnectionCounter,
    pub in_flight: InFlight,
    pub auth: Arc<Authenticator>,
    pub sql: SqlClient,
    pub streaming: Arc<StreamingClient>,
    pub driver: Arc<DriverSession>,
    pub account_url: String,
}

/// What a run did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub streams: Vec<String>,
    pub rows: u64,
    pub connections_opened: u64,
    pub elapsed: Duration,
}
