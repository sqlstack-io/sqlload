use std::time::Duration;

use crate::delivery::models::Warehouse;
use crate::http::models::{ConnectionCounter, InFlight};

/// One HTTP pool and the destination's clients built on it.
#[derive(Debug, Clone)]
pub struct Clients {
    pub http: reqwest::Client,
    pub connections: ConnectionCounter,
    pub in_flight: InFlight,
    pub warehouse: Warehouse,
}

/// What a run did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub streams: Vec<String>,
    pub rows: u64,
    pub connections_opened: u64,
    pub elapsed: Duration,
}
