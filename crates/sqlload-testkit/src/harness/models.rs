use sqlload::exporter::models::RunSummary;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::fake_snowflake::models::FakeSnowflake;
use crate::redpanda::models::Redpanda;

/// A broker and a fake warehouse.
#[derive(Debug)]
pub struct Stack {
    pub redpanda: Redpanda,
    pub fake: FakeSnowflake,
}

/// A running exporter and the token that stops it.
#[derive(Debug)]
pub struct Running {
    pub cancel: CancellationToken,
    pub task: JoinHandle<Result<RunSummary, String>>,
}
