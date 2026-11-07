//! Start Postgres.

use crate::postgres::constants::TAG;
use crate::postgres::main::start_postgres_version::start_postgres_version;
use crate::postgres::models::Postgres;

/// A Postgres server with logical replication enabled, on a free host port.
pub async fn start_postgres() -> Result<Postgres, String> {
    start_postgres_version(TAG).await
}
