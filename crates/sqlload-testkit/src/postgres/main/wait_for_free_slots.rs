//! Wait for the replication slots of a load that stopped to be released.

use tokio_postgres::Client;

use crate::postgres::constants::{SLOT_ATTEMPTS, SLOT_RETRY_DELAY};

/// Wait until no replication slot on `client`'s server is active. Postgres ends the walsender of a load that stopped mid-run a moment after the load exits, and a load started before then fails, finding its slot "active for PID …".
pub async fn wait_for_free_slots(client: &Client) -> Result<(), String> {
    for _ in 0..SLOT_ATTEMPTS {
        let active: i64 = client
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE active",
                &[],
            )
            .await
            .map_err(|error| error.to_string())?
            .get(0);
        if active == 0 {
            return Ok(());
        }
        tokio::time::sleep(SLOT_RETRY_DELAY).await;
    }
    Err("a replication slot was still active a minute after its load stopped".to_owned())
}
