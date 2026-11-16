//! Hold a replication slot from inside the Postgres container, as a load that stopped leaves it held.

use testcontainers::core::ExecCommand;
use tokio_postgres::Client;

use crate::postgres::constants::{DATABASE, SLOT_ATTEMPTS, SLOT_RETRY_DELAY, USER};
use crate::postgres::models::Postgres;

/// Stream logical slot `slot` (publication `publication`) with `pg_recvlogical` inside the container for `seconds`, then stop it; returns once `client` sees `pg_recvlogical` holding it, so a load started next finds it held by that session rather than by a load still ending.
pub async fn hold_slot(
    postgres: &Postgres,
    client: &Client,
    (slot, publication): (&str, &str),
    seconds: u64,
) -> Result<(), String> {
    let script = format!(
        "pg_recvlogical -U {USER} -d {DATABASE} --slot {slot} --start -o proto_version=1 -o publication_names={publication} -f /dev/null & sleep {seconds}; kill $!"
    );
    postgres
        .container
        .exec(ExecCommand::new(["sh", "-c", script.as_str()]))
        .await
        .map_err(|error| error.to_string())?;
    for _ in 0..SLOT_ATTEMPTS {
        let active: bool = client
            .query_opt(
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots AS s JOIN pg_stat_activity AS a ON a.pid = s.active_pid WHERE s.slot_name = $1 AND a.application_name = 'pg_recvlogical')",
                &[&slot],
            )
            .await
            .map_err(|error| error.to_string())?
            .is_some_and(|row| row.get(0));
        if active {
            return Ok(());
        }
        tokio::time::sleep(SLOT_RETRY_DELAY).await;
    }
    Err(format!("pg_recvlogical never held slot {slot}"))
}
