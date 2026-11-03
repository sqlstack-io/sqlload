//! Wait for appends.

use std::time::Instant;

use crate::fake_snowflake::models::FakeSnowflake;
use crate::harness::constants::{POLL, WAIT};

/// Wait until the fake has accepted at least `appends` append requests.
pub async fn wait_for_appends(fake: &FakeSnowflake, appends: u64) -> Result<(), String> {
    let started = Instant::now();
    while fake.appends() < appends {
        if started.elapsed() > WAIT {
            return Err(format!("only {} appends", fake.appends()));
        }
        tokio::time::sleep(POLL).await;
    }
    Ok(())
}
