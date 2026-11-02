//! Wait for rows.

use std::time::Instant;

use crate::fake_snowflake::models::FakeSnowflake;
use crate::harness::constants::{POLL, WAIT};

/// Wait until `table` holds at least `rows` committed rows.
pub async fn wait_for_rows(fake: &FakeSnowflake, table: &str, rows: usize) -> Result<(), String> {
    let started = Instant::now();
    while fake.rows(table).len() < rows {
        if started.elapsed() > WAIT {
            return Err(format!(
                "{table} has {} of {rows} rows",
                fake.rows(table).len()
            ));
        }
        tokio::time::sleep(POLL).await;
    }
    Ok(())
}
