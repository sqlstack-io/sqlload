//! Wait for statements.

use std::time::Instant;

use crate::fake_snowflake::models::FakeSnowflake;
use crate::harness::constants::{POLL, WAIT};

/// Wait until at least `count` SQL statements containing `text` have arrived; returns how many did.
pub async fn wait_for_statements(
    fake: &FakeSnowflake,
    text: &str,
    count: usize,
) -> Result<usize, String> {
    let started = Instant::now();
    let matching = || {
        fake.statements()
            .iter()
            .filter(|statement| statement.contains(text))
            .count()
    };
    while matching() < count {
        if started.elapsed() > WAIT {
            return Err(format!("{} of {count} statements with {text}", matching()));
        }
        tokio::time::sleep(POLL).await;
    }
    Ok(matching())
}
