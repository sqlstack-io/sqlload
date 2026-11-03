//! Wait for committed offsets.

use std::time::Instant;

use crate::harness::constants::{POLL, WAIT};
use crate::harness::models::Stack;
use crate::redpanda::main::committed_offsets::committed_offsets;

/// Wait until `group` has committed at least `records` records of `topic`; returns how many it has.
pub async fn wait_for_commits(
    stack: &Stack,
    group: &str,
    topic: &str,
    records: i64,
) -> Result<i64, String> {
    let started = Instant::now();
    loop {
        let committed = committed_offsets(&stack.redpanda.brokers, group, topic).await?;
        if committed >= records {
            return Ok(committed);
        }
        if started.elapsed() > WAIT {
            return Err(format!(
                "{group} committed {committed} of {records} records on {topic}"
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}
