//! Start a broker and the fake.

use crate::fake_snowflake::main::start_fake_snowflake::start_fake_snowflake;
use crate::harness::models::Stack;
use crate::redpanda::main::start_redpanda::start_redpanda;

/// A fresh Redpanda broker and fake Snowflake.
pub async fn start_stack() -> Result<Stack, String> {
    Ok(Stack {
        redpanda: start_redpanda().await?,
        fake: start_fake_snowflake().await?,
    })
}
