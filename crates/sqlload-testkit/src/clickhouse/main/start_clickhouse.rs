//! Start ClickHouse.

use std::time::Duration;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::clickhouse::constants::{
    HTTP_PORT, IMAGE, PASSWORD, READY_ATTEMPTS, READY_RETRY_DELAY, TAG, TAG_VARIABLE, USER,
};
use crate::clickhouse::main::run_client::run_client;
use crate::clickhouse::models::ClickHouse;

/// A ClickHouse server (`TAG`, or the tag `TAG_VARIABLE` names) with a password user, its HTTP interface on a free host port, once it answers queries.
pub async fn start_clickhouse() -> Result<ClickHouse, String> {
    let tag = match std::env::var(TAG_VARIABLE) {
        Ok(tag) if !tag.is_empty() => tag,
        Ok(_) | Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => {
            TAG.to_owned()
        }
    };
    let container = GenericImage::new(IMAGE.to_owned(), tag)
        .with_exposed_port(HTTP_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Logging errors to"))
        .with_env_var("CLICKHOUSE_USER", USER)
        .with_env_var("CLICKHOUSE_PASSWORD", PASSWORD)
        .with_env_var("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1")
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .map_err(|error| format!("cannot start ClickHouse: {error}"))?;
    let port = container
        .get_host_port_ipv4(HTTP_PORT.tcp())
        .await
        .map_err(|error| error.to_string())?;
    let clickhouse = ClickHouse { port, container };
    let mut last_error = String::new();
    for _ in 0..READY_ATTEMPTS {
        match run_client(&clickhouse, "SELECT 1", &[]).await {
            Ok(_) => return Ok(clickhouse),
            Err(error) => last_error = error,
        }
        tokio::time::sleep(READY_RETRY_DELAY).await;
    }
    Err(format!("ClickHouse never accepted queries: {last_error}"))
}
