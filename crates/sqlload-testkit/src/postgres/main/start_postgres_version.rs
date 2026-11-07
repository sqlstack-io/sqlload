//! Start Postgres of a chosen version.

use std::time::Duration;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::postgres::constants::{DATABASE, IMAGE, PASSWORD, USER};
use crate::postgres::models::Postgres;

/// A Postgres server of image tag `tag` (for example `15-alpine`) with logical replication enabled, on a free host port.
pub async fn start_postgres_version(tag: &str) -> Result<Postgres, String> {
    let container = GenericImage::new(IMAGE, tag)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", USER)
        .with_env_var("POSTGRES_PASSWORD", PASSWORD)
        .with_env_var("POSTGRES_DB", DATABASE)
        .with_cmd(["postgres", "-c", "wal_level=logical"])
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .map_err(|error| format!("cannot start Postgres: {error}"))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| error.to_string())?;
    Ok(Postgres {
        host: "127.0.0.1".to_owned(),
        port,
        container,
    })
}
