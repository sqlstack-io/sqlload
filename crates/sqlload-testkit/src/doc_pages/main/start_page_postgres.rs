//! Start the Postgres server a docs page starts with `docker run`.

use std::time::Duration;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::doc_pages::constants::{CONNECT_ATTEMPTS, PAGE_POSTGRES_IMAGE};
use crate::doc_pages::models::PagePostgres;

/// A Postgres server whose `postgres` user has `password`, as the page's `docker run` starts it, and a client of it.
pub async fn start_page_postgres(password: &str) -> Result<PagePostgres, String> {
    let (image, tag) = PAGE_POSTGRES_IMAGE;
    let container = GenericImage::new(image, tag)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", password)
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .map_err(|error| format!("cannot start Postgres: {error}"))?;
    let port = container
        .get_host_port_ipv4(5432.tcp())
        .await
        .map_err(|error| error.to_string())?;
    let address =
        format!("host=127.0.0.1 port={port} user=postgres password={password} dbname=postgres");
    let mut last_error = String::new();
    for _ in 0..CONNECT_ATTEMPTS {
        match tokio_postgres::connect(&address, tokio_postgres::NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(connection);
                return Ok(PagePostgres {
                    container,
                    client,
                    port,
                });
            }
            Err(error) => last_error = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("cannot connect to Postgres: {last_error}"))
}
