//! Connect to the test server, waiting for it to accept connections.

use sqlload::postgres_source::main::connect_postgres::connect_postgres;
use sqlload::postgres_source::models::PostgresAddress;
use tokio_postgres::Client;

use crate::postgres::constants::{CONNECT_ATTEMPTS, CONNECT_RETRY_DELAY, DATABASE, PASSWORD, USER};
use crate::postgres::models::Postgres;

/// A client for `postgres`, retried until the server is ready.
pub async fn connect(postgres: &Postgres) -> Result<Client, String> {
    let mut last_error = String::new();
    for _ in 0..CONNECT_ATTEMPTS {
        let address = PostgresAddress {
            host: &postgres.host,
            port: postgres.port,
            user: USER,
            password: Some(PASSWORD),
            database: DATABASE,
        };
        match connect_postgres(address).await {
            Ok(client) => return Ok(client),
            Err(error) => last_error = error.to_string(),
        }
        tokio::time::sleep(CONNECT_RETRY_DELAY).await;
    }
    Err(last_error)
}
