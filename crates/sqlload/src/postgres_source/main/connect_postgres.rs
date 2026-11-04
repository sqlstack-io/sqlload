//! Connect to Postgres.

use tokio_postgres::{Client, Config, NoTls};

use crate::postgres_source::constants::SESSION_OPTIONS;
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::PostgresAddress;

/// An open client; the connection runs on its own task until the client is dropped.
pub async fn connect_postgres(address: PostgresAddress<'_>) -> Result<Client, PostgresError> {
    let mut config = Config::new();
    config
        .host(address.host)
        .port(address.port)
        .user(address.user)
        .dbname(address.database)
        .application_name("sqlload")
        .options(SESSION_OPTIONS);
    if let Some(password) = address.password {
        config.password(password);
    }
    let (client, connection) =
        config
            .connect(NoTls)
            .await
            .map_err(|source| PostgresError::Connect {
                address: format!("{}:{}/{}", address.host, address.port, address.database),
                source,
            })?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "Postgres connection closed with an error");
        }
    });
    Ok(client)
}
