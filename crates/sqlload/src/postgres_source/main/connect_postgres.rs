//! Connect to Postgres.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio_postgres::config::SslMode as ClientSslMode;
use tokio_postgres::{Client, Config};

use crate::postgres_source::_helpers::rustls_connect::RustlsConnect;
use crate::postgres_source::_helpers::server_reason::server_reason;
use crate::postgres_source::_helpers::tls::{certificate_check, client_config};
use crate::postgres_source::constants::SESSION_OPTIONS;
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::{Encryption, PostgresAddress, SslMode};

/// An open client, and what protects it; the connection runs on its own task until the client is dropped. As in libpq, `prefer` connects in plain text when the server refuses TLS or the handshake fails, and a Unix socket never uses TLS.
pub async fn connect_postgres(
    address: PostgresAddress<'_>,
) -> Result<(Client, Encryption), PostgresError> {
    let shown = format!("{}:{}/{}", address.host, address.port, address.database);
    let tls = RustlsConnect {
        config: client_config(&address.tls).map_err(|reason| PostgresError::Tls {
            address: shown.clone(),
            reason,
        })?,
        encrypted: Arc::new(AtomicBool::new(false)),
        refused: Arc::new(AtomicBool::new(false)),
    };
    let mode = match (address.tls.mode, address.host.starts_with('/')) {
        (SslMode::Disable, _) | (_, true) => ClientSslMode::Disable,
        (SslMode::Prefer, false) => ClientSslMode::Prefer,
        (SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull, false) => {
            ClientSslMode::Require
        }
    };
    let connected = client_settings(&address, mode).connect(tls.clone()).await;
    let fall_back = mode == ClientSslMode::Prefer && tls.refused.load(Ordering::Relaxed);
    let connected = match (connected, fall_back) {
        (Err(_), true) => {
            client_settings(&address, ClientSslMode::Disable)
                .connect(tls.clone())
                .await
        }
        (connected, _) => connected,
    };
    let (client, connection) = connected.map_err(|source| PostgresError::Connect {
        address: shown,
        source,
    })?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(
                reason = %server_reason(&error),
                "Postgres connection closed with an error"
            );
        }
    });
    let encryption = match tls.encrypted.load(Ordering::Relaxed) {
        true => certificate_check(&address.tls),
        false => Encryption::None,
    };
    Ok((client, encryption))
}

/// The client's settings for `address`, with TLS mode `mode`.
fn client_settings(address: &PostgresAddress<'_>, mode: ClientSslMode) -> Config {
    let mut config = Config::new();
    config
        .host(address.host)
        .port(address.port)
        .user(address.user)
        .dbname(address.database)
        .application_name("sqlload")
        .options(SESSION_OPTIONS)
        .ssl_mode(mode);
    if let Some(password) = address.password {
        config.password(password);
    }
    config
}
