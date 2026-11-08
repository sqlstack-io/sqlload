//! Connect to Postgres.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio_postgres::config::SslMode as ClientSslMode;
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{Client, Config};

use crate::postgres_source::_helpers::rustls_connect::RustlsConnect;
use crate::postgres_source::_helpers::server_reason::server_reason;
use crate::postgres_source::_helpers::tls::{certificate_check, client_config};
use crate::postgres_source::constants::SESSION_OPTIONS;
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::{Encryption, PostgresAddress, SslMode};
use crate::ssh_tunnel::main::open_channel::open_channel;
use crate::ssh_tunnel::models::TunnelStream;

/// An open client, and what protects it; the connection runs on its own task until the client is dropped. As in libpq, `prefer` connects in plain text when the server refuses TLS or the handshake fails, and a Unix socket never uses TLS. Through a bastion, TLS still runs end to end with the database and `verify-full` checks the database's own host name.
pub async fn connect_postgres(
    address: PostgresAddress<'_>,
) -> Result<(Client, Encryption), PostgresError> {
    let tls = RustlsConnect {
        config: client_config(&address.tls).map_err(|reason| PostgresError::Tls {
            address: address.shown(),
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
    let connected = attempt(&address, mode, tls.clone()).await;
    let fall_back = mode == ClientSslMode::Prefer && tls.refused.load(Ordering::Relaxed);
    let client = match (connected, fall_back) {
        (Err(_), true) => attempt(&address, ClientSslMode::Disable, tls.clone()).await,
        (connected, _) => connected,
    }?;
    let encryption = match tls.encrypted.load(Ordering::Relaxed) {
        true => certificate_check(&address.tls),
        false => Encryption::None,
    };
    Ok((client, encryption))
}

/// One try at a client for `address` with TLS mode `mode`, directly or through its bastion; its connection runs on its own task, which logs how it ended if it failed.
async fn attempt(
    address: &PostgresAddress<'_>,
    mode: ClientSslMode,
    mut tls: RustlsConnect,
) -> Result<Client, PostgresError> {
    let settings = client_settings(address, mode);
    let failed = |source| PostgresError::Connect {
        address: address.shown(),
        source,
    };
    type Connection = Pin<Box<dyn Future<Output = Result<(), tokio_postgres::Error>> + Send>>;
    let (client, connection): (Client, Connection) = match address.ssh {
        None => {
            let (client, connection) = settings.connect(tls).await.map_err(failed)?;
            (client, Box::pin(connection))
        }
        Some(tunnel) => {
            let stream = open_channel(tunnel, address.host, address.port).await?;
            let handshake =
                MakeTlsConnect::<TunnelStream>::make_tls_connect(&mut tls, address.host).map_err(
                    |error| PostgresError::Tls {
                        address: address.shown(),
                        reason: error.to_string(),
                    },
                )?;
            let (client, connection) = settings
                .connect_raw(stream, handshake)
                .await
                .map_err(failed)?;
            (client, Box::pin(connection))
        }
    };
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(
                reason = %server_reason(&error),
                "Postgres connection closed with an error"
            );
        }
    });
    Ok(client)
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
