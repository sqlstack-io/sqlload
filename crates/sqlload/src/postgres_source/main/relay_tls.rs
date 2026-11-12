//! TLS for a client that speaks only plain text: the replication client.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio_rustls::TlsConnector;

#[cfg(windows)]
use crate::postgres_source::_helpers::loopback_relay::loopback_relay;
use crate::postgres_source::_helpers::tls::client_config;
use crate::postgres_source::constants::SSL_REQUEST;
#[cfg(unix)]
use crate::postgres_source::constants::{APPLICATION_NAME, RELAY_ACCEPT_TIMEOUT};
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::{PostgresAddress, RelayEndpoint, SslMode};
use crate::ssh_tunnel::main::open_channel::open_channel;

/// When `address` may use TLS or goes through a bastion: where the replication client connects (a private socket folder whose `.s.PGSQL.<port>` socket, or on Windows a loopback port, carries the next session opened on it to the server, encrypted, or in plain text when the server refuses TLS under `prefer` (one connection either way, so the server logs no abandoned one), and the application name it must send. `None` to connect directly: `sslmode = "disable"`, a Unix socket host, or a failed handshake under `prefer`, which libpq also retries in plain text. Through a bastion it never connects directly: that retry opens a fresh channel.
pub async fn relay_tls(
    address: &PostgresAddress<'_>,
) -> Result<Option<RelayEndpoint>, PostgresError> {
    let failed = |reason: String| PostgresError::Tls {
        address: address.shown(),
        reason,
    };
    if let Some(tunnel) = address.ssh {
        if address.tls.mode != SslMode::Disable {
            let server = open_channel(tunnel, address.host, address.port).await?;
            if let Some(relayed) = negotiate(server, address).await? {
                return Ok(Some(relayed));
            }
        }
        let server = open_channel(tunnel, address.host, address.port).await?;
        return Ok(Some(relay(server, address.port).await.map_err(failed)?));
    }
    if address.tls.mode == SslMode::Disable || address.host.starts_with('/') {
        return Ok(None);
    }
    let server = TcpStream::connect((address.host, address.port))
        .await
        .map_err(|error| failed(error.to_string()))?;
    server
        .set_nodelay(true)
        .map_err(|error| failed(error.to_string()))?;
    negotiate(server, address).await
}

/// Ask `server` for TLS and relay the session it allows; `None` when the handshake failed under `prefer`.
async fn negotiate<S>(
    mut server: S,
    address: &PostgresAddress<'_>,
) -> Result<Option<RelayEndpoint>, PostgresError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let failed = |reason: String| PostgresError::Tls {
        address: address.shown(),
        reason,
    };
    let config = client_config(&address.tls).map_err(failed)?;
    server
        .write_all(&SSL_REQUEST)
        .await
        .map_err(|error| failed(error.to_string()))?;
    if server
        .read_u8()
        .await
        .map_err(|error| failed(error.to_string()))?
        != b'S'
    {
        return match address.tls.mode {
            SslMode::Prefer => Ok(Some(relay(server, address.port).await.map_err(failed)?)),
            _ => Err(failed("the server does not offer TLS".to_owned())),
        };
    }
    let name = rustls::pki_types::ServerName::try_from(address.host.to_owned())
        .map_err(|error| failed(error.to_string()))?;
    let session = match TlsConnector::from(config).connect(name, server).await {
        Ok(session) => session,
        Err(_) if address.tls.mode == SslMode::Prefer => return Ok(None),
        Err(error) => return Err(failed(format!("TLS handshake failed: {error}"))),
    };
    Ok(Some(relay(session, address.port).await.map_err(failed)?))
}

/// A folder only this user can enter (some systems ignore a socket's own permissions) whose `.s.PGSQL.<port>` socket pipes the first session opened on it through `server`; the folder and its socket go once it is accepted, or after [`RELAY_ACCEPT_TIMEOUT`] without one.
#[cfg(unix)]
async fn relay<S>(mut server: S, port: u16) -> Result<RelayEndpoint, String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let folder = tempfile::Builder::new()
        .prefix("sqlload-")
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .map_err(|error| error.to_string())?;
    let listener = UnixListener::bind(folder.path().join(format!(".s.PGSQL.{port}")))
        .map_err(|error| error.to_string())?;
    let host = folder.path().to_string_lossy().into_owned();
    tokio::spawn(async move {
        let accepted = tokio::time::timeout(RELAY_ACCEPT_TIMEOUT, listener.accept()).await;
        drop(listener);
        drop(folder);
        if let Ok(Ok((mut local, _))) = accepted {
            let _ = tokio::io::copy_bidirectional(&mut local, &mut server).await;
        }
    });
    Ok(RelayEndpoint {
        host,
        port,
        application_name: crate::auth::models::Secret(APPLICATION_NAME.to_owned()),
    })
}

/// pgwire-replication reaches no socket file on Windows, only TCP, so the session goes through a loopback port that admits only this relay's secret (see [`loopback_relay`]).
#[cfg(windows)]
async fn relay<S>(server: S, _port: u16) -> Result<RelayEndpoint, String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    loopback_relay(server).await
}
