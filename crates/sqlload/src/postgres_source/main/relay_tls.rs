//! TLS for a client that speaks only plain text: the replication client.

use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixListener};
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use crate::postgres_source::_helpers::tls::client_config;
use crate::postgres_source::constants::{RELAY_ACCEPT_TIMEOUT, SSL_REQUEST};
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::models::{PostgresAddress, SslMode};

/// When `address` calls for TLS and the server takes it: a private socket folder whose `.s.PGSQL.<port>` socket carries the next session opened on it to the server, encrypted. `None` to connect directly: `sslmode = "disable"`, a Unix socket host, or a server refusing TLS under `prefer`.
pub async fn relay_tls(address: &PostgresAddress<'_>) -> Result<Option<String>, PostgresError> {
    if address.tls.mode == SslMode::Disable || address.host.starts_with('/') {
        return Ok(None);
    }
    let failed = |reason: String| PostgresError::Tls {
        address: format!("{}:{}/{}", address.host, address.port, address.database),
        reason,
    };
    let config = client_config(&address.tls).map_err(failed)?;
    let mut server = TcpStream::connect((address.host, address.port))
        .await
        .map_err(|error| failed(error.to_string()))?;
    server
        .set_nodelay(true)
        .map_err(|error| failed(error.to_string()))?;
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
            SslMode::Prefer => Ok(None),
            _ => Err(failed("the server does not offer TLS".to_owned())),
        };
    }
    let name = rustls::pki_types::ServerName::try_from(address.host.to_owned())
        .map_err(|error| failed(error.to_string()))?;
    let session = TlsConnector::from(config)
        .connect(name, server)
        .await
        .map_err(|error| failed(format!("TLS handshake failed: {error}")))?;
    let folder = tempfile::Builder::new()
        .prefix("sqlload-")
        .tempdir()
        .map_err(|error| failed(error.to_string()))?;
    let listener = UnixListener::bind(folder.path().join(format!(".s.PGSQL.{}", address.port)))
        .map_err(|error| failed(error.to_string()))?;
    let host = folder.path().to_string_lossy().into_owned();
    tokio::spawn(relay_one(folder, listener, session));
    Ok(Some(host))
}

/// Pipe the first session opened on `listener` through `session`; the folder and its socket go once it is accepted, or after [`RELAY_ACCEPT_TIMEOUT`] without one.
async fn relay_one(folder: TempDir, listener: UnixListener, mut session: TlsStream<TcpStream>) {
    let accepted = tokio::time::timeout(RELAY_ACCEPT_TIMEOUT, listener.accept()).await;
    drop(listener);
    drop(folder);
    if let Ok(Ok((mut local, _))) = accepted {
        let _ = tokio::io::copy_bidirectional(&mut local, &mut session).await;
    }
}
