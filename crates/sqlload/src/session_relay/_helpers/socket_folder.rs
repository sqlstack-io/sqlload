use std::os::unix::fs::PermissionsExt;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::UnixListener;

use crate::auth::models::Secret;
use crate::session_relay::constants::{APPLICATION_NAME, RELAY_ACCEPT_TIMEOUT};
use crate::session_relay::models::RelayEndpoint;

/// A folder only this user can enter (some systems ignore a socket's own permissions) whose `.s.PGSQL.<port>` socket pipes the first session opened on it through `server`; the folder and its socket go once it is accepted, or after [`RELAY_ACCEPT_TIMEOUT`] without one.
pub(crate) fn socket_folder_relay<S>(mut server: S, port: u16) -> Result<RelayEndpoint, String>
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
        application_name: Secret(APPLICATION_NAME.to_owned()),
    })
}
