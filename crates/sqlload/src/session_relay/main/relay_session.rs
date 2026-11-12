//! Open a relay that pipes one session through a connection SQLLoad opened.

use tokio::io::{AsyncRead, AsyncWrite};

#[cfg(windows)]
use crate::session_relay::_helpers::loopback::loopback_relay;
#[cfg(unix)]
use crate::session_relay::_helpers::socket_folder::socket_folder_relay;
use crate::session_relay::models::RelayEndpoint;

/// Where the client reaches `server` (the session to a Postgres server listening on `port`): on Unix a `.s.PGSQL.<port>` socket in a folder only this user can enter; on Windows, where pgwire-replication reaches no socket file, a 127.0.0.1 port that admits only the connection presenting the returned application name. Either serves the first session admitted and closes, or closes after [`RELAY_ACCEPT_TIMEOUT`](crate::session_relay::constants::RELAY_ACCEPT_TIMEOUT) without one.
pub async fn relay_session<S>(server: S, port: u16) -> Result<RelayEndpoint, String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    #[cfg(unix)]
    let relayed = socket_folder_relay(server, port);
    #[cfg(windows)]
    let relayed = {
        let _ = port;
        loopback_relay(server).await
    };
    relayed
}
