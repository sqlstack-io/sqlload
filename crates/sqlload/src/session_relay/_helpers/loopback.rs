//! A relay on a loopback port, where the client cannot reach a private socket (Windows). Any local user can connect to the port, so the relay forwards the session only to the connection whose startup message names this relay's secret as its application name, and nothing reaches the server before that check passes.

use std::net::Ipv4Addr;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::auth::models::Secret;
use crate::session_relay::_helpers::startup::admitted_startup;
use crate::session_relay::constants::{
    APPLICATION_NAME, RELAY_ACCEPT_TIMEOUT, RELAY_STARTUP_TIMEOUT, STARTUP_MESSAGE_LIMIT,
    STARTUP_MESSAGE_MINIMUM,
};
use crate::session_relay::models::RelayEndpoint;

/// A port on 127.0.0.1 that pipes `server` to the first connection presenting the returned application name; it closes once that connection is admitted, or after [`RELAY_ACCEPT_TIMEOUT`] without one. Other connections are dropped, each after [`RELAY_STARTUP_TIMEOUT`] at most, without a byte of theirs reaching the server.
pub(crate) async fn loopback_relay<S>(server: S) -> Result<RelayEndpoint, String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut secret = [0u8; 32];
    getrandom::fill(&mut secret).map_err(|error| error.to_string())?;
    let expected = Secret(format!("{APPLICATION_NAME}-{}", hex::encode(secret)));
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .map_err(|error| error.to_string())?;
    let port = listener
        .local_addr()
        .map_err(|error| error.to_string())?
        .port();
    tokio::spawn(serve(listener, server, expected.clone()));
    Ok(RelayEndpoint {
        host: Ipv4Addr::LOCALHOST.to_string(),
        port,
        application_name: expected,
    })
}

/// Admit connections to `listener` side by side, so a silent one holds up none of the others, and pipe `server` to the first admitted. A refused connection's result is matched inside its arm: a pattern on the arm itself would disable it for the rest of the `select!` after one refusal.
async fn serve<S>(listener: TcpListener, mut server: S, expected: Secret)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + RELAY_ACCEPT_TIMEOUT;
    let mut pending = JoinSet::new();
    let (mut local, startup) = loop {
        tokio::select! {
            accepted = listener.accept() => {
                if let Ok((stream, _)) = accepted {
                    pending.spawn(admit(stream, expected.clone()));
                }
            }
            Some(joined) = pending.join_next() => {
                if let Ok(Ok(admitted)) = joined {
                    break admitted;
                }
            }
            () = tokio::time::sleep_until(deadline) => return,
        }
    };
    drop(listener);
    drop(pending);
    let _ = async {
        server.write_all(&startup).await?;
        tokio::io::copy_bidirectional(&mut local, &mut server).await
    }
    .await;
}

/// `stream` and the startup message to forward for it, when it sends one naming `expected` within [`RELAY_STARTUP_TIMEOUT`]; why it is refused otherwise.
async fn admit(mut stream: TcpStream, expected: Secret) -> Result<(TcpStream, Vec<u8>), String> {
    let message = tokio::time::timeout(RELAY_STARTUP_TIMEOUT, read_startup(&mut stream))
        .await
        .map_err(|_| "no startup message in time".to_owned())??;
    let forwarded = admitted_startup(&message, expected.expose())?;
    stream
        .set_nodelay(true)
        .map_err(|error| error.to_string())?;
    Ok((stream, forwarded))
}

/// One startup message: its length, and as many bytes as that gives, from [`STARTUP_MESSAGE_MINIMUM`] to [`STARTUP_MESSAGE_LIMIT`].
async fn read_startup<R: AsyncRead + Unpin>(stream: &mut R) -> Result<Vec<u8>, String> {
    let mut length = [0u8; 4];
    stream
        .read_exact(&mut length)
        .await
        .map_err(|error| error.to_string())?;
    let declared =
        usize::try_from(u32::from_be_bytes(length)).map_err(|error| error.to_string())?;
    if !(STARTUP_MESSAGE_MINIMUM..=STARTUP_MESSAGE_LIMIT).contains(&declared) {
        return Err(format!("a startup message of {declared} bytes"));
    }
    let mut message = vec![0; declared];
    message[..4].copy_from_slice(&length);
    stream
        .read_exact(&mut message[4..])
        .await
        .map_err(|error| error.to_string())?;
    Ok(message)
}
