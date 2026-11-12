//! A relay on a loopback port, where the replication client cannot reach a private socket (Windows). Any local user can connect to the port, so the relay forwards the session only to the connection whose startup message names this relay's secret as its application name, and nothing reaches the server before that check passes.

use std::net::Ipv4Addr;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::auth::models::Secret;
use crate::postgres_source::constants::{
    APPLICATION_NAME, PROTOCOL_VERSION, RELAY_ACCEPT_TIMEOUT, RELAY_STARTUP_TIMEOUT,
    STARTUP_MESSAGE_LIMIT,
};
use crate::postgres_source::models::RelayEndpoint;

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

/// Admit connections to `listener` side by side, so a silent one holds up none of the others, and pipe `server` to the first admitted.
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
            Some(Ok(Some(admitted))) = pending.join_next() => break admitted,
            () = tokio::time::sleep_until(deadline) => return,
        }
    };
    drop(listener);
    drop(pending);
    if server.write_all(&startup).await.is_ok() {
        let _ = tokio::io::copy_bidirectional(&mut local, &mut server).await;
    }
}

/// `stream` and the startup message to forward for it, when it sends one naming `expected` within [`RELAY_STARTUP_TIMEOUT`].
async fn admit(mut stream: TcpStream, expected: Secret) -> Option<(TcpStream, Vec<u8>)> {
    let message = tokio::time::timeout(RELAY_STARTUP_TIMEOUT, read_startup(&mut stream))
        .await
        .ok()??;
    let forwarded = admitted_startup(&message, expected.expose())?;
    let _ = stream.set_nodelay(true);
    Some((stream, forwarded))
}

/// One startup message: its length, and as many bytes as that gives, up to [`STARTUP_MESSAGE_LIMIT`].
async fn read_startup<R: AsyncRead + Unpin>(stream: &mut R) -> Option<Vec<u8>> {
    let mut length = [0u8; 4];
    stream.read_exact(&mut length).await.ok()?;
    let declared = usize::try_from(u32::from_be_bytes(length)).ok()?;
    if !(8..=STARTUP_MESSAGE_LIMIT).contains(&declared) {
        return None;
    }
    let mut message = vec![0; declared];
    message[..4].copy_from_slice(&length);
    stream.read_exact(&mut message[4..]).await.ok()?;
    Some(message)
}

/// `message`, a whole protocol 3.0 startup message, with its one `application_name` replaced by [`APPLICATION_NAME`]; `None` unless that name is `expected`.
pub(crate) fn admitted_startup(message: &[u8], expected: &str) -> Option<Vec<u8>> {
    let declared = usize::try_from(u32::from_be_bytes(message.get(..4)?.try_into().ok()?)).ok()?;
    if declared != message.len() || message.get(4..8)? != PROTOCOL_VERSION {
        return None;
    }
    // Pairs of NUL-terminated names and values, then one more NUL.
    let fields: Vec<&[u8]> = message[8..]
        .strip_suffix(b"\0\0")?
        .split(|byte| *byte == 0)
        .collect();
    if !fields.len().is_multiple_of(2) || fields.iter().step_by(2).any(|name| name.is_empty()) {
        return None;
    }
    let named: Vec<usize> = (0..fields.len())
        .step_by(2)
        .filter(|index| fields[*index] == b"application_name")
        .collect();
    let [name] = named.as_slice() else {
        return None;
    };
    if !same_secret(fields[name + 1], expected.as_bytes()) {
        return None;
    }
    let mut forwarded = vec![0; 4];
    forwarded.extend_from_slice(&PROTOCOL_VERSION);
    for (index, field) in fields.iter().enumerate() {
        match index == name + 1 {
            true => forwarded.extend_from_slice(APPLICATION_NAME.as_bytes()),
            false => forwarded.extend_from_slice(field),
        }
        forwarded.push(0);
    }
    forwarded.push(0);
    let length = u32::try_from(forwarded.len()).ok()?;
    forwarded[..4].copy_from_slice(&length.to_be_bytes());
    Some(forwarded)
}

/// Whether `given` is `expected`, comparing every byte whatever the first difference, so the time taken tells nothing of the secret.
fn same_secret(given: &[u8], expected: &[u8]) -> bool {
    given.len() == expected.len()
        && given
            .iter()
            .zip(expected)
            .fold(0u8, |difference, (left, right)| difference | (left ^ right))
            == 0
}
