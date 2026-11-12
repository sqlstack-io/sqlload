use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::net::TcpStream;

use crate::session_relay::_helpers::loopback::loopback_relay;
use crate::session_relay::constants::{PROTOCOL_VERSION, RELAY_STARTUP_TIMEOUT};
use crate::session_relay::tests::test_types::RelayOutcome;

/// A relay secret as the relay makes them: `sqlload-` and 64 hex digits.
pub(crate) const SECRET: &str =
    "sqlload-6f1c0d2a9b8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a";

/// Postgres's `SSLRequest`.
pub(crate) const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];

/// What the server receives for the replication client's startup message.
pub(crate) const FORWARDED: &str =
    "user=replicator,database=shop,replication=database,application_name=sqlload";

pub(crate) fn block_on<T>(
    body: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(body)
}

/// A message of protocol `version` whose body after the version is `body`, framed with its length.
pub(crate) fn framed(version: [u8; 4], body: &[u8]) -> Vec<u8> {
    let length = u32::try_from(body.len() + 8).unwrap_or(u32::MAX);
    [&length.to_be_bytes()[..], &version, body].concat()
}

/// A startup message of protocol `version` with `params`, framed as libpq and pgwire-replication frame it.
pub(crate) fn startup_message(version: [u8; 4], params: &[(&str, &str)]) -> Vec<u8> {
    let mut body = Vec::new();
    for (name, value) in params {
        body.extend_from_slice(format!("{name}\0{value}\0").as_bytes());
    }
    body.push(0);
    framed(version, &body)
}

/// The replication client's startup message, naming `application_name`.
pub(crate) fn client_startup(application_name: &str) -> Vec<u8> {
    startup_message(
        PROTOCOL_VERSION,
        &[
            ("user", "replicator"),
            ("database", "shop"),
            ("replication", "database"),
            ("application_name", application_name),
        ],
    )
}

/// `message` with a length one less than it is.
pub(crate) fn declaring_one_less(mut message: Vec<u8>) -> Vec<u8> {
    message[3] -= 1;
    message
}

/// `message` without its final NUL, its length one less to match.
pub(crate) fn without_final_nul(mut message: Vec<u8>) -> Vec<u8> {
    message.pop();
    declaring_one_less(message)
}

/// A whole startup message's parameters as `name=value` joined by `,`; `malformed` when it is not one.
pub(crate) fn startup_params(message: &[u8]) -> String {
    let declared = message
        .first_chunk::<4>()
        .and_then(|length| usize::try_from(u32::from_be_bytes(*length)).ok());
    let well_formed =
        declared == Some(message.len()) && message.get(4..8) == Some(&PROTOCOL_VERSION[..]);
    message
        .get(8..)
        .and_then(|rest| rest.strip_suffix(b"\0\0"))
        .filter(|_| well_formed)
        .map(|rest| {
            String::from_utf8_lossy(rest)
                .split('\0')
                .collect::<Vec<_>>()
                .chunks(2)
                .map(|pair| pair.join("="))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_else(|| "malformed".to_owned())
}

pub(crate) fn contains(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle.as_bytes())
}

/// The whole first message the server side of a relay received.
async fn read_forwarded(upstream: &mut DuplexStream) -> Result<Vec<u8>, String> {
    let mut length = [0u8; 4];
    upstream
        .read_exact(&mut length)
        .await
        .map_err(|error| error.to_string())?;
    let declared =
        usize::try_from(u32::from_be_bytes(length)).map_err(|error| error.to_string())?;
    let mut message = length.to_vec();
    message.resize(declared.max(4), 0);
    upstream
        .read_exact(&mut message[4..])
        .await
        .map_err(|error| error.to_string())?;
    Ok(message)
}

/// Whether the relay closes `stream` within two seconds without sending it a byte.
async fn closed_unanswered(stream: &mut TcpStream) -> bool {
    let mut byte = [0u8; 1];
    tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
        .await
        .is_ok_and(|read| read.unwrap_or_default() == 0)
}

async fn io<T>(step: impl std::future::Future<Output = std::io::Result<T>>) -> Result<T, String> {
    step.await.map_err(|error| error.to_string())
}

/// A loopback relay in front of an in-memory server: open a connection for each of `probes`, then send each its bytes at once (so refusals finish together), then open the replication client with the relay's application name, which must be relayed well before a silent connection's startup deadline; then pipe a byte each way, and connect once more.
pub(crate) async fn relay_scenario(probes: &[Vec<u8>]) -> Result<RelayOutcome, String> {
    let (server, mut upstream) = tokio::io::duplex(64 * 1024);
    let endpoint = loopback_relay(server).await?;
    let address = (endpoint.host.clone(), endpoint.port);
    let mut opened = Vec::new();
    for _ in probes {
        opened.push(io(TcpStream::connect(&address)).await?);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    for (stream, bytes) in opened.iter_mut().zip(probes) {
        io(stream.write_all(bytes)).await?;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut byte = [0u8; 1];
    let upstream_bytes_before_client =
        tokio::time::timeout(Duration::from_millis(200), upstream.read(&mut byte))
            .await
            .map_or(0, |read| read.unwrap_or_default());
    let mut client = io(TcpStream::connect(&address)).await?;
    io(client.write_all(&client_startup(endpoint.application_name.expose()))).await?;
    let forwarded = tokio::time::timeout(RELAY_STARTUP_TIMEOUT / 2, read_forwarded(&mut upstream))
        .await
        .map_err(|_| "the client was not relayed in time".to_owned())??;
    let secret = &endpoint.application_name.expose()["sqlload-".len()..];
    io(upstream.write_all(b"R")).await?;
    let mut answer = [0u8; 1];
    io(client.read_exact(&mut answer)).await?;
    io(client.write_all(b"p")).await?;
    let mut echoed = [0u8; 1];
    io(upstream.read_exact(&mut echoed)).await?;
    let mut probes_closed_unanswered = 0;
    for stream in &mut opened {
        probes_closed_unanswered += usize::from(closed_unanswered(stream).await);
    }
    let later_connection_refused =
        tokio::time::timeout(Duration::from_secs(10), TcpStream::connect(&address))
            .await
            .map_err(|_| "connecting after the client hung".to_owned())?
            .is_err();
    Ok(RelayOutcome {
        upstream_bytes_before_client,
        forwarded: startup_params(&forwarded),
        secret_forwarded: contains(&forwarded, secret),
        piped: (answer, echoed) == (*b"R", *b"p"),
        probes_closed_unanswered,
        later_connection_refused,
    })
}

/// What every scenario's relay does once its client connects, having refused `probes` connections.
pub(crate) fn relayed(probes: usize) -> RelayOutcome {
    RelayOutcome {
        upstream_bytes_before_client: 0,
        forwarded: FORWARDED.to_owned(),
        secret_forwarded: false,
        piped: true,
        probes_closed_unanswered: probes,
        later_connection_refused: true,
    }
}
