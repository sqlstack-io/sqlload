use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::net::TcpStream;

use crate::postgres_source::_helpers::loopback_relay::{admitted_startup, loopback_relay};
use crate::postgres_source::constants::{PROTOCOL_VERSION, RELAY_STARTUP_TIMEOUT};
use crate::postgres_source::models::RelayEndpoint;
use crate::postgres_source::tests::helpers::{startup_message, startup_params};
use crate::postgres_source::tests::test_types::AdmittedStartupTestCase;

const SECRET: &str = "sqlload-6f1c0d2a9b8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a";

fn block_on(body: impl std::future::Future<Output = Result<(), String>>) -> Result<(), String> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(body)
}

fn owned(params: &[(&str, &str)]) -> Vec<(String, String)> {
    params
        .iter()
        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
        .collect()
}

/// The replication client's startup message, naming `application_name`.
fn client_startup(application_name: &str) -> Vec<u8> {
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

/// The whole first message the server side of the relay received.
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

fn contains(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle.as_bytes())
}

async fn connect(endpoint: &RelayEndpoint) -> Result<TcpStream, String> {
    TcpStream::connect((endpoint.host.as_str(), endpoint.port))
        .await
        .map_err(|error| error.to_string())
}

/// Whether the relay closes `stream` without answering, within `wait`.
async fn closed_unanswered(stream: &mut TcpStream, wait: Duration) -> Result<bool, String> {
    let mut byte = [0u8; 1];
    match tokio::time::timeout(wait, stream.read(&mut byte)).await {
        Ok(Ok(0) | Err(_)) => Ok(true),
        Ok(Ok(_)) => Ok(false),
        Err(_) => Err("the relay kept the connection open".to_owned()),
    }
}

#[test]
fn given_startup_messages_when_the_relay_checks_them_then_only_the_secret_is_admitted_and_never_forwarded()
-> Result<(), String> {
    let wrong_last = format!("{}0", &SECRET[..SECRET.len() - 1]);
    let mut short_length = client_startup(SECRET);
    short_length[3] -= 1;
    let mut no_terminator = client_startup(SECRET);
    no_terminator.pop();
    no_terminator[3] -= 1;
    let test_cases = [
        AdmittedStartupTestCase {
            description: "the secret, among the other parameters",
            message: client_startup(SECRET),
            expected_forwarded: Some(owned(&[
                ("user", "replicator"),
                ("database", "shop"),
                ("replication", "database"),
                ("application_name", "sqlload"),
            ])),
        },
        AdmittedStartupTestCase {
            description: "the secret first, before an empty value",
            message: startup_message(
                PROTOCOL_VERSION,
                &[("application_name", SECRET), ("options", ""), ("user", "u")],
            ),
            expected_forwarded: Some(owned(&[
                ("application_name", "sqlload"),
                ("options", ""),
                ("user", "u"),
            ])),
        },
        AdmittedStartupTestCase {
            description: "a secret wrong in its last character",
            message: client_startup(&wrong_last),
            expected_forwarded: None,
        },
        AdmittedStartupTestCase {
            description: "a prefix of the secret",
            message: client_startup(&SECRET[..SECRET.len() - 1]),
            expected_forwarded: None,
        },
        AdmittedStartupTestCase {
            description: "the secret with more after it",
            message: client_startup(&format!("{SECRET}0")),
            expected_forwarded: None,
        },
        AdmittedStartupTestCase {
            description: "the plain application name",
            message: client_startup("sqlload"),
            expected_forwarded: None,
        },
        AdmittedStartupTestCase {
            description: "no application name",
            message: startup_message(PROTOCOL_VERSION, &[("user", "u"), ("database", "d")]),
            expected_forwarded: None,
        },
        AdmittedStartupTestCase {
            description: "the secret, then a second application name",
            message: startup_message(
                PROTOCOL_VERSION,
                &[("application_name", SECRET), ("application_name", "other")],
            ),
            expected_forwarded: None,
        },
        AdmittedStartupTestCase {
            description: "an SSLRequest",
            message: vec![0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f],
            expected_forwarded: None,
        },
        AdmittedStartupTestCase {
            description: "protocol 2.0 with the secret",
            message: startup_message([0, 2, 0, 0], &[("application_name", SECRET)]),
            expected_forwarded: None,
        },
        AdmittedStartupTestCase {
            description: "a length one short of the message",
            message: short_length,
            expected_forwarded: None,
        },
        AdmittedStartupTestCase {
            description: "no final NUL",
            message: no_terminator,
            expected_forwarded: None,
        },
    ];
    for test_case in test_cases {
        let forwarded = admitted_startup(&test_case.message, SECRET);
        let found = forwarded.as_deref().map(startup_params).transpose()?;
        if found != test_case.expected_forwarded {
            return Err(format!(
                "{}: forwarded {found:?}, expected {:?}",
                test_case.description, test_case.expected_forwarded
            ));
        }
        if forwarded.is_some_and(|bytes| contains(&bytes, &SECRET["sqlload-".len()..])) {
            return Err(format!(
                "{}: the secret was forwarded",
                test_case.description
            ));
        }
    }
    Ok(())
}

#[test]
fn given_a_silent_connection_first_when_the_replication_client_connects_then_it_is_relayed_without_waiting()
-> Result<(), String> {
    block_on(async {
        let (server, mut upstream) = tokio::io::duplex(64 * 1024);
        let endpoint = loopback_relay(server).await?;
        let _silent = connect(&endpoint).await?;
        let started = Instant::now();
        let mut client = connect(&endpoint).await?;
        client
            .write_all(&client_startup(endpoint.application_name.expose()))
            .await
            .map_err(|error| error.to_string())?;
        let forwarded = tokio::time::timeout(RELAY_STARTUP_TIMEOUT, read_forwarded(&mut upstream))
            .await
            .map_err(|_| "the client was held up behind the silent connection".to_owned())??;
        if started.elapsed() >= RELAY_STARTUP_TIMEOUT {
            return Err(format!("relayed after {:?}", started.elapsed()));
        }
        let name = startup_params(&forwarded)?
            .into_iter()
            .find(|(name, _)| name == "application_name");
        if name != Some(("application_name".to_owned(), "sqlload".to_owned())) {
            return Err(format!("forwarded application name {name:?}"));
        }
        // The session is piped both ways once admitted.
        upstream
            .write_all(b"R")
            .await
            .map_err(|error| error.to_string())?;
        let mut answer = [0u8; 1];
        client
            .read_exact(&mut answer)
            .await
            .map_err(|error| error.to_string())?;
        client
            .write_all(b"p")
            .await
            .map_err(|error| error.to_string())?;
        upstream
            .read_exact(&mut answer)
            .await
            .map_err(|error| error.to_string())?;
        match &answer {
            b"p" => Ok(()),
            other => Err(format!("piped {other:?}")),
        }
    })
}

#[test]
fn given_connections_without_the_secret_when_they_connect_then_they_are_dropped_and_the_server_receives_nothing()
-> Result<(), String> {
    block_on(async {
        let (server, mut upstream) = tokio::io::duplex(64 * 1024);
        let endpoint = loopback_relay(server).await?;
        let secret = endpoint.application_name.expose().to_owned();
        if format!("{endpoint:?}").contains(&secret) {
            return Err("the relay's Debug shows its secret".to_owned());
        }
        let wrong = format!("sqlload-{}", "0".repeat(64));
        let attempts: [(&str, Vec<u8>); 4] = [
            ("a wrong secret", client_startup(&wrong)),
            ("the plain application name", client_startup("sqlload")),
            ("an oversize length", vec![0, 1, 0, 0, 0, 3, 0, 0]),
            ("an SSLRequest", vec![0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f]),
        ];
        for (description, bytes) in attempts {
            let mut stream = connect(&endpoint).await?;
            stream
                .write_all(&bytes)
                .await
                .map_err(|error| error.to_string())?;
            if !closed_unanswered(&mut stream, RELAY_STARTUP_TIMEOUT * 2)
                .await
                .map_err(|error| format!("{description}: {error}"))?
            {
                return Err(format!("{description}: the relay answered"));
            }
        }
        let mut byte = [0u8; 1];
        if tokio::time::timeout(Duration::from_millis(300), upstream.read(&mut byte))
            .await
            .is_ok()
        {
            return Err("the server received bytes from a refused connection".to_owned());
        }
        let mut client = connect(&endpoint).await?;
        client
            .write_all(&client_startup(&secret))
            .await
            .map_err(|error| error.to_string())?;
        let forwarded = read_forwarded(&mut upstream).await?;
        if contains(&forwarded, &secret["sqlload-".len()..]) {
            return Err("the secret reached the server".to_owned());
        }
        let expected = owned(&[
            ("user", "replicator"),
            ("database", "shop"),
            ("replication", "database"),
            ("application_name", "sqlload"),
        ]);
        let found = startup_params(&forwarded)?;
        if found != expected {
            return Err(format!("forwarded {found:?}"));
        }
        // Admitted once: the port is closed to every later connection.
        match tokio::time::timeout(Duration::from_secs(10), connect(&endpoint)).await {
            Ok(Err(_)) => Ok(()),
            Ok(Ok(mut late)) => match closed_unanswered(&mut late, Duration::from_secs(2)).await {
                Ok(true) => Ok(()),
                _ => Err("a connection after the admitted one was kept".to_owned()),
            },
            Err(_) => Err("connecting after admission hung".to_owned()),
        }
    })
}
