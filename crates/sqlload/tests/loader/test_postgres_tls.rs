use std::path::Path;

use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::debug_project::debug_project;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::models::PlanOptions;
use sqlload::postgres_source::main::relay_tls::relay_tls;
use sqlload::postgres_source::models::{PostgresAddress, PostgresTls, SslMode};
use sqlload_testkit::postgres::constants::{CERT_USER, DATABASE, PASSWORD, USER};
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;
use sqlload_testkit::postgres::main::start_postgres_tls::start_postgres_tls;

use crate::helpers::{
    CDC_CHANGES, cdc_project, described_project, duckdb_text, run_async, run_sqlload, tls_project,
};
use crate::test_postgres_cdc::{MERGED_QUERY, MERGED_RESULT, SETUP};
use crate::test_types::{
    CdcTlsTestCase, PlanWarningTestCase, RelaySessionTestCase, RelayTestCase, TlsDebugTestCase,
    TlsServer,
};

/// Where a case's settings name the test certificates.
const FILES: &str = "FILES";
/// How `debug` starts a source line that connected.
const OK: &str = "ok    source app  ";
/// How `debug` starts a source line that failed.
const FAIL: &str = "fail  source app  ";

/// `debug` against a TLS-only server and a plain-text one under every `sslmode`: what protects the connection when it opens, and why it fails when it must.
#[test]
fn given_each_sslmode_when_debugging_a_tls_only_and_a_plain_server_then_it_connects_as_libpq_would()
-> Result<(), String> {
    let test_cases = [
        TlsDebugTestCase {
            description: "disable is refused by a TLS-only server",
            server: TlsServer::TlsOnly,
            host: "localhost",
            user: USER,
            settings: "sslmode = \"disable\"",
            expected_status: FAIL,
            expected_text: "no encryption",
        },
        TlsDebugTestCase {
            description: "prefer takes TLS when offered, unchecked",
            server: TlsServer::TlsOnly,
            host: "localhost",
            user: USER,
            settings: "",
            expected_status: OK,
            expected_text: ", TLS, certificate not checked",
        },
        TlsDebugTestCase {
            description: "require encrypts without checking the certificate",
            server: TlsServer::TlsOnly,
            host: "localhost",
            user: USER,
            settings: "sslmode = \"require\"",
            expected_status: OK,
            expected_text: ", TLS, certificate not checked",
        },
        TlsDebugTestCase {
            description: "require with a root certificate checks the chain, as libpq does",
            server: TlsServer::TlsOnly,
            host: "localhost",
            user: USER,
            settings: "sslmode = \"require\"\nsslrootcert = \"FILES/ca.pem\"",
            expected_status: OK,
            expected_text: ", TLS, CA checked",
        },
        TlsDebugTestCase {
            description: "require with an unrelated root certificate fails",
            server: TlsServer::TlsOnly,
            host: "localhost",
            user: USER,
            settings: "sslmode = \"require\"\nsslrootcert = \"FILES/other_ca.pem\"",
            expected_status: FAIL,
            expected_text: "UnknownIssuer",
        },
        TlsDebugTestCase {
            description: "verify-ca with the server's CA connects",
            server: TlsServer::TlsOnly,
            host: "localhost",
            user: USER,
            settings: "sslmode = \"verify-ca\"\nsslrootcert = \"FILES/ca.pem\"",
            expected_status: OK,
            expected_text: ", TLS, CA checked",
        },
        TlsDebugTestCase {
            description: "verify-ca with a wrong CA fails",
            server: TlsServer::TlsOnly,
            host: "localhost",
            user: USER,
            settings: "sslmode = \"verify-ca\"\nsslrootcert = \"FILES/other_ca.pem\"",
            expected_status: FAIL,
            expected_text: "UnknownIssuer",
        },
        TlsDebugTestCase {
            description: "verify-ca ignores a host name the certificate does not name",
            server: TlsServer::TlsOnly,
            host: "127.0.0.1",
            user: USER,
            settings: "sslmode = \"verify-ca\"\nsslrootcert = \"FILES/ca.pem\"",
            expected_status: OK,
            expected_text: ", TLS, CA checked",
        },
        TlsDebugTestCase {
            description: "verify-full with the server's CA and its name connects",
            server: TlsServer::TlsOnly,
            host: "localhost",
            user: USER,
            settings: "sslmode = \"verify-full\"\nsslrootcert = \"FILES/ca.pem\"",
            expected_status: OK,
            expected_text: ", TLS, CA and host name checked",
        },
        TlsDebugTestCase {
            description: "verify-full refuses a host name the certificate does not name",
            server: TlsServer::TlsOnly,
            host: "127.0.0.1",
            user: USER,
            settings: "sslmode = \"verify-full\"\nsslrootcert = \"FILES/ca.pem\"",
            expected_status: FAIL,
            expected_text: "certificate not valid for name",
        },
        TlsDebugTestCase {
            description: "verify-full without sslrootcert trusts only the built-in public roots",
            server: TlsServer::TlsOnly,
            host: "localhost",
            user: USER,
            settings: "sslmode = \"verify-full\"",
            expected_status: FAIL,
            expected_text: "UnknownIssuer",
        },
        TlsDebugTestCase {
            description: "a client certificate signs in the user that needs one",
            server: TlsServer::TlsOnly,
            host: "localhost",
            user: CERT_USER,
            settings: "sslmode = \"verify-full\"\nsslrootcert = \"FILES/ca.pem\"\nsslcert = \"FILES/client.pem\"\nsslkey = \"FILES/client.key\"",
            expected_status: OK,
            expected_text: ", TLS, CA and host name checked",
        },
        TlsDebugTestCase {
            description: "without its client certificate that user is refused",
            server: TlsServer::TlsOnly,
            host: "localhost",
            user: CERT_USER,
            settings: "sslmode = \"verify-full\"\nsslrootcert = \"FILES/ca.pem\"",
            expected_status: FAIL,
            expected_text: "certificate",
        },
        TlsDebugTestCase {
            description: "sslcert without sslkey is refused before connecting",
            server: TlsServer::TlsOnly,
            host: "localhost",
            user: CERT_USER,
            settings: "sslmode = \"require\"\nsslcert = \"FILES/client.pem\"",
            expected_status: FAIL,
            expected_text: "sslcert and sslkey must be set together",
        },
        TlsDebugTestCase {
            description: "disable reads a plain server in plain text",
            server: TlsServer::Plain,
            host: "127.0.0.1",
            user: USER,
            settings: "sslmode = \"disable\"",
            expected_status: OK,
            expected_text: ", not encrypted",
        },
        TlsDebugTestCase {
            description: "prefer falls back to plain text on a plain server",
            server: TlsServer::Plain,
            host: "127.0.0.1",
            user: USER,
            settings: "",
            expected_status: OK,
            expected_text: ", not encrypted",
        },
        TlsDebugTestCase {
            description: "require never falls back to plain text",
            server: TlsServer::Plain,
            host: "127.0.0.1",
            user: USER,
            settings: "sslmode = \"require\"",
            expected_status: FAIL,
            expected_text: "server does not support TLS",
        },
        TlsDebugTestCase {
            description: "verify-ca never falls back to plain text",
            server: TlsServer::Plain,
            host: "127.0.0.1",
            user: USER,
            settings: "sslmode = \"verify-ca\"\nsslrootcert = \"FILES/ca.pem\"",
            expected_status: FAIL,
            expected_text: "server does not support TLS",
        },
        TlsDebugTestCase {
            description: "verify-full never falls back to plain text",
            server: TlsServer::Plain,
            host: "localhost",
            user: USER,
            settings: "sslmode = \"verify-full\"\nsslrootcert = \"FILES/ca.pem\"",
            expected_status: FAIL,
            expected_text: "server does not support TLS",
        },
    ];
    run_async(async {
        let tls = start_postgres_tls().await?;
        let plain = start_postgres().await?;
        let ports = [tls.postgres.port, plain.port];
        let files = tls.files.path().display().to_string();
        for test_case in test_cases {
            let settings = test_case.settings.replace(FILES, &files);
            let port = ports[test_case.server as usize];
            let text = tls_project((test_case.host, port, test_case.user), &settings);
            let project = described_project(&text).await?;
            let (lines, _) = debug_project(&project, true).await;
            let source = lines
                .iter()
                .find(|line| line.contains("source app  "))
                .cloned()
                .unwrap_or_default();
            assert!(
                source.starts_with(test_case.expected_status)
                    && source.contains(test_case.expected_text),
                "{}: {lines:?}",
                test_case.description
            );
        }
        Ok(())
    })
}

/// `plan` warns when `prefer` reads a server over the network in plain text, and not for a loopback host or an explicit `disable`.
#[test]
fn given_prefer_when_a_plain_server_is_planned_then_only_a_network_fallback_warns()
-> Result<(), String> {
    let test_cases = [
        PlanWarningTestCase {
            description: "prefer over the network warns",
            network: true,
            settings: "",
            expected_warning: true,
        },
        PlanWarningTestCase {
            description: "an explicit disable does not",
            network: true,
            settings: "sslmode = \"disable\"",
            expected_warning: false,
        },
        PlanWarningTestCase {
            description: "prefer to a loopback address does not",
            network: false,
            settings: "",
            expected_warning: false,
        },
    ];
    run_async(async {
        let plain = start_postgres().await?;
        let bridge = plain
            .container
            .get_bridge_ip_address()
            .await
            .map_err(|error| error.to_string())?
            .to_string();
        let addresses = [(plain.host.clone(), plain.port), (bridge, 5432)];
        for test_case in test_cases {
            let (host, port) = &addresses[usize::from(test_case.network)];
            let text = tls_project((host, *port, USER), test_case.settings);
            let project = described_project(&text).await?;
            let destinations = connect_targets(&project)
                .await
                .map_err(|error| error.to_string())?;
            let plan = plan_project(&project, &destinations, &PlanOptions::default())
                .await
                .map_err(|error| error.to_string())?;
            let warned = plan.warnings.iter().any(|warning| {
                warning.contains(&format!("reads {host}:{port} in plain text"))
                    && warning.contains("sslmode = \"require\"")
            });
            assert_eq!(
                warned, test_case.expected_warning,
                "{}: {:?}",
                test_case.description, plan.warnings
            );
        }
        Ok(())
    })
}

/// The replication relay checks the certificate as a direct connection does and never falls back to plain text when TLS is required.
#[test]
fn given_the_replication_relay_when_opening_it_then_it_checks_like_a_direct_connection()
-> Result<(), String> {
    let test_cases = [
        RelayTestCase {
            description: "verify-full with the CA and the certificate's name relays",
            server: TlsServer::TlsOnly,
            host: "localhost",
            mode: SslMode::VerifyFull,
            root_cert: Some("ca.pem"),
            expected_relayed: true,
            expected_error: "",
        },
        RelayTestCase {
            description: "verify-full refuses a name the certificate does not hold",
            server: TlsServer::TlsOnly,
            host: "127.0.0.1",
            mode: SslMode::VerifyFull,
            root_cert: Some("ca.pem"),
            expected_relayed: false,
            expected_error: "certificate not valid for name",
        },
        RelayTestCase {
            description: "verify-ca with a wrong CA is refused",
            server: TlsServer::TlsOnly,
            host: "localhost",
            mode: SslMode::VerifyCa,
            root_cert: Some("other_ca.pem"),
            expected_relayed: false,
            expected_error: "UnknownIssuer",
        },
        RelayTestCase {
            description: "verify-ca ignores the host name",
            server: TlsServer::TlsOnly,
            host: "127.0.0.1",
            mode: SslMode::VerifyCa,
            root_cert: Some("ca.pem"),
            expected_relayed: true,
            expected_error: "",
        },
        RelayTestCase {
            description: "require on a plain server never falls back",
            server: TlsServer::Plain,
            host: "127.0.0.1",
            mode: SslMode::Require,
            root_cert: None,
            expected_relayed: false,
            expected_error: "does not offer TLS",
        },
        RelayTestCase {
            description: "verify-full on a plain server never falls back",
            server: TlsServer::Plain,
            host: "localhost",
            mode: SslMode::VerifyFull,
            root_cert: Some("ca.pem"),
            expected_relayed: false,
            expected_error: "does not offer TLS",
        },
        RelayTestCase {
            description: "prefer on a plain server relays its one connection in plain text",
            server: TlsServer::Plain,
            host: "127.0.0.1",
            mode: SslMode::Prefer,
            root_cert: None,
            expected_relayed: true,
            expected_error: "",
        },
        RelayTestCase {
            description: "disable connects directly",
            server: TlsServer::TlsOnly,
            host: "127.0.0.1",
            mode: SslMode::Disable,
            root_cert: None,
            expected_relayed: false,
            expected_error: "",
        },
    ];
    run_async(async {
        let tls = start_postgres_tls().await?;
        let plain = start_postgres().await?;
        let ports = [tls.postgres.port, plain.port];
        for test_case in test_cases {
            let root = test_case.root_cert.map(|name| tls.files.path().join(name));
            let address = PostgresAddress {
                host: test_case.host,
                port: ports[test_case.server as usize],
                user: USER,
                password: Some(PASSWORD),
                database: DATABASE,
                tls: PostgresTls {
                    mode: test_case.mode,
                    root_cert: root.as_deref(),
                    ..PostgresTls::default()
                },
            };
            let relayed = relay_tls(&address).await;
            let error = relayed
                .as_ref()
                .err()
                .map(ToString::to_string)
                .unwrap_or_default();
            assert!(
                relayed.as_ref().is_ok_and(Option::is_some) == test_case.expected_relayed
                    && error.contains(test_case.expected_error)
                    && error.is_empty() == test_case.expected_error.is_empty(),
                "{}: {relayed:?}",
                test_case.description
            );
        }
        Ok(())
    })
}

/// A relay serves one session on a socket in a private folder, and the folder is gone once that session is taken.
#[test]
fn given_a_relay_when_its_session_is_taken_then_its_socket_folder_is_gone() -> Result<(), String> {
    let test_cases = [
        RelaySessionTestCase {
            description: "a verify-full relay to the TLS-only server",
            server: TlsServer::TlsOnly,
            host: "localhost",
            mode: SslMode::VerifyFull,
            expected_folder_mode: 0o700,
            expected_left: (false, false),
        },
        RelaySessionTestCase {
            description: "a prefer relay to a plain server",
            server: TlsServer::Plain,
            host: "127.0.0.1",
            mode: SslMode::Prefer,
            expected_folder_mode: 0o700,
            expected_left: (false, false),
        },
    ];
    run_async(async {
        let tls = start_postgres_tls().await?;
        let plain = start_postgres().await?;
        let ports = [tls.postgres.port, plain.port];
        let root = tls.files.path().join("ca.pem");
        for test_case in test_cases {
            let port = ports[test_case.server as usize];
            let address = PostgresAddress {
                host: test_case.host,
                port,
                user: USER,
                password: Some(PASSWORD),
                database: DATABASE,
                tls: PostgresTls {
                    mode: test_case.mode,
                    root_cert: Some(&root),
                    ..PostgresTls::default()
                },
            };
            let folder = relay_tls(&address)
                .await
                .map_err(|error| error.to_string())?
                .unwrap_or_default();
            let socket = Path::new(&folder).join(format!(".s.PGSQL.{port}"));
            let mode = std::os::unix::fs::PermissionsExt::mode(
                &std::fs::metadata(&folder)
                    .map_err(|error| error.to_string())?
                    .permissions(),
            ) & 0o777;
            let session = tokio::net::UnixStream::connect(&socket)
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            let second = tokio::net::UnixStream::connect(&socket).await;
            assert_eq!(
                (mode, (Path::new(&folder).exists(), second.is_ok())),
                (test_case.expected_folder_mode, test_case.expected_left),
                "{}",
                test_case.description
            );
            drop(session);
        }
        Ok(())
    })
}

/// A `postgres_cdc` source on a TLS-only server: the snapshot reads over TLS and the changes stream through the relay.
#[test]
fn given_a_tls_only_server_when_capturing_changes_then_they_land_once() -> Result<(), String> {
    let test_cases = [
        CdcTlsTestCase {
            description: "verify-full, checking the CA and the host name",
            settings: "sslmode = \"verify-full\"\nsslrootcert = \"FILES/ca.pem\"",
            expected_result: MERGED_RESULT,
        },
        CdcTlsTestCase {
            description: "require, checking nothing",
            settings: "sslmode = \"require\"",
            expected_result: MERGED_RESULT,
        },
    ];
    for test_case in test_cases {
        run_async(async {
            let tls = start_postgres_tls().await?;
            let client = connect(&tls.postgres).await?;
            client
                .batch_execute(SETUP)
                .await
                .map_err(|error| error.to_string())?;
            let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
            let config = folder.path().join("sqlload.toml");
            let database = folder.path().join("out.duckdb");
            let settings = test_case
                .settings
                .replace(FILES, &tls.files.path().display().to_string());
            let text = cdc_project(tls.postgres.port, &database).replacen(
                "host = \"127.0.0.1\"",
                &format!("host = \"localhost\"\n{settings}"),
                1,
            );
            std::fs::write(&config, text).map_err(|error| error.to_string())?;
            let snapshot = run_sqlload(&config, &["load", "app"], ("", ""))?;
            client
                .batch_execute(CDC_CHANGES)
                .await
                .map_err(|error| error.to_string())?;
            let streamed = run_sqlload(&config, &["load", "app"], ("", ""))?;
            assert_eq!(
                (
                    snapshot.0,
                    streamed.0,
                    duckdb_text(&database, MERGED_QUERY)?
                ),
                (true, true, test_case.expected_result.to_owned()),
                "{}:\n{}\n{}",
                test_case.description,
                snapshot.1,
                streamed.1
            );
            Ok(())
        })?;
    }
    Ok(())
}
