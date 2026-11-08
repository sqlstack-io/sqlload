use std::time::Duration;

use sqlload::loader::main::debug_project::debug_project;
use sqlload_testkit::postgres::constants::USER;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;
use sqlload_testkit::postgres::main::start_postgres_tls::start_postgres_tls;
use sqlload_testkit::ssh::main::start_bastion::start_bastion;
use sqlload_testkit::ssh::models::Bastion;

use crate::helpers::{
    CDC_CHANGES, bridge_address, cdc_project, described_project, duckdb_text, run_async,
    run_sqlload, spawn_sqlload, stop_sqlload, tls_project,
};
use crate::test_postgres_cdc::{MERGED_QUERY, MERGED_RESULT, SETUP};
use crate::test_types::{CdcSshTestCase, SshDebugTestCase, TlsServer};

/// How `debug` starts a source line that connected.
const OK: &str = "ok    source app  ";
/// How `debug` starts a source line that failed.
const FAIL: &str = "fail  source app  ";
/// A tunnel the bastion accepts: the right key, and its host keys known.
const TUNNEL: &str = "ssh = { host = \"127.0.0.1\", port = SSH_PORT, user = \"loader\", private_key_file = \"KEYS/client.key\", known_hosts = \"KEYS/known_hosts\" }";

/// `settings` with its placeholders filled for `bastion` and the test certificates in `files`.
fn filled(settings: &str, bastion: &Bastion, files: &str) -> String {
    settings
        .replace("SSH_PORT", &bastion.port.to_string())
        .replace("KEYS", &bastion.files.path().display().to_string())
        .replace("FILES", files)
}

/// `debug` through a bastion: which hop failed, and what protects the connection when it opens; TLS runs end to end with the database, whose own name `verify-full` checks.
#[test]
fn given_a_bastion_when_debugging_then_it_names_the_hop_that_failed() -> Result<(), String> {
    let test_cases = [
        SshDebugTestCase {
            description: "a known bastion and an accepted key reach a plain server",
            server: TlsServer::Plain,
            port: 5432,
            user: USER,
            settings: TUNNEL,
            expected_status: OK,
            expected_text: ", not encrypted, via ssh loader@127.0.0.1:",
        },
        SshDebugTestCase {
            description: "TLS runs end to end through the tunnel",
            server: TlsServer::TlsOnly,
            port: 5432,
            user: USER,
            settings: "sslmode = \"verify-ca\"\nsslrootcert = \"FILES/ca.pem\"\nssh = { host = \"127.0.0.1\", port = SSH_PORT, user = \"loader\", private_key_file = \"KEYS/client.key\", known_hosts = \"KEYS/known_hosts\" }",
            expected_status: OK,
            expected_text: ", TLS, CA checked, via ssh loader@127.0.0.1:",
        },
        SshDebugTestCase {
            description: "verify-full checks the database's name, not the bastion's",
            server: TlsServer::TlsOnly,
            port: 5432,
            user: USER,
            settings: "sslmode = \"verify-full\"\nsslrootcert = \"FILES/ca.pem\"\nssh = { host = \"127.0.0.1\", port = SSH_PORT, user = \"loader\", private_key_file = \"KEYS/client.key\", known_hosts = \"KEYS/known_hosts\" }",
            expected_status: FAIL,
            expected_text: "certificate not valid for name",
        },
        SshDebugTestCase {
            description: "an unknown bastion is refused with its fingerprint",
            server: TlsServer::Plain,
            port: 5432,
            user: USER,
            settings: "ssh = { host = \"127.0.0.1\", port = SSH_PORT, user = \"loader\", private_key_file = \"KEYS/client.key\", known_hosts = \"KEYS/empty_hosts\" }",
            expected_status: FAIL,
            expected_text: "host key SHA256:",
        },
        SshDebugTestCase {
            description: "a bastion whose key changed is refused",
            server: TlsServer::Plain,
            port: 5432,
            user: USER,
            settings: "ssh = { host = \"127.0.0.1\", port = SSH_PORT, user = \"loader\", private_key_file = \"KEYS/client.key\", known_hosts = \"KEYS/changed_hosts\" }",
            expected_status: FAIL,
            expected_text: "host key changed",
        },
        SshDebugTestCase {
            description: "a key the bastion does not know is refused",
            server: TlsServer::Plain,
            port: 5432,
            user: USER,
            settings: "ssh = { host = \"127.0.0.1\", port = SSH_PORT, user = \"loader\", private_key_file = \"KEYS/other.key\", known_hosts = \"KEYS/known_hosts\" }",
            expected_status: FAIL,
            expected_text: "the bastion refused loader's key",
        },
        SshDebugTestCase {
            description: "a database the bastion cannot reach",
            server: TlsServer::Plain,
            port: 5433,
            user: USER,
            settings: TUNNEL,
            expected_status: FAIL,
            expected_text: "the bastion cannot open",
        },
        SshDebugTestCase {
            description: "a database that refuses the user, past the tunnel",
            server: TlsServer::Plain,
            port: 5432,
            user: "nobody",
            settings: TUNNEL,
            expected_status: FAIL,
            expected_text: ":5432/app via ssh loader@127.0.0.1:",
        },
    ];
    run_async(async {
        let bastion = start_bastion().await?;
        let tls = start_postgres_tls().await?;
        let plain = start_postgres().await?;
        let hosts = [
            bridge_address(&tls.postgres).await?,
            bridge_address(&plain).await?,
        ];
        let files = tls.files.path().display().to_string();
        for test_case in test_cases {
            let settings = filled(test_case.settings, &bastion, &files);
            let host = &hosts[test_case.server as usize];
            let text = tls_project((host, test_case.port, test_case.user), &settings);
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

/// A `postgres_cdc` source behind a bastion: the plan names the route, the snapshot reads through the tunnel and the changes stream through the relay over it.
#[test]
fn given_a_bastion_when_capturing_changes_then_they_land_once() -> Result<(), String> {
    let test_cases = [
        CdcSshTestCase {
            description: "a plain server with TLS off, still relayed through the tunnel",
            server: TlsServer::Plain,
            settings: "sslmode = \"disable\"",
            expected_result: MERGED_RESULT,
        },
        CdcSshTestCase {
            description: "a TLS-only server, its CA checked end to end",
            server: TlsServer::TlsOnly,
            settings: "sslmode = \"verify-ca\"\nsslrootcert = \"FILES/ca.pem\"",
            expected_result: MERGED_RESULT,
        },
    ];
    for test_case in test_cases {
        run_async(async {
            let bastion = start_bastion().await?;
            let tls = start_postgres_tls().await?;
            let plain = start_postgres().await?;
            let server = [&tls.postgres, &plain][test_case.server as usize];
            let client = connect(server).await?;
            client
                .batch_execute(SETUP)
                .await
                .map_err(|error| error.to_string())?;
            let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
            let config = folder.path().join("sqlload.toml");
            let database = folder.path().join("out.duckdb");
            let settings = filled(
                &format!("{}\n{TUNNEL}", test_case.settings),
                &bastion,
                &tls.files.path().display().to_string(),
            );
            let text = cdc_project(server.port, &database).replacen(
                &format!("host = \"127.0.0.1\"\nport = {}", server.port),
                &format!(
                    "host = \"{}\"\nport = 5432\n{settings}",
                    bridge_address(server).await?
                ),
                1,
            );
            std::fs::write(&config, text).map_err(|error| error.to_string())?;
            let plan = run_sqlload(&config, &["plan", "app"], ("", ""))?;
            let snapshot = run_sqlload(&config, &["load", "app"], ("", ""))?;
            client
                .batch_execute(CDC_CHANGES)
                .await
                .map_err(|error| error.to_string())?;
            let streamed = run_sqlload(&config, &["load", "app"], ("", ""))?;
            let route = format!("via ssh loader@127.0.0.1:{}", bastion.port);
            assert_eq!(
                (
                    plan.1.contains(&route),
                    snapshot.0,
                    streamed.0,
                    duckdb_text(&database, MERGED_QUERY)?
                ),
                (true, true, true, test_case.expected_result.to_owned()),
                "{}:\n{}\n{}\n{}",
                test_case.description,
                plan.1,
                snapshot.1,
                streamed.1
            );
            Ok(())
        })?;
    }
    Ok(())
}

/// A tunnel that drops in the middle of a read fails the table with an error naming the tunnel, not the database.
#[test]
fn given_a_load_through_a_bastion_when_the_tunnel_drops_then_the_table_fails_naming_it()
-> Result<(), String> {
    run_async(async {
        let bastion = start_bastion().await?;
        let plain = start_postgres().await?;
        connect(&plain)
            .await?
            .batch_execute(
                "CREATE VIEW public.slow AS SELECT g AS id FROM generate_series(1, 2000) AS g, LATERAL (SELECT pg_sleep(0.01)) AS pause;
                 COMMENT ON VIEW public.slow IS 'Rows that take 20 seconds to read.';",
            )
            .await
            .map_err(|error| error.to_string())?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let config = folder.path().join("sqlload.toml");
        let text = tls_project(
            (&bridge_address(&plain).await?, 5432, USER),
            &filled(TUNNEL, &bastion, ""),
        )
        .replace(
            ":memory:",
            &folder.path().join("out.duckdb").display().to_string(),
        );
        std::fs::write(&config, text).map_err(|error| error.to_string())?;
        let load = spawn_sqlload(&config, &["load", "app"], "")?;
        tokio::time::sleep(Duration::from_secs(6)).await;
        bastion
            .container
            .stop()
            .await
            .map_err(|error| error.to_string())?;
        let (succeeded, output) = stop_sqlload(load, "-0").await?;
        let route = format!("the SSH tunnel through ssh loader@127.0.0.1:{} closed", bastion.port);
        assert!(!succeeded && output.contains(&route), "{output}");
        Ok(())
    })
}
