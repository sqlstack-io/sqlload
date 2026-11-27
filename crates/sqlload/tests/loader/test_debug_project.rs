use sqlload::loader::main::debug_project::debug_project;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::{described_project, duckdb_project, run_async};
use crate::test_types::DebugProjectTestCase;

#[test]
fn given_a_loader_project_when_debugging_then_every_connection_is_checked_and_reported()
-> Result<(), String> {
    let test_cases = [
        DebugProjectTestCase {
            description: "every target and source answers",
            extra: |_, directory| {
                format!(
                    "destination = \"dev\"\n\n[connections.lake]\nkind = \"filesystem\"\nroot = \"{}\"\n\n[targets.lake]\nconnection = \"lake\"\n",
                    directory.display()
                )
            },
            connect: true,
            expected_lines: &[
                "ok    config  3 connections, 2 targets, 1 sources",
                "ok    target dev  DuckDB v",
                "ok    target lake  ",
                "ok    source app  sqlload@app, PostgreSQL",
            ],
            expected_ok: true,
        },
        DebugProjectTestCase {
            description: "an unreachable source fails without hiding the other checks",
            extra: |port, _| {
                format!(
                    "[connections.down]\nkind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"x\"\ndatabase = \"x\"\n\n[sources.down]\nkind = \"postgres\"\nconnection = \"down\"\ninclude = [\"public.*\"]\n",
                    port + 1
                )
            },
            connect: true,
            expected_lines: &[
                "ok    config  3 connections, 1 targets, 2 sources",
                "ok    target dev  DuckDB v",
                "ok    source app  ",
                "fail  source down  cannot connect to Postgres",
            ],
            expected_ok: false,
        },
        DebugProjectTestCase {
            description: "a BigQuery connection says who it signs in as and through what, never the key, and a key of the wrong type fails",
            extra: |_, _| {
                "[connections.warehouse]\nkind = \"bigquery\"\nlocation = \"EU\"\nimpersonate_service_account = \"reader@other.iam.gserviceaccount.com\"\nservice_account_key = '{\"type\": \"service_account\", \"client_email\": \"loader@demo.iam.gserviceaccount.com\", \"private_key\": \"-----BEGIN PRIVATE KEY-----\", \"token_uri\": \"https://oauth2.googleapis.com/token\", \"project_id\": \"demo\"}'\n\n[connections.wrong]\nkind = \"bigquery\"\nlocation = \"EU\"\nservice_account_key = '{\"type\": \"authorized_user\"}'\n".to_owned()
            },
            connect: true,
            expected_lines: &[
                "ok    config  4 connections, 1 targets, 1 sources",
                "warn  connection warehouse  not used by any target or source",
                "warn  connection wrong  not used by any target or source",
                "ok    connection warehouse  BigQuery signs in with the service-account key of loader@demo.iam.gserviceaccount.com (the connection's service_account_key), impersonating reader@other.iam.gserviceaccount.com",
                "fail  connection wrong  the connection's service_account_key is a \"authorized_user\" credentials file; it takes a service-account key",
                "ok    target dev  DuckDB v",
                "ok    source app  ",
            ],
            expected_ok: false,
        },
        DebugProjectTestCase {
            description: "--no-connection only checks the configuration and flags unused connections",
            extra: |_, _| {
                "[connections.spare]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n".to_owned()
            },
            connect: false,
            expected_lines: &[
                "ok    config  3 connections, 1 targets, 1 sources",
                "warn  connection spare  not used by any target or source",
            ],
            expected_ok: true,
        },
    ];
    run_async(|| async move {
        let postgres = start_postgres().await?;
        let directory =
            std::env::temp_dir().join(format!("sqlload-debug-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
        for test_case in test_cases {
            let text = duckdb_project(postgres.port, &(test_case.extra)(postgres.port, &directory));
            let project = described_project(&text).await?;
            let (lines, ok) = debug_project(&project, test_case.connect).await;
            assert_eq!(
                lines.len(),
                test_case.expected_lines.len(),
                "{}: {lines:?}",
                test_case.description
            );
            assert!(
                lines
                    .iter()
                    .zip(test_case.expected_lines)
                    .all(|(line, start)| line.starts_with(start)),
                "{}: {lines:?}",
                test_case.description
            );
            assert_eq!(ok, test_case.expected_ok, "{}", test_case.description);
        }
        let _ = std::fs::remove_dir_all(&directory);
        Ok(())
    })
}
