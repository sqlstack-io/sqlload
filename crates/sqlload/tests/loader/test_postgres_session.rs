use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::run_async;
use crate::session_helpers::{load, message, session_project};
use crate::test_types::PostgresSessionTestCase;

const SETUP: &str = "
CREATE TABLE public.accounts (id int8 PRIMARY KEY, name text, updated_at timestamptz NOT NULL);
INSERT INTO public.accounts VALUES (1, 'Acme', '2026-11-01 10:00:00+00'), (2, 'Beta', '2026-11-02 10:00:00+00');
CREATE SCHEMA raw;
";

#[test]
fn given_a_callers_postgres_session_when_loading_twice_then_rows_copy_in_and_the_checks_before_a_write_run_on_it()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [
            PostgresSessionTestCase {
                description: "merge copies the rows in, matching the key the table's unique index holds",
                settings: "write_strategy = \"merge\"\nunique_key = \"{primary_key}\"\ncursor_column = \"updated_at\"",
                change: "UPDATE public.accounts SET name = 'Acme Two', updated_at = '2026-11-03 10:00:00+00' WHERE id = 1",
                expected_second: "ok",
                expected_rows: &["1:Acme Two", "2:Beta"],
            },
            PostgresSessionTestCase {
                description: "replace is refused while a foreign key would cascade its deletes into another table",
                settings: "write_strategy = \"replace\"",
                change: "ALTER TABLE raw.accounts ADD PRIMARY KEY (id); CREATE TABLE public.notes (account_id int8 REFERENCES raw.accounts (id) ON DELETE CASCADE)",
                expected_second: "ON DELETE CASCADE",
                expected_rows: &["1:Acme", "2:Beta"],
            },
        ];
        for test_case in test_cases {
            let postgres = start_postgres().await?;
            let client = connect(&postgres).await?;
            client
                .batch_execute(SETUP)
                .await
                .map_err(|error| message(&error))?;
            let project = session_project(postgres.port, test_case.settings)?;
            let first = load(&project).await?;
            client
                .batch_execute(test_case.change)
                .await
                .map_err(|error| message(&error))?;
            let second = load(&project).await.unwrap_or_else(|blocked| blocked);
            let rows: Vec<String> = client
                .query(
                    "SELECT id::text || ':' || name FROM raw.accounts ORDER BY id",
                    &[],
                )
                .await
                .map_err(|error| error.to_string())?
                .iter()
                .map(|row| row.get(0))
                .collect();
            assert_eq!(first, "ok", "{}", test_case.description);
            assert!(
                second.contains(test_case.expected_second),
                "{}: {second}",
                test_case.description
            );
            assert_eq!(rows, test_case.expected_rows, "{}", test_case.description);
        }
        Ok(())
    })
}
