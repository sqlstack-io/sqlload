use sqlload_testkit::clickhouse::main::run_client::run_client;
use sqlload_testkit::clickhouse::main::start_clickhouse::start_clickhouse;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::run_async;
use crate::session_helpers::{clickhouse_session_project, load, message};
use crate::test_types::ClickHouseSessionTestCase;

const SETUP: &str = "
CREATE TABLE public.accounts (id int8 PRIMARY KEY, name text NOT NULL, updated_at timestamptz NOT NULL);
INSERT INTO public.accounts VALUES (1, 'Acme', '2026-11-01 10:00:00+00'), (2, 'Beta', '2026-11-02 10:00:00+00');
";

/// The loaded table's rows, `id:name` by id.
const ROWS: &str = "SELECT arrayStringConcat(groupArray(row), ',') FROM (SELECT toString(id) || ':' || name AS row FROM raw.accounts ORDER BY id)";

#[test]
fn given_a_callers_clickhouse_session_when_loading_twice_with_each_write_mode_then_its_publish_steps_run_on_the_session()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [
            ClickHouseSessionTestCase {
                description: "append adds the rows after the cursor",
                settings: "write_strategy = \"append\"\ncursor_column = \"updated_at\"",
                change: "INSERT INTO public.accounts VALUES (3, 'Gamma', '2026-11-03 10:00:00+00')",
                expected_rows: "1:Acme,2:Beta,3:Gamma",
            },
            ClickHouseSessionTestCase {
                description: "replace swaps the whole table in",
                settings: "write_strategy = \"replace\"",
                change: "UPDATE public.accounts SET name = 'Acme Two'; DELETE FROM public.accounts WHERE id = 2",
                expected_rows: "1:Acme Two",
            },
            ClickHouseSessionTestCase {
                description: "merge replaces the rows of the keys it reads, partition by partition",
                settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = \"updated_at\"",
                change: "UPDATE public.accounts SET name = 'Acme Two', updated_at = '2026-11-03 10:00:00+00' WHERE id = 1",
                expected_rows: "1:Acme Two,2:Beta",
            },
            ClickHouseSessionTestCase {
                description: "delete_insert replaces the cursor range it reads",
                settings: "write_strategy = \"delete_insert\"\ncursor_column = \"updated_at\"\nlookback = \"2d\"",
                change: "UPDATE public.accounts SET name = 'Beta Two', updated_at = '2026-11-03 10:00:00+00' WHERE id = 2",
                expected_rows: "1:Acme,2:Beta Two",
            },
        ];
        for test_case in test_cases {
            let postgres = start_postgres().await?;
            let clickhouse = start_clickhouse().await?;
            let client = connect(&postgres).await?;
            client
                .batch_execute(SETUP)
                .await
                .map_err(|error| message(&error))?;
            let project =
                clickhouse_session_project(postgres.port, clickhouse.port, test_case.settings)?;
            let first = load(&project).await?;
            client
                .batch_execute(test_case.change)
                .await
                .map_err(|error| message(&error))?;
            let second = load(&project).await?;
            let rows = run_client(&clickhouse, ROWS, &[]).await?;
            assert_eq!(
                (first.as_str(), second.as_str(), rows.trim()),
                ("ok", "ok", test_case.expected_rows),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
