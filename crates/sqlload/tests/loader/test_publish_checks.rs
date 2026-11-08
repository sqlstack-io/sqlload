use sqlload::adapters::dispatch::main::session::single_text::single_text;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::models::PlanOptions;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::{described_project, run_async};
use crate::postgres_helpers::{checks_project_text, load_once};
use crate::test_types::PostgresTargetTestCase;

const SETUP: &str = "
CREATE TABLE public.customers (id int8, email text);
CREATE TABLE public.events (event_id int8, kind text, at timestamptz NOT NULL);
CREATE TABLE public.tags (tag text);
INSERT INTO public.customers VALUES (1, 'a@x'), (2, 'b@x'), (3, 'c@x'), (4, 'd@x');
INSERT INTO public.events VALUES (1, 'open', '2026-11-01 09:00:00+00');
INSERT INTO public.tags VALUES ('red'), ('blue');
CREATE SCHEMA mart;
";

const CUSTOMERS: &str = "SELECT string_agg(CAST(id AS VARCHAR) || ':' || email, ',' ORDER BY id) || ' / ' || (SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name LIKE '%sqlload_staging%') FROM mart.customers";

#[test]
fn given_tables_with_checks_when_loading_then_failing_rows_are_never_published()
-> Result<(), String> {
    for target in ["app_pg", "local"] {
        run_async(async {
            let postgres = start_postgres().await?;
            let client = connect(&postgres).await?;
            client
                .batch_execute(SETUP)
                .await
                .map_err(|error| error.to_string())?;
            let project = described_project(&checks_project_text(postgres.port, target)).await?;
            let destinations = connect_targets(&project)
                .await
                .map_err(|error| error.to_string())?;
            let destination = destinations.get("out").ok_or("no target out")?;
            let test_cases = [
                PostgresTargetTestCase {
                    description: "rows that pass every check load, and a check under warn only logs",
                    source_change: "",
                    options: PlanOptions::default(),
                    expected_load: "ok",
                    check_query: "SELECT (SELECT string_agg(tag, ',' ORDER BY tag) FROM mart.tags) || ' / ' || (SELECT CAST(count(*) AS VARCHAR) FROM mart.customers)",
                    expected_result: "blue,red / 4",
                },
                PostgresTargetTestCase {
                    description: "a repeated key fails replace, naming an example, and the table keeps its rows with no staging left",
                    source_change: "INSERT INTO public.customers VALUES (2, 'b2@x');",
                    options: PlanOptions::default(),
                    expected_load: "checks.unique failed: 1 value(s) of (id) appear in more than one row, such as (2); nothing was published",
                    check_query: CUSTOMERS,
                    expected_result: "1:a@x,2:b@x,3:c@x,4:d@x / 0",
                },
                PostgresTargetTestCase {
                    description: "a NULL in a not_null column fails replace",
                    source_change: "DELETE FROM public.customers WHERE email = 'b2@x'; UPDATE public.customers SET email = NULL WHERE id = 3;",
                    options: PlanOptions::default(),
                    expected_load: "checks.not_null failed: 1 row(s) have no email",
                    check_query: CUSTOMERS,
                    expected_result: "1:a@x,2:b@x,3:c@x,4:d@x / 0",
                },
                PostgresTargetTestCase {
                    description: "losing more rows than max_row_drop allows fails replace",
                    source_change: "UPDATE public.customers SET email = 'c@x' WHERE id = 3; DELETE FROM public.customers WHERE id > 1;",
                    options: PlanOptions::default(),
                    expected_load: "checks.max_row_drop failed: the table would go from 4 to 1 rows, losing more than 50%",
                    check_query: CUSTOMERS,
                    expected_result: "1:a@x,2:b@x,3:c@x,4:d@x / 0",
                },
                PostgresTargetTestCase {
                    description: "dropping within the limit publishes",
                    source_change: "INSERT INTO public.customers VALUES (2, 'b@x'), (3, 'c@x');",
                    options: PlanOptions::default(),
                    expected_load: "ok",
                    check_query: CUSTOMERS,
                    expected_result: "1:a@x,2:b@x,3:c@x / 0",
                },
                PostgresTargetTestCase {
                    description: "an appended batch failing not_null publishes none of its rows",
                    source_change: "INSERT INTO public.events VALUES (2, 'close', '2026-11-02 09:00:00+00'), (3, NULL, '2026-11-03 09:00:00+00');",
                    options: PlanOptions::default(),
                    expected_load: "checks.not_null failed: 1 row(s) have no kind",
                    check_query: "SELECT string_agg(CAST(event_id AS VARCHAR), ',' ORDER BY event_id) FROM mart.events",
                    expected_result: "1",
                },
                PostgresTargetTestCase {
                    description: "once the source is fixed the next append publishes the batch",
                    source_change: "UPDATE public.events SET kind = 'reopen' WHERE event_id = 3;",
                    options: PlanOptions::default(),
                    expected_load: "ok",
                    check_query: "SELECT string_agg(CAST(event_id AS VARCHAR), ',' ORDER BY event_id) FROM mart.events",
                    expected_result: "1,2,3",
                },
            ];
            for test_case in test_cases {
                client
                    .batch_execute(test_case.source_change)
                    .await
                    .map_err(|error| format!("{}: {error}", test_case.description))?;
                let loaded = load_once(&project, &destinations, &test_case.options)
                    .await
                    .unwrap_or_else(|error| error);
                assert!(
                    loaded.contains(test_case.expected_load),
                    "{target}: {}: {loaded}",
                    test_case.description
                );
                let found = single_text(destination, test_case.check_query.to_owned())
                    .await
                    .map_err(|error| format!("{}: {error}", test_case.description))?;
                assert_eq!(
                    found.unwrap_or_default(),
                    test_case.expected_result,
                    "{target}: {}",
                    test_case.description
                );
            }
            Ok(())
        })?;
    }
    Ok(())
}
