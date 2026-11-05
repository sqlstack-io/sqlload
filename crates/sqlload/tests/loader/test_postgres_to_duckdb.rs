use sqlload::adapters::duckdb::main::query_text::query_text;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::{Destination, PlanOptions};
use sqlload::project::main::parse_project::parse_project;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::{duckdb_project, load_all, run_async};
use crate::test_types::LoadRunTestCase;

const SETUP: &str = "
CREATE TABLE public.accounts (id int8 PRIMARY KEY, name text, email text, balance numeric(12,2), updated_at timestamptz NOT NULL);
CREATE TABLE public.events (event_id int8, kind text, at timestamptz NOT NULL);
CREATE TABLE public.stores (code text, city text);
INSERT INTO public.accounts VALUES
  (1, 'Acme Ltd', 'a@example.com', 10.50, '2026-11-01 10:00:00+00'),
  (2, 'Beta Co', 'b@example.com', 20.00, '2026-11-02 10:00:00+00');
INSERT INTO public.events VALUES (1, 'open', '2026-11-01 09:00:00+00'), (2, 'close', '2026-11-02 09:00:00+00');
INSERT INTO public.stores VALUES ('L1', 'Leeds'), ('Y1', 'York');
";

const TABLES: &str = r#"
[sources.app.tables."public.accounts"]
cursor_column = "updated_at"
lookback = "1h"

[sources.app.tables."public.events"]
write_strategy = "delete_insert"
cursor_column = "at"

[sources.app.tables."public.stores"]
mask = { city = "hash" }
"#;

#[test]
fn given_postgres_tables_when_loading_repeatedly_into_duckdb_then_each_strategy_keeps_the_table_right()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let text = duckdb_project(postgres.port, TABLES);
        let project = parse_project(&text, &|_| None).map_err(|error| error.to_string())?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let Some(Destination::DuckDb(duckdb)) = destinations.get("dev") else {
            return Err("no DuckDB destination".to_owned());
        };
        let test_cases = [
            LoadRunTestCase {
                description: "the first load creates every table without excluded columns",
                source_change: "",
                check_query: "SELECT string_agg(table_name || '.' || column_name, ',' ORDER BY table_name, ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'app__accounts'",
                expected_result: "app__accounts.id,app__accounts.name,app__accounts.balance,app__accounts.updated_at,app__accounts._sqlload_load_id,app__accounts._sqlload_loaded_at",
            },
            LoadRunTestCase {
                description: "merge with a cursor upserts changed rows and keeps the rest",
                source_change: "UPDATE public.accounts SET balance = 99.99, updated_at = '2026-11-03 10:00:00+00' WHERE id = 2; INSERT INTO public.accounts VALUES (3, 'Gamma', 'g@example.com', 1.00, '2026-11-03 11:00:00+00');",
                check_query: "SELECT string_agg(id || '=' || balance, ',' ORDER BY id) FROM raw.app__accounts",
                expected_result: "1=10.50,2=99.99,3=1.00",
            },
            LoadRunTestCase {
                description: "a rerun with no changes leaves exactly one row per key",
                source_change: "",
                check_query: "SELECT count(*) || '/' || count(DISTINCT id) FROM raw.app__accounts",
                expected_result: "3/3",
            },
            LoadRunTestCase {
                description: "delete_insert replaces the rows from the cursor onwards without duplicates",
                source_change: "INSERT INTO public.events VALUES (3, 'reopen', '2026-11-02 09:00:00+00');",
                check_query: "SELECT string_agg(event_id || ':' || kind, ',' ORDER BY event_id) FROM raw.app__events",
                expected_result: "1:open,2:close,3:reopen",
            },
            LoadRunTestCase {
                description: "replace without a cursor mirrors deletes in the source",
                source_change: "DELETE FROM public.stores WHERE code = 'Y1';",
                check_query: "SELECT string_agg(code, ',' ORDER BY code) FROM raw.app__stores",
                expected_result: "L1",
            },
            LoadRunTestCase {
                description: "a masked column holds the SHA-256 hex of each value",
                source_change: "",
                check_query: "SELECT string_agg(code || '@' || city, ',' ORDER BY code) FROM raw.app__stores",
                expected_result: "L1@8593b9db65212160979d946950601c26622a219c80f1f122679eb69ec5b96600",
            },
            LoadRunTestCase {
                description: "a new source column is added to the table and filled",
                source_change: "ALTER TABLE public.stores ADD COLUMN opened date; UPDATE public.stores SET opened = '2020-01-02';",
                check_query: "SELECT string_agg(code || '@' || CAST(opened AS VARCHAR), ',') FROM raw.app__stores",
                expected_result: "L1@2020-01-02",
            },
            LoadRunTestCase {
                description: "every row carries the metadata of the load that wrote it",
                source_change: "",
                check_query: "SELECT CAST(count(*) FILTER (WHERE _sqlload_load_id IS NULL OR _sqlload_loaded_at IS NULL) AS VARCHAR) FROM raw.app__stores",
                expected_result: "0",
            },
        ];
        for test_case in test_cases {
            client
                .batch_execute(test_case.source_change)
                .await
                .map_err(|error| error.to_string())?;
            let plan = plan_project(&project, &destinations, &PlanOptions::default())
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            let (lines, ok) = render_plan(&plan);
            assert!(ok, "{}:\n{}", test_case.description, lines.join("\n"));
            apply_project(&plan, &destinations)
                .await
                .map_err(|error| error.to_string())?;
            load_all(&project, &plan, &destinations)
                .await
                .map_err(|error| format!("{}: {error}", test_case.description))?;
            let result = query_text(duckdb, test_case.check_query.to_owned())
                .await
                .map_err(|error| error.to_string())?;
            assert_eq!(
                result.as_deref(),
                Some(test_case.expected_result),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
