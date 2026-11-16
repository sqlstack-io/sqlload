use sqlload_testkit::postgres::constants::PASSWORD;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::{run_async, words};
use crate::merge_helpers::{hard_steps, run_sync_deletes, soft_steps, sync_deletes_plan};
use crate::test_types::{SyncDeletesRefusalTestCase, SyncDeletesTestCase};

#[test]
fn given_a_merge_with_deletes_when_scanning_its_source_keys_then_only_rows_loaded_before_the_read_whose_key_is_gone_are_marked_within_the_limit()
-> Result<(), String> {
    run_async(async {
        let source = start_postgres().await?;
        let client = connect(&source).await?;
        let postgres = format!(
            "kind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"sqlload\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"",
            source.port
        );
        let test_cases = [
            SyncDeletesTestCase {
                description: "DuckDB, soft: a gone key's row gets _sqlload_deleted_at, and the merge clears it when the key comes back",
                connection: "kind = \"duckdb\"\ndatabase = \":memory:\"".to_owned(),
                schema: "raw",
                reset: &[],
                deletes: "soft",
                marks: "soft-deletes",
                check_query: "SELECT string_agg(CAST(id AS VARCHAR) || ':' || CASE WHEN _sqlload_deleted_at IS NULL THEN 'live' ELSE 'deleted' END, ',' ORDER BY id) FROM raw.app__items",
                expected_steps: soft_steps(),
            },
            SyncDeletesTestCase {
                description: "DuckDB, hard: a gone key's row is removed",
                connection: "kind = \"duckdb\"\ndatabase = \":memory:\"".to_owned(),
                schema: "raw",
                reset: &[],
                deletes: "hard",
                marks: "deletes",
                check_query: "SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) FROM raw.app__items",
                expected_steps: hard_steps(),
            },
            SyncDeletesTestCase {
                description: "Postgres, soft",
                connection: postgres.clone(),
                schema: "items_soft",
                reset: &["DROP SCHEMA IF EXISTS items_soft CASCADE"],
                deletes: "soft",
                marks: "soft-deletes",
                check_query: "SELECT string_agg(id || ':' || CASE WHEN _sqlload_deleted_at IS NULL THEN 'live' ELSE 'deleted' END, ',' ORDER BY id) FROM items_soft.app__items",
                expected_steps: soft_steps(),
            },
            SyncDeletesTestCase {
                description: "Postgres, hard",
                connection: postgres,
                schema: "items_hard",
                reset: &["DROP SCHEMA IF EXISTS items_hard CASCADE"],
                deletes: "hard",
                marks: "deletes",
                check_query: "SELECT string_agg(CAST(id AS text), ',' ORDER BY id) FROM items_hard.app__items",
                expected_steps: hard_steps(),
            },
        ];
        for test_case in &test_cases {
            let steps = run_sync_deletes(test_case, (&client, source.port)).await?;
            assert_eq!(steps, test_case.expected_steps, "{}", test_case.description);
        }
        Ok(())
    })
}

#[test]
fn given_deletes_where_nothing_can_scan_when_planning_then_plan_fails_naming_the_fix()
-> Result<(), String> {
    run_async(async {
        let source = start_postgres().await?;
        let client = connect(&source).await?;
        let test_cases = [
            SyncDeletesRefusalTestCase {
                description: "--sync-deletes when no selected table sets deletes",
                settings: "",
                sync_deletes: true,
                expected_error: "reason --sync-deletes reads every key of each selected table with deletes and marks its rows of the keys the source no longer holds, but none of the selected tables sets deletes └── fix set deletes = \"soft\" or \"hard\" on a merge table, or drop --sync-deletes",
            },
            SyncDeletesRefusalTestCase {
                description: "deletes on a delete_insert, which has no key to match",
                settings: "deletes = \"soft\"",
                sync_deletes: false,
                expected_error: "deletes marks the rows whose unique_key the source no longer holds, which needs write_strategy = \"merge\"; delete_insert keeps no key to match",
            },
        ];
        for test_case in &test_cases {
            let plan = sync_deletes_plan(
                (&client, source.port),
                (test_case.settings, test_case.sync_deletes),
            )
            .await?;
            assert!(
                words(&plan).contains(test_case.expected_error) && plan.contains("Plan failed"),
                "{}: {plan}",
                test_case.description
            );
        }
        Ok(())
    })
}
