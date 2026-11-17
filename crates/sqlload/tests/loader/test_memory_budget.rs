//! Rows of 1 MiB loaded into a DuckDB file under a memory budget that gives DuckDB a `memory_limit` of 256MB, which one commit of 300 of them overflows.

use crate::helpers::run_async;
use crate::memory_helpers::{run_killed_load, run_wide_load, wide_postgres};
use crate::test_types::{KilledLoadTestCase, WideLoadTestCase};

/// Every row's body changed, as the second load must write it.
const NEW_BODY: &str = "UPDATE public.{table} SET body = repeat(md5('n' || id::text), 32768);";

/// Every row's body changed and its cursors moved past the last load's.
const NEW_BODY_LATER: &str = "UPDATE public.{table} SET body = repeat(md5('n' || id::text), 32768), at = at + interval '400 days', at2 = at2 + interval '400 days';";

/// The watchdog reads the process as past 90% of the budget while the table named holds rows (test builds only).
const TRIP: &[(&str, &str)] = &[("SQLLOAD_TEST_WATCH_TRIP", "wide_tripped")];

const fn wide(
    description: &'static str,
    settings: &'static str,
    (rows, key_rows): (u32, u32),
    change: Option<&'static str>,
    expected_result: &'static str,
) -> WideLoadTestCase {
    WideLoadTestCase {
        description,
        settings,
        rows,
        key_rows,
        change,
        environment: &[],
        expected_success: true,
        expected_output: &[],
        expected_result,
    }
}

#[test]
fn given_300_rows_of_1_mib_when_loading_them_with_a_256mb_duckdb_limit_then_every_strategy_loads_them_in_pieces()
-> Result<(), String> {
    run_async(|| async move {
        let (postgres, client) = wide_postgres().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let test_cases = [
            (
                "wide_replace",
                wide(
                    "replace builds the table in pieces, the first load and the next",
                    "write_strategy = \"replace\"",
                    (300, 1),
                    Some(NEW_BODY),
                    "300:300:300",
                ),
            ),
            (
                "wide_append",
                wide(
                    "append by an ordered cursor commits pieces straight into the table it creates",
                    "write_strategy = \"append\"\ncursor_column = \"at\"\nappend_cursor_inclusive = false",
                    (300, 1),
                    None,
                    "300:300:0",
                ),
            ),
            (
                "wide_append_all",
                wide(
                    "append without a cursor commits pieces straight into the table it creates",
                    "write_strategy = \"append\"",
                    (300, 1),
                    None,
                    "300:300:0",
                ),
            ),
            (
                "wide_merge",
                wide(
                    "merge creates the table, then updates every row of it in pieces",
                    "write_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = \"at\"",
                    (300, 1),
                    Some(NEW_BODY_LATER),
                    "300:300:300",
                ),
            ),
            (
                "wide_newest",
                wide(
                    "merge by newest_by updates every row in pieces, each newer than the table's",
                    "write_strategy = \"merge\"\nunique_key = [\"id\"]\nnewest_by = \"at\"",
                    (300, 1),
                    Some(NEW_BODY_LATER),
                    "300:300:300",
                ),
            ),
            (
                "wide_keyed",
                wide(
                    "delete_insert by replace_key replaces keys of ten rows in pieces cut between keys",
                    "write_strategy = \"delete_insert\"\nreplace_key = [\"k\"]",
                    (300, 10),
                    Some(NEW_BODY),
                    "300:300:300",
                ),
            ),
            (
                "wide_window",
                wide(
                    "delete_insert by cursor windows replaces each window in pieces cut between cursor values",
                    "write_strategy = \"delete_insert\"\ncursor_column = \"at\"\nbatch_size = \"1 day\"\ncursor_start = \"2026-10-31T00:00:00Z\"\nlookback = \"100 days\"",
                    (300, 1),
                    Some(NEW_BODY),
                    "300:300:300",
                ),
            ),
        ];
        for (name, test_case) in &test_cases {
            run_wide_load((&client, postgres.port), folder.path(), (name, test_case)).await?;
        }
        Ok(())
    })
}

#[test]
fn given_rows_no_commit_can_hold_when_loading_them_then_only_their_table_stops_naming_the_memory_limit_it_needs()
-> Result<(), String> {
    run_async(|| async move {
        let (postgres, client) = wide_postgres().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let test_cases = [
            (
                "wide_768",
                WideLoadTestCase {
                    expected_success: false,
                    expected_output: &["{table}", "row group", "memory_limit"],
                    ..wide(
                        "768 rows of 1 MiB fill one row group past what a checkpoint at 256MB rewrites: the table stops before it, the other loads",
                        "write_strategy = \"replace\"",
                        (768, 1),
                        None,
                        "0:0:0",
                    )
                },
            ),
            (
                "wide_one_key",
                WideLoadTestCase {
                    expected_success: false,
                    expected_output: &["{table}", "one k", "memory_limit"],
                    ..wide(
                        "one replace_key value of 300 MiB can't be cut between keys: the table stops, keeping its rows",
                        "write_strategy = \"delete_insert\"\nreplace_key = [\"k\"]",
                        (300, 1000),
                        Some(NEW_BODY),
                        "300:300:0",
                    )
                },
            ),
            (
                "wide_tripped",
                WideLoadTestCase {
                    environment: TRIP,
                    expected_success: false,
                    expected_output: &["{table}", "90%", "memory_limit", "batch_size"],
                    ..wide(
                        "past 90% of the budget the watchdog interrupts the largest table, which rolls back and fails, keeping its rows; the other loads",
                        "write_strategy = \"replace\"",
                        (300, 1),
                        Some(NEW_BODY),
                        "300:300:0",
                    )
                },
            ),
        ];
        for (name, test_case) in &test_cases {
            run_wide_load((&client, postgres.port), folder.path(), (name, test_case)).await?;
        }
        Ok(())
    })
}

#[test]
fn given_a_load_killed_between_its_pieces_when_it_runs_again_then_the_table_is_what_one_clean_load_leaves()
-> Result<(), String> {
    run_async(|| async move {
        let (postgres, client) = wide_postgres().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let killed = |description, settings, (key_rows, change, older_marker), expected_marker, expected_result| KilledLoadTestCase {
            description,
            settings,
            key_rows,
            change,
            older_marker,
            expected_marker,
            expected_result,
        };
        let test_cases = [
            (
                "killed_append_all",
                killed(
                    "append without a cursor: the rerun removes the killed load's rows first",
                    "write_strategy = \"append\"",
                    (1, None, false),
                    true,
                    "300:300:0",
                ),
            ),
            (
                "killed_unordered",
                killed(
                    "append by an unindexed cursor: the rerun reads from before the killed load",
                    "write_strategy = \"append\"\ncursor_column = \"at2\"\nappend_cursor_inclusive = false",
                    (1, None, false),
                    true,
                    "300:300:0",
                ),
            ),
            (
                "killed_text",
                killed(
                    "append by a text cursor: the rerun reads from before the killed load",
                    "write_strategy = \"append\"\ncursor_column = \"tag\"\nappend_cursor_inclusive = false",
                    (1, None, false),
                    true,
                    "300:300:0",
                ),
            ),
            (
                "killed_keyed",
                killed(
                    "delete_insert by replace_key: the rerun replaces the keys the killed load replaced",
                    "write_strategy = \"delete_insert\"\nreplace_key = [\"k\"]",
                    (10, Some(NEW_BODY), false),
                    false,
                    "300:300:300",
                ),
            ),
            (
                "killed_window",
                killed(
                    "delete_insert by cursor windows: the rerun replaces the windows the killed load replaced",
                    "write_strategy = \"delete_insert\"\ncursor_column = \"at\"\nbatch_size = \"1 day\"\ncursor_start = \"2026-10-31T00:00:00Z\"\nlookback = \"100 days\"",
                    (1, Some(NEW_BODY), false),
                    false,
                    "300:300:300",
                ),
            ),
            (
                "killed_older",
                killed(
                    "a marker of an older load removes only that load's rows, never the rows of the load running",
                    "write_strategy = \"append\"\ncursor_column = \"at\"\nappend_cursor_inclusive = false",
                    (1, None, true),
                    true,
                    "300:300:0",
                ),
            ),
        ];
        for (name, test_case) in &test_cases {
            run_killed_load((&client, postgres.port), folder.path(), (name, test_case)).await?;
        }
        Ok(())
    })
}
