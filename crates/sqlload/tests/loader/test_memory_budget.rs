//! Rows of 1 MiB loaded into a DuckDB file under a memory budget that gives DuckDB a `memory_limit` of 256MB, which one commit of 300 of them overflows.

use crate::helpers::{duckdb_text, run_async};
use crate::memory_helpers::{
    BUDGET, FEW_WIDE_ROWS, KILLED, LOAD, NEW_BODY, NEW_BODY_KILLED, NEW_BODY_LATER,
    NEW_BODY_TRIPPED, OFF, OFF_DUCKDB_2TB, OFF_DUCKDB_256MB, OFF_PLAN, OFF_TRIPPED, OLDER_MARKER,
    PLAN, REPEATED_KEY, SMALL_ROWS, WIDE_THEN_SMALL, expected_loads, missing_phrases,
    run_wide_load, wide_postgres,
};
use crate::test_types::{RestatedPiecesTestCase, WideLoadTestCase};

#[test]
fn given_300_rows_of_1_mib_when_loading_them_with_a_256mb_duckdb_limit_then_every_strategy_loads_them_in_pieces()
-> Result<(), String> {
    let test_cases = [
        WideLoadTestCase {
            description: "replace builds the table in pieces, the first load and the next, under the budget the plan's memory line shows",
            table: "wide_replace",
            settings: "write_strategy = \"replace\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY, PLAN],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
            ],
            expected_output: &[
                "memory  1.1 GiB budget (--memory-limit); ",
                "; DuckDB 488 MiB (memory_limit 244 MiB, 2 threads); ",
                " at once\n\nLoads (2)",
            ],
        },
        WideLoadTestCase {
            description: "append by an ordered cursor commits pieces straight into the table it creates",
            table: "wide_append",
            settings: "write_strategy = \"append\"\ncursor_column = \"at\"\nappend_cursor_inclusive = false",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD],
            expected_results: &[(true, "300:300:0|2:2:0|0")],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "append without a cursor commits pieces straight into the table it creates",
            table: "wide_append_all",
            settings: "write_strategy = \"append\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD],
            expected_results: &[(true, "300:300:0|2:2:0|0")],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "merge creates the table, then updates every row of it in pieces",
            table: "wide_merge",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = \"at\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY_LATER],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "merge by newest_by updates every row in pieces, each newer than the table's",
            table: "wide_newest",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nnewest_by = \"at\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY_LATER],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "merge sending its rows at commit holds them and sends them a piece at a time",
            table: "wide_held",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nsend = \"at_commit\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "delete_insert by replace_key replaces keys of ten rows in pieces cut between keys",
            table: "wide_keyed",
            settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"k\"]",
            rows: 300,
            key_rows: 10,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "delete_insert by cursor windows replaces each window in pieces cut between cursor values",
            table: "wide_window",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"at\"\nbatch_size = \"1 day\"\ncursor_start = \"2026-10-31T00:00:00Z\"\nlookback = \"100 days\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:300|2:2:0|none"),
            ],
            expected_output: &[],
        },
    ];
    run_async(|| async move {
        let (postgres, client) = wide_postgres().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let (loads, output) =
                run_wide_load((&client, postgres.port), folder.path(), test_case).await?;
            assert_eq!(
                loads,
                expected_loads(test_case.expected_results),
                "{}:\n{output}",
                test_case.description
            );
            assert_eq!(
                missing_phrases(test_case.expected_output, (test_case.table, &output)),
                Vec::<String>::new(),
                "{}: phrases missing from:\n{output}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_table_of_small_rows_when_appending_a_few_wide_ones_then_its_last_row_group_counts_only_the_strings_it_holds()
-> Result<(), String> {
    let test_cases = [
        WideLoadTestCase {
            description: "50,000 rows of one byte, then 20 rows of which five hold 300 KiB: the row group holds 1.5 MiB of large values, not the 20 rows' average width times 50,020 rows",
            table: "wide_few",
            settings: "write_strategy = \"append\"\ncursor_column = \"at\"\nappend_cursor_inclusive = false",
            rows: 2,
            key_rows: 1,
            beside: &["small"],
            steps: &[SMALL_ROWS, FEW_WIDE_ROWS],
            expected_results: &[
                (true, "50000:50000:0|2:2:0|0"),
                (true, "50020:50020:0|2:2:0|0"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "a first window of 40 rows of 300 KiB and ten of one byte, then 29 windows of one-byte rows: the row group holds 12 MiB of large values, not the first window's width times 1,500 rows",
            table: "wide_then_small",
            settings: "write_strategy = \"append\"\ncursor_column = \"id\"\nappend_cursor_inclusive = false\nbatch_size = 50\ncursor_start = \"0\"",
            rows: 2,
            key_rows: 1,
            beside: &["small"],
            steps: &[WIDE_THEN_SMALL],
            expected_results: &[(true, "1500:1500:0|2:2:0|0")],
            expected_output: &[],
        },
    ];
    run_async(|| async move {
        let (postgres, client) = wide_postgres().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let (loads, output) =
                run_wide_load((&client, postgres.port), folder.path(), test_case).await?;
            assert_eq!(
                loads,
                expected_loads(test_case.expected_results),
                "{}:\n{output}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_rows_no_commit_can_hold_when_loading_them_then_only_their_table_stops_naming_the_memory_limit_it_needs()
-> Result<(), String> {
    let test_cases = [
        WideLoadTestCase {
            description: "768 rows of 1 MiB fill one row group past what a checkpoint at 256MB rewrites: the table stops before it, the other loads",
            table: "wide_768",
            settings: "write_strategy = \"replace\"",
            rows: 768,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD],
            expected_results: &[(false, "0:0:0|2:2:0|none")],
            expected_output: &["{table}", "row group", "memory_limit"],
        },
        WideLoadTestCase {
            description: "a merge under on_duplicate_keys = \"fail\" whose 300 MiB repeat a key isn't cut into pieces, which would merge the repeat: its one commit doesn't fit, so it stops, writing nothing",
            table: "wide_repeated",
            settings: "write_strategy = \"merge\"\nunique_key = [\"k\"]\non_duplicate_keys = \"fail\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[REPEATED_KEY],
            expected_results: &[(false, "0:0:0|2:2:0|none")],
            expected_output: &[
                "{table}",
                "refuses repeated keys commits whole",
                "memory_limit",
            ],
        },
        WideLoadTestCase {
            description: "the same merge of 300 MiB without a repeated key stops the same way, writing nothing",
            table: "wide_refusing",
            settings: "write_strategy = \"merge\"\nunique_key = [\"k\"]\non_duplicate_keys = \"fail\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD],
            expected_results: &[(false, "0:0:0|2:2:0|none")],
            expected_output: &[
                "{table}",
                "refuses repeated keys commits whole",
                "memory_limit",
            ],
        },
        WideLoadTestCase {
            description: "a merge under on_duplicate_keys = \"fail\" of 12 rows of 1 MiB, which fit one commit, is refused for its repeated key, writing nothing",
            table: "wide_refused",
            settings: "write_strategy = \"merge\"\nunique_key = [\"k\"]\non_duplicate_keys = \"fail\"",
            rows: 12,
            key_rows: 1,
            beside: &["small"],
            steps: &[REPEATED_KEY],
            expected_results: &[(false, "0:0:0|2:2:0|none")],
            expected_output: &["{table}", "repeat its unique_key", "(k)"],
        },
        WideLoadTestCase {
            description: "one replace_key value of 300 MiB can't be cut between keys: the table stops, keeping its rows",
            table: "wide_one_key",
            settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"k\"]",
            rows: 300,
            key_rows: 1000,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (false, "300:300:0|2:2:0|none"),
            ],
            expected_output: &["{table}", "one k value", "memory_limit"],
        },
        WideLoadTestCase {
            description: "past 90% of the budget the watchdog interrupts the largest table, which rolls back and fails, keeping its rows; the other loads, and the run goes on to its end",
            table: "wide_tripped",
            settings: "write_strategy = \"replace\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, NEW_BODY_TRIPPED],
            expected_results: &[
                (true, "300:300:0|2:2:0|none"),
                (false, "300:300:0|2:2:0|none"),
            ],
            expected_output: &[
                "{table}",
                "90%",
                "memory_limit",
                "batch_size",
                "1 loaded, 1 failed",
            ],
        },
    ];
    run_async(|| async move {
        let (postgres, client) = wide_postgres().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let (loads, output) =
                run_wide_load((&client, postgres.port), folder.path(), test_case).await?;
            assert_eq!(
                loads,
                expected_loads(test_case.expected_results),
                "{}:\n{output}",
                test_case.description
            );
            assert_eq!(
                missing_phrases(test_case.expected_output, (test_case.table, &output)),
                Vec::<String>::new(),
                "{}: phrases missing from:\n{output}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_load_killed_between_its_pieces_when_it_runs_again_then_the_table_is_what_one_clean_load_leaves()
-> Result<(), String> {
    let test_cases = [
        WideLoadTestCase {
            description: "append without a cursor: the killed load's first piece of 61 rows stays marked, and the rerun removes it before loading all 300",
            table: "killed_append_all",
            settings: "write_strategy = \"append\"",
            rows: 300,
            key_rows: 1,
            beside: &[],
            steps: &[KILLED, LOAD],
            expected_results: &[(false, "61:61:0|1"), (true, "300:300:0|0")],
            expected_output: &["killed_append_all: removed the rows of load"],
        },
        WideLoadTestCase {
            description: "append by an unindexed cursor: the rerun removes the killed load's rows and reads from before it",
            table: "killed_unordered",
            settings: "write_strategy = \"append\"\ncursor_column = \"at2\"\nappend_cursor_inclusive = false",
            rows: 300,
            key_rows: 1,
            beside: &[],
            steps: &[KILLED, LOAD],
            expected_results: &[(false, "61:61:0|1"), (true, "300:300:0|0")],
            expected_output: &["killed_unordered: removed the rows of load"],
        },
        WideLoadTestCase {
            description: "append by a text cursor: the rerun removes the killed load's rows and reads from before it",
            table: "killed_text",
            settings: "write_strategy = \"append\"\ncursor_column = \"tag\"\nappend_cursor_inclusive = false",
            rows: 300,
            key_rows: 1,
            beside: &[],
            steps: &[KILLED, LOAD],
            expected_results: &[(false, "61:61:0|1"), (true, "300:300:0|0")],
            expected_output: &["killed_text: removed the rows of load"],
        },
        WideLoadTestCase {
            description: "delete_insert by replace_key: the killed load replaced the 19 rows of its first keys, and the rerun replaces every key",
            table: "killed_keyed",
            settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"k\"]",
            rows: 300,
            key_rows: 10,
            beside: &[],
            steps: &[LOAD, NEW_BODY_KILLED, LOAD],
            expected_results: &[
                (true, "300:300:0|none"),
                (false, "300:300:19|none"),
                (true, "300:300:300|none"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "delete_insert by cursor windows: the killed load replaced the 11 rows of its first window, whose day starts at 02:00 after the lookback, and 20 of the next, and the rerun replaces every window",
            table: "killed_window",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"at\"\nbatch_size = \"1 day\"\ncursor_start = \"2026-10-31T00:00:00Z\"\nlookback = \"100 days\"",
            rows: 300,
            key_rows: 1,
            beside: &[],
            steps: &[LOAD, NEW_BODY_KILLED, LOAD],
            expected_results: &[
                (true, "300:300:0|none"),
                (false, "300:300:31|none"),
                (true, "300:300:300|none"),
            ],
            expected_output: &[],
        },
        WideLoadTestCase {
            description: "a marker of another load removes only that load's three rows, never the rows of the load before it",
            table: "killed_older",
            settings: "write_strategy = \"append\"\ncursor_column = \"at\"\nappend_cursor_inclusive = false",
            rows: 300,
            key_rows: 1,
            beside: &[],
            steps: &[LOAD, OLDER_MARKER],
            expected_results: &[(true, "300:300:0|0"), (true, "300:300:0|0")],
            expected_output: &["killed_older: removed the rows of load older-load"],
        },
    ];
    run_async(|| async move {
        let (postgres, client) = wide_postgres().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let (loads, output) =
                run_wide_load((&client, postgres.port), folder.path(), test_case).await?;
            assert_eq!(
                loads,
                expected_loads(test_case.expected_results),
                "{}:\n{output}",
                test_case.description
            );
            assert_eq!(
                missing_phrases(test_case.expected_output, (test_case.table, &output)),
                Vec::<String>::new(),
                "{}: phrases missing from:\n{output}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_memory_limit_off_when_loading_1_mib_rows_then_they_commit_whole_without_a_watchdog_and_only_duckdbs_own_limit_stops_a_row_group()
-> Result<(), String> {
    let test_cases = [
        WideLoadTestCase {
            description: "a merge refusing repeated keys, which the budget stops as its 300 MiB don't fit one commit, loads in one commit with memory_limit off; plan says off",
            table: "wide_off_refusing",
            settings: "write_strategy = \"merge\"\nunique_key = [\"k\"]\non_duplicate_keys = \"fail\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[LOAD, OFF, OFF_PLAN],
            expected_results: &[
                (false, "0:0:0|2:2:0|none"),
                (true, "300:300:0|2:2:0|none"),
                (true, "300:300:0|2:2:0|none"),
            ],
            expected_output: &["memory  off (--memory-limit): no budget or watchdog"],
        },
        WideLoadTestCase {
            description: "an append without a cursor, which the budget cuts into pieces, commits once with memory_limit off, and leaves no _sqlload_unfinished table",
            table: "wide_off_append",
            settings: "write_strategy = \"append\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[OFF],
            expected_results: &[(true, "300:300:0|2:2:0|none")],
            expected_output: &["2 loaded, 302 rows"],
        },
        WideLoadTestCase {
            description: "no watchdog runs with memory_limit off: a load a watchdog would stop at 90% loads",
            table: "wide_off_watched",
            settings: "write_strategy = \"replace\"",
            rows: 300,
            key_rows: 1,
            beside: &["small"],
            steps: &[OFF_TRIPPED],
            expected_results: &[(true, "300:300:0|2:2:0|none")],
            expected_output: &["2 loaded"],
        },
        WideLoadTestCase {
            description: "with memory_limit off the checkpoint guard stays, against DuckDB's own memory_limit: at 256MB, 768 rows of 1 MiB stop before their row group outgrows it",
            table: "wide_off_768",
            settings: "write_strategy = \"replace\"",
            rows: 768,
            key_rows: 1,
            beside: &["small"],
            steps: &[OFF_DUCKDB_256MB],
            expected_results: &[(false, "0:0:0|2:2:0|none")],
            expected_output: &[
                "{table}",
                "row group",
                "DuckDB's own memory_limit (the run has no memory budget)",
            ],
        },
        WideLoadTestCase {
            description: "DuckDB's own memory_limit reported in TiB is read, and the load runs with memory_limit off",
            table: "wide_off_tib",
            settings: "write_strategy = \"replace\"",
            rows: 4,
            key_rows: 1,
            beside: &["small"],
            steps: &[OFF_DUCKDB_2TB],
            expected_results: &[(true, "4:4:0|2:2:0|none")],
            expected_output: &["2 loaded"],
        },
    ];
    run_async(|| async move {
        let (postgres, client) = wide_postgres().await?;
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let (loads, output) =
                run_wide_load((&client, postgres.port), folder.path(), test_case).await?;
            assert_eq!(
                loads,
                expected_loads(test_case.expected_results),
                "{}:\n{output}",
                test_case.description
            );
            assert_eq!(
                missing_phrases(test_case.expected_output, (test_case.table, &output)),
                Vec::<String>::new(),
                "{}: phrases missing from:\n{output}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_later_file_restating_a_value_an_earlier_piece_committed_when_loading_under_a_budget_then_the_later_file_replaces_it()
-> Result<(), String> {
    let test_cases = [
        RestatedPiecesTestCase {
            description: "files committed together, cut into pieces, keep only the later file's row of 1",
            settings: "",
            expected_result: "70:1:new",
        },
        RestatedPiecesTestCase {
            description: "files committed one at a time, the later one cut into pieces, keep only its row of 1",
            settings: "exclude_columns = [\"_sqlload_source_row\"]\n",
            expected_result: "70:1:new",
        },
    ];
    let body = "x".repeat(1 << 20);
    let rows = |ids: std::ops::RangeInclusive<u32>| -> String {
        ids.map(|id| format!("{id},{body}\n")).collect()
    };
    let files = [
        ("c.csv", format!("id,v\n{}", rows(1..=40)), 1_000),
        ("a.csv", format!("id,v\n{}1,new\n", rows(41..=70)), 2_000),
    ];
    for test_case in &test_cases {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let folder = directory.path().join("in");
        std::fs::create_dir_all(&folder).map_err(|error| error.to_string())?;
        for (name, text, modified_secs) in &files {
            let path = folder.join(name);
            std::fs::write(&path, text).map_err(|error| error.to_string())?;
            let modified = std::time::UNIX_EPOCH + std::time::Duration::from_secs(*modified_secs);
            std::fs::File::options()
                .write(true)
                .open(&path)
                .and_then(|opened| opened.set_modified(modified))
                .map_err(|error| error.to_string())?;
        }
        let database = directory.path().join("restated.duckdb");
        let config = directory.path().join("sqlload.toml");
        std::fs::write(
            &config,
            format!(
                "[connections.files]\nkind = \"filesystem\"\nroot = \"{}\"\n\n[connections.local]\nkind = \"duckdb\"\ndatabase = \"{}\"\n\n[targets.dev]\nconnection = \"local\"\nloader_schema = \"raw\"\n\n[sources.restated]\nkind = \"files\"\nconnection = \"files\"\npath = \"in/\"\nformat = \"csv\"\nwrite_strategy = \"delete_insert\"\nreplace_key = [\"id\"]\non_overlapping_files = \"last_wins\"\n{}",
                directory.path().display(),
                database.display(),
                test_case.settings
            ),
        )
        .map_err(|error| error.to_string())?;
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_sqlload"))
            .arg("--config")
            .arg(&config)
            .args(["--no-color", "load", "--memory-limit", BUDGET])
            .output()
            .map_err(|error| error.to_string())?;
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let result = duckdb_text(
            &database,
            "SELECT count(*) || ':' || count(*) FILTER (WHERE id = 1) || ':' || string_agg(v, ',') FILTER (WHERE length(v) < 10) FROM raw.restated",
        )?;
        assert_eq!(
            result, test_case.expected_result,
            "{}:\n{output}",
            test_case.description
        );
    }
    Ok(())
}
