use sqlload_testkit::postgres::constants::PASSWORD;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::run_async;
use crate::merge_helpers::{load_step, run_merge_case};
use crate::test_types::{FilesMergeTestCase, MergeFile, MergeStep};

/// Every row as `version:line:amount`, then how many file commits the run log records.
const CHECK: &str = "SELECT (SELECT string_agg(COALESCE(le_version, 'NULL') || ':' || line || ':' || CAST(amount AS VARCHAR), ',' ORDER BY le_version NULLS LAST, line) FROM raw.scores) || '|' || (SELECT CAST(count(*) AS VARCHAR) FROM raw._sqlload_runs WHERE table_name = 'scores' AND event = 'run')";

/// No table is created when plan refuses the settings.
const NO_TABLE: &str =
    "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'scores'";

const LAST_WINS: &str = "write_strategy = \"delete_insert\"\nreplace_key = [\"le_version\"]\non_overlapping_files = \"last_wins\"\n";

const LAST_WINS_BY_PATH: &str = "write_strategy = \"delete_insert\"\nreplace_key = [\"le_version\"]\non_overlapping_files = \"last_wins\"\norder = \"path\"\n";

const FAILS: &str = "write_strategy = \"delete_insert\"\nreplace_key = [\"le_version\"]\non_overlapping_files = \"fail\"\n";

/// P01 and P02, sent first.
const FIRST: MergeFile = MergeFile {
    key: "first.csv",
    contents: "le_version,line,amount\nP01,A,1\nP01,B,2\nP02,A,10\n",
    modified_secs: 1_000,
};

/// P02 restated with other lines, and the new P03, sent later.
const SECOND: MergeFile = MergeFile {
    key: "second.csv",
    contents: "le_version,line,amount\nP02,X,7\nP02,Y,8\nP03,Z,9\n",
    modified_secs: 2_000,
};

const LAST_WINS_PLAN: &str = "2 files, one commit each, oldest modified first (ties by path), each recorded in the run log; the last file holding a replace_key value replaces it (on_overlapping_files = \"last_wins\")";

const OUTSIDE: &str = "on_overlapping_files says which file wins when two files of a files source's folder hold one replace_key value";

const ONE_RUN: &[MergeStep] = &[load_step(&[FIRST, SECOND], "", LAST_WINS_PLAN)];

const TWO_RUNS: &[MergeStep] = &[load_step(&[FIRST], "", ""), load_step(&[SECOND], "", "")];

/// The later of the two dated files was modified first.
const BY_PATH: &[MergeStep] = &[load_step(
    &[
        MergeFile {
            key: "sales_2026-11-14.csv",
            contents: "le_version,line,amount\nP02,X,7\n",
            modified_secs: 1_000,
        },
        MergeFile {
            key: "sales_2026-11-07.csv",
            contents: "le_version,line,amount\nP01,A,1\nP02,A,10\n",
            modified_secs: 2_000,
        },
    ],
    "",
    "in path order, each recorded in the run log; the last file holding a replace_key value replaces it",
)];

const SPLIT: &[MergeStep] = &[load_step(
    &[FIRST, SECOND],
    "drop/second.csv: it holds the replace_key (le_version) value (P02), which first.csv also holds, loaded earlier in this run; each file replaces the rows of the values it holds, so it would delete the rows first.csv loaded, as a value split across part files would keep only its last part. One file must hold all of a value's rows. Fix: if the files are parts of one export, put all the rows of each (le_version) value in one file; if newer files restate older ones, set on_overlapping_files = \"last_wins\", so the file last in order replaces the values, or remove the older of first.csv and second.csv from the folder by hand and set after_load = { move_to = ... } (or \"delete\"), so that each file leaves the folder once it loads",
    "",
)];

const REFUSED: &[MergeStep] = &[load_step(&[FIRST], OUTSIDE, "")];

/// The newer file loads first, and the older one, by path, arrives in a later run.
const SENT_LATE: &[MergeStep] = &[
    load_step(
        &[MergeFile {
            key: "sales_2026-11-14.csv",
            contents: "le_version,line,amount\nP02,X,7\n",
            modified_secs: 1_000,
        }],
        "",
        "",
    ),
    load_step(
        &[MergeFile {
            key: "sales_2026-11-07.csv",
            contents: "le_version,line,amount\nP01,A,1\nP02,A,10\n",
            modified_secs: 2_000,
        }],
        "drop/sales_2026-11-07.csv: the table holds rows of its replace_key (le_version) values from sales_2026-11-14.csv, which is still in the folder and comes after sales_2026-11-07.csv in order; with on_overlapping_files = \"last_wins\" the file last in order keeps a value, so sales_2026-11-07.csv would take back values a later file replaced",
        "",
    ),
];

/// One file of P01 and P02, merged by key.
const MERGED: &[MergeStep] = &[load_step(&[FIRST], "", "")];

#[test]
fn given_files_holding_one_replace_key_value_when_loading_into_duckdb_then_on_overlapping_files_says_which_wins()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [
            FilesMergeTestCase {
                description: "in one run, the later file replaces the value both hold, and the earlier file's other values stay",
                settings: LAST_WINS,
                steps: ONE_RUN,
                check_query: CHECK,
                expected_result: "P01:A:1,P01:B:2,P02:X:7,P02:Y:8,P03:Z:9|2",
            },
            FilesMergeTestCase {
                description: "across runs, a file sent later replaces a value of a file still in the folder",
                settings: LAST_WINS,
                steps: TWO_RUNS,
                check_query: CHECK,
                expected_result: "P01:A:1,P01:B:2,P02:X:7,P02:Y:8,P03:Z:9|2",
            },
            FilesMergeTestCase {
                description: "with order = \"path\", the file last by path wins, though it was modified first",
                settings: LAST_WINS_BY_PATH,
                steps: BY_PATH,
                check_query: CHECK,
                expected_result: "P01:A:1,P02:X:7|2",
            },
            FilesMergeTestCase {
                description: "with on_overlapping_files = \"fail\", a value split across part files still fails on the later file, naming both and the setting",
                settings: FAILS,
                steps: SPLIT,
                check_query: CHECK,
                expected_result: "P01:A:1,P01:B:2,P02:A:10|1",
            },
            FilesMergeTestCase {
                description: "with order = \"path\", an older file sent after a newer one fails instead of taking back the newer file's value, which stays",
                settings: LAST_WINS_BY_PATH,
                steps: SENT_LATE,
                check_query: CHECK,
                expected_result: "P02:X:7|1",
            },
            FilesMergeTestCase {
                description: "set on the source, it leaves alone a table of the source that merges",
                settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"le_version\"]\non_overlapping_files = \"last_wins\"\n[sources.scores.tables.scores]\nwrite_strategy = \"merge\"\nunique_key = [\"le_version\", \"line\"]\n",
                steps: MERGED,
                check_query: CHECK,
                expected_result: "P01:A:1,P01:B:2,P02:A:10|1",
            },
            FilesMergeTestCase {
                description: "plan refuses it on a merge, where the last file already wins each key",
                settings: "write_strategy = \"merge\"\nunique_key = [\"le_version\", \"line\"]\non_overlapping_files = \"last_wins\"\n",
                steps: REFUSED,
                check_query: NO_TABLE,
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "plan refuses it on an append",
                settings: "write_strategy = \"append\"\non_overlapping_files = \"fail\"\n",
                steps: REFUSED,
                check_query: NO_TABLE,
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "plan refuses it on a delete_insert by cursor",
                settings: "write_strategy = \"delete_insert\"\ncursor_column = \"le_version\"\non_overlapping_files = \"last_wins\"\n",
                steps: REFUSED,
                check_query: NO_TABLE,
                expected_result: "0",
            },
        ];
        for test_case in &test_cases {
            let result = run_merge_case(
                ("kind = \"duckdb\"\ndatabase = \":memory:\"", "raw"),
                test_case,
            )
            .await?;
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

#[test]
fn given_files_holding_one_replace_key_value_when_loading_into_postgres_then_on_overlapping_files_says_which_wins()
-> Result<(), String> {
    run_async(|| async move {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let target = format!(
            "kind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"sqlload\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"",
            postgres.port
        );
        let test_cases = [
            FilesMergeTestCase {
                description: "in one run, the later file replaces the value both hold, and the earlier file's other values stay",
                settings: LAST_WINS,
                steps: ONE_RUN,
                check_query: CHECK,
                expected_result: "P01:A:1,P01:B:2,P02:X:7,P02:Y:8,P03:Z:9|2",
            },
            FilesMergeTestCase {
                description: "across runs, a file sent later replaces a value of a file still in the folder",
                settings: LAST_WINS,
                steps: TWO_RUNS,
                check_query: CHECK,
                expected_result: "P01:A:1,P01:B:2,P02:X:7,P02:Y:8,P03:Z:9|2",
            },
            FilesMergeTestCase {
                description: "with on_overlapping_files = \"fail\", a value split across part files still fails on the later file",
                settings: FAILS,
                steps: SPLIT,
                check_query: CHECK,
                expected_result: "P01:A:1,P01:B:2,P02:A:10|1",
            },
        ];
        for test_case in &test_cases {
            client
                .batch_execute("DROP SCHEMA IF EXISTS raw CASCADE")
                .await
                .map_err(|error| error.to_string())?;
            let result = run_merge_case((&target, "raw"), test_case).await?;
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
