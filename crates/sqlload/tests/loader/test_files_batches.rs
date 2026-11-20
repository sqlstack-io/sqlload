use sqlload_testkit::postgres::constants::PASSWORD;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::run_async;
use crate::merge_helpers::{run_batch_case, run_many_files_case};
use crate::test_types::{BatchFile, FileBatchTestCase, ManyFilesTestCase};

const MERGE: &str = "write_strategy = \"merge\"\nunique_key = [\"id\"]\n";

const NEWEST_BY: &str = "write_strategy = \"merge\"\nunique_key = [\"id\"]\nnewest_by = \"seen\"\n";

const REPLACE_LAST_WINS: &str = "write_strategy = \"delete_insert\"\nreplace_key = [\"id\"]\non_overlapping_files = \"last_wins\"\n";

const REPLACE_UNNUMBERED: &str = "write_strategy = \"delete_insert\"\nreplace_key = [\"id\"]\non_overlapping_files = \"last_wins\"\nexclude_columns = [\"_sqlload_source_row\"]\n";

const REPLACE_BY_V: &str = "write_strategy = \"delete_insert\"\nreplace_key = [\"v\"]\non_overlapping_files = \"last_wins\"\n";

/// Keys repeated inside a file (1 in `c`, 4 in `a`, 2 in `d`) and across files, whose names sort unlike their modified times.
const REPEATS: &[BatchFile] = &[
    BatchFile {
        key: "c",
        rows: &[(1, "a1", 1), (2, "a2", 1), (1, "a1b", 1), (3, "a3", 1)],
        modified_secs: 1_000,
    },
    BatchFile {
        key: "a",
        rows: &[(2, "b2", 2), (4, "b4", 2), (4, "b4b", 2)],
        modified_secs: 2_000,
    },
    BatchFile {
        key: "b",
        rows: &[(1, "c1", 3), (5, "c5", 3)],
        modified_secs: 3_000,
    },
    BatchFile {
        key: "d",
        rows: &[(3, "d3", 4), (2, "d2", 4), (2, "d2b", 4)],
        modified_secs: 4_000,
    },
];

/// Under `newest_by = "seen"`: a later file's older row of 1 loses, its tie with the first file's row of 1 wins, and a tie inside a file goes to its last row.
const NEWEST: &[BatchFile] = &[
    BatchFile {
        key: "c",
        rows: &[(1, "a1", 5), (2, "a2", 1)],
        modified_secs: 1_000,
    },
    BatchFile {
        key: "a",
        rows: &[(1, "b1", 3), (2, "b2", 1), (2, "b2b", 1)],
        modified_secs: 2_000,
    },
    BatchFile {
        key: "b",
        rows: &[(1, "c1", 5), (3, "c3", 1)],
        modified_secs: 3_000,
    },
];

/// Values of `replace_key` restated by later files: 2 by `a`, then 1 by `b`, while `c` keeps both rows of its own 1 until then.
const RESTATED: &[BatchFile] = &[
    BatchFile {
        key: "c",
        rows: &[(1, "a1", 1), (1, "a1b", 1), (2, "a2", 1)],
        modified_secs: 1_000,
    },
    BatchFile {
        key: "a",
        rows: &[(2, "b2", 2), (3, "b3", 2)],
        modified_secs: 2_000,
    },
    BatchFile {
        key: "b",
        rows: &[(1, "c1", 3)],
        modified_secs: 3_000,
    },
];

const REPLACE_BY_UUID: &str = "write_strategy = \"delete_insert\"\nreplace_key = [\"v\"]\non_overlapping_files = \"last_wins\"\n[sources.{name}.tables.{name}]\ncolumns = { v = \"uuid\" }\n";

/// One `uuid` value written in capitals, then by a later file in small letters: one value, which only differs as text.
const UUID_CASES: &[BatchFile] = &[
    BatchFile {
        key: "c",
        rows: &[(1, "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", 1)],
        modified_secs: 1_000,
    },
    BatchFile {
        key: "a",
        rows: &[(2, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", 2)],
        modified_secs: 2_000,
    },
];

/// A row without a `v`, then a later file's row whose `v` is the text `NULL`: two values, which only look alike as text.
const NULL_TEXT: &[BatchFile] = &[
    BatchFile {
        key: "c",
        rows: &[(1, "", 1)],
        modified_secs: 1_000,
    },
    BatchFile {
        key: "a",
        rows: &[(2, "NULL", 2)],
        modified_secs: 2_000,
    },
];

#[test]
fn given_files_loaded_in_one_commit_when_loading_into_duckdb_then_the_rows_match_loading_them_file_by_file()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [
            FileBatchTestCase {
                description: "a CSV merge keeps the newest file's row of each key, and the last row of a key a file repeats",
                settings: MERGE,
                format: "csv",
                files: REPEATS,
                expected_result: "0|0|1:c1,2:d2b,3:d3,4:b4b,5:c5|1|4",
            },
            FileBatchTestCase {
                description: "a Parquet merge, which DuckDB reads itself, keeps the same rows",
                settings: MERGE,
                format: "parquet",
                files: REPEATS,
                expected_result: "0|0|1:c1,2:d2b,3:d3,4:b4b,5:c5|1|4",
            },
            FileBatchTestCase {
                description: "a merge with newest_by keeps the newest row of each key, a tie going to the later file",
                settings: NEWEST_BY,
                format: "csv",
                files: NEWEST,
                expected_result: "0|0|1:c1,2:b2b,3:c3|1|3",
            },
            FileBatchTestCase {
                description: "a CSV delete_insert by replace_key under last_wins keeps each value's rows from the last file holding it",
                settings: REPLACE_LAST_WINS,
                format: "csv",
                files: RESTATED,
                expected_result: "0|0|1:c1,2:b2,3:b3|1|3",
            },
            FileBatchTestCase {
                description: "under last_wins a later file replaces only its own values, so a NULL value stays beside the text NULL",
                settings: REPLACE_BY_V,
                format: "csv",
                files: NULL_TEXT,
                expected_result: "0|0|2:NULL|1|2",
            },
            FileBatchTestCase {
                description: "under last_wins a later file replaces a uuid value however its letters are written, as the table compares them",
                settings: REPLACE_BY_UUID,
                format: "csv",
                files: UUID_CASES,
                expected_result: "0|0|2:a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11|1|2",
            },
            FileBatchTestCase {
                description: "without _sqlload_source_row, a delete_insert by replace_key under last_wins commits each file, keeping the same rows",
                settings: REPLACE_UNNUMBERED,
                format: "csv",
                files: RESTATED,
                expected_result: "0|0|1:c1,2:b2,3:b3|3|3",
            },
            FileBatchTestCase {
                description: "a Parquet delete_insert by replace_key under last_wins keeps the same rows",
                settings: REPLACE_LAST_WINS,
                format: "parquet",
                files: RESTATED,
                expected_result: "0|0|1:c1,2:b2,3:b3|1|3",
            },
        ];
        for test_case in &test_cases {
            let result = run_batch_case(
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
fn given_files_loaded_in_one_commit_when_loading_into_postgres_then_the_rows_match_loading_them_file_by_file()
-> Result<(), String> {
    run_async(|| async move {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let target = format!(
            "kind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"sqlload\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"",
            postgres.port
        );
        let test_cases = [
            FileBatchTestCase {
                description: "a merge keeps the newest file's row of each key, and the last row of a key a file repeats",
                settings: MERGE,
                format: "csv",
                files: REPEATS,
                expected_result: "0|0|1:c1,2:d2b,3:d3,4:b4b,5:c5|1|4",
            },
            FileBatchTestCase {
                description: "a delete_insert by replace_key under last_wins keeps each value's rows from the last file holding it",
                settings: REPLACE_LAST_WINS,
                format: "csv",
                files: RESTATED,
                expected_result: "0|0|1:c1,2:b2,3:b3|1|3",
            },
        ];
        for test_case in &test_cases {
            client
                .batch_execute("DROP SCHEMA IF EXISTS raw CASCADE")
                .await
                .map_err(|error| error.to_string())?;
            let result = run_batch_case((&target, "raw"), test_case).await?;
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
fn given_more_than_1000_files_when_merging_into_duckdb_then_they_commit_in_batches_resuming_after_a_bad_file()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [
            ManyFilesTestCase {
                description: "1,030 Parquet files, which DuckDB reads itself, merge in 2 commits, each key holding its newest file's last row",
                settings: MERGE,
                format: "parquet",
                files: 1_030,
                bad_files: &[],
                expected_error: "",
                expected_after_error: "",
                expected_result: "0|0|50|2|3090",
            },
            ManyFilesTestCase {
                description: "a bad CSV file in the second batch fails it, naming the file, after the first batch committed; once fixed, the next load merges the second batch, and each file once",
                settings: MERGE,
                format: "csv",
                files: 1_030,
                bad_files: &[1_005],
                expected_error: "f01005.csv",
                expected_after_error: "1000|1",
                expected_result: "0|0|50|2|3090",
            },
        ];
        for test_case in &test_cases {
            let (after_error, result) = run_many_files_case(test_case).await?;
            assert_eq!(
                (after_error.as_str(), result.as_deref()),
                (
                    test_case.expected_after_error,
                    Some(test_case.expected_result)
                ),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
