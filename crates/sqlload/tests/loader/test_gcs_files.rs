use sqlload::files_source::models::AfterLoad;
use sqlload_testkit::fake_gcs::main::start_fake_gcs::start_fake_gcs;

use crate::after_load_helpers::run_store_after_load;
use crate::files_helpers::{gcs_store, no_files, run_files_to_files, third_day};
use crate::helpers::{plain, run_async, run_files_scenario, run_text_files_scenario, two_days};
use crate::test_files_to_duckdb::{APPEND_CHECK, APPEND_RUNS};
use crate::test_text_files_to_duckdb::{
    PATTERN_CHECK, PATTERN_RESULT, PATTERN_RUNS, PATTERN_SETTINGS,
};
use crate::test_types::{
    FilesScenarioTestCase, FilesToFilesTestCase, S3AfterLoadTestCase, TextFile, TextFilesRun,
    TextFilesScenarioTestCase,
};

/// The later of two dated files, by path, modified first, each holding P02.
const RESTATED: &[TextFile] = &[
    TextFile {
        key: "sales_2026-11-14.csv",
        contents: "le_version,line,amount\nP02,X,7\n",
        encode: plain,
    },
    TextFile {
        key: "sales_2026-11-07.csv",
        contents: "le_version,line,amount\nP01,A,1\nP02,A,10\n",
        encode: plain,
    },
];

#[test]
fn given_parquet_files_in_gcs_when_appending_them_into_duckdb_then_they_load_as_from_s3()
-> Result<(), String> {
    let test_cases = [FilesScenarioTestCase {
        description: "Parquet appended from GCS loads each file once and adds columns from newer files",
        store: gcs_store,
        settings: "write_strategy = \"append\"\npartition_columns = { day = \"date\" }\n",
        runs: APPEND_RUNS,
        check_query: APPEND_CHECK,
        expected_result: "5,4,3,2026-11-06",
    }];
    run_async(|| async move {
        let server = start_fake_gcs().await?;
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let result = run_files_scenario(&server.endpoint, directory.path(), test_case).await?;
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
fn given_text_files_in_gcs_when_pattern_order_and_replace_key_pick_them_then_they_load_as_from_s3()
-> Result<(), String> {
    let test_cases = [
        TextFilesScenarioTestCase {
            description: "pattern picks pipe-delimited .txt files in GCS beside another, merged on their key in path order",
            store: gcs_store,
            settings: PATTERN_SETTINGS,
            runs: PATTERN_RUNS,
            check_query: PATTERN_CHECK,
            expected_result: PATTERN_RESULT,
        },
        TextFilesScenarioTestCase {
            description: "replace_key with on_overlapping_files = \"last_wins\" in path order keeps P02 from the file last by path in GCS",
            store: gcs_store,
            settings: "format = \"csv\"\nwrite_strategy = \"delete_insert\"\nreplace_key = [\"le_version\"]\non_overlapping_files = \"last_wins\"\norder = \"path\"\n",
            runs: &[TextFilesRun {
                files: RESTATED,
                expected_error: "",
                expected_plan: "the last file holding a replace_key value replaces it",
            }],
            check_query: "SELECT string_agg(le_version || ':' || line || ':' || CAST(amount AS VARCHAR), ',' ORDER BY le_version, line) FROM raw.scores",
            expected_result: "P01:A:1,P02:X:7",
        },
    ];
    run_async(|| async move {
        let server = start_fake_gcs().await?;
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let result =
                run_text_files_scenario(&server.endpoint, directory.path(), test_case).await?;
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
fn given_a_files_target_in_gcs_when_a_files_source_in_gcs_loads_into_it_then_each_strategy_holds_each_row_once()
-> Result<(), String> {
    const APPEND: FilesToFilesTestCase = FilesToFilesTestCase {
        description: "append writes each new day once",
        store: gcs_store,
        settings: "write_strategy = \"append\"",
        runs: &[two_days, no_files, third_day],
        expected_held: &["6|1", "6|1", "8|2"],
        expected_reads: "",
    };
    let test_cases = [
        FilesToFilesTestCase { ..APPEND },
        FilesToFilesTestCase {
            description: "replace writes every row once, replacing the last load's files",
            settings: "write_strategy = \"replace\"",
            expected_held: &["6|1", "6|1", "8|1"],
            ..APPEND
        },
        FilesToFilesTestCase {
            description: "delete_insert by day replaces the newest day's files, never doubling",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\ndestination_path = \"PREFIX/out/{table}/day={cursor:date}/\"",
            expected_held: &["6|1", "6|2", "8|2"],
            ..APPEND
        },
    ];
    run_async(|| async move {
        let server = start_fake_gcs().await?;
        for test_case in &test_cases {
            let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
            let (held, _) =
                run_files_to_files(&server.endpoint, directory.path(), test_case).await?;
            assert_eq!(
                held,
                test_case
                    .expected_held
                    .iter()
                    .map(|held| (*held).to_owned())
                    .collect::<Vec<String>>(),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_files_in_gcs_when_after_load_acts_then_they_move_server_side_or_are_deleted_and_a_changed_file_stays()
-> Result<(), String> {
    let test_cases = [
        S3AfterLoadTestCase {
            description: "moves keep their key below move_to",
            folder: "gcs-move/",
            after_load: AfterLoad::MoveTo("gcs-move/done".to_owned()),
            files: &[("drop/a.csv", "a"), ("drop/2026/b.csv", "b")],
            rewritten: &[],
            expected_report: "move done 2 0",
            expected_files: &["done/2026/b.csv=b", "done/a.csv=a"],
        },
        S3AfterLoadTestCase {
            description: "deletes remove only the listed files, not one rewritten since",
            folder: "gcs-delete/",
            after_load: AfterLoad::Delete,
            files: &[("drop/a.csv", "a"), ("drop/b.csv", "b")],
            rewritten: &[("drop/a.csv", "a, longer now")],
            expected_report: "delete failed 1 1",
            expected_files: &["drop/a.csv=a, longer now"],
        },
    ];
    run_async(|| async move {
        let server = start_fake_gcs().await?;
        for test_case in &test_cases {
            let (store, _) = gcs_store(&server.endpoint, std::path::Path::new(""));
            let actual = run_store_after_load(store, test_case).await?;
            assert_eq!(
                actual,
                (
                    test_case.expected_report.to_owned(),
                    test_case
                        .expected_files
                        .iter()
                        .map(|file| (*file).to_owned())
                        .collect()
                ),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
