use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;
use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::files_helpers::{
    late_and_new, no_files, old_record, run_files_to_files, run_planted, same_record, third_day,
};
use crate::helpers::{
    local_store, run_async, run_metadata_round_trip, seaweedfs_store, two_batches, two_days,
};
use crate::test_types::{FilesToFilesTestCase, MetadataRoundTripTestCase, PlantedTestCase};

/// The DuckDB table's columns in order, its rows, its distinct load ids, and the rows a load before `{stamp}` wrote.
const ROUND_TRIP_CHECK: &str = "SELECT (SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'orders') || '|' || count(*) || '|' || count(DISTINCT _sqlload_load_id) || '|' || count(*) FILTER (WHERE _sqlload_loaded_at < CAST('{stamp}' AS TIMESTAMPTZ)) FROM raw.orders";

const APPEND: FilesToFilesTestCase = FilesToFilesTestCase {
    description: "",
    store: local_store,
    settings: "write_strategy = \"append\"",
    runs: &[two_days, no_files, third_day],
    expected_held: &["6|1", "6|1", "8|2"],
    expected_reads: "",
};

const PLANTED: PlantedTestCase = PlantedTestCase {
    description: "",
    strategy: "append",
    planted: &[],
    rewrite: same_record,
    reload: false,
    interleaved: 0,
    expected_error: None,
    expected_state: "8|2 records:2 markers:",
};

#[test]
fn given_files_a_files_target_wrote_when_a_files_source_loads_them_twice_then_the_metadata_is_this_loads()
-> Result<(), String> {
    let test_cases = [MetadataRoundTripTestCase {
        description: "the files' loader columns are dropped and the second load stamps every row",
        setup: "CREATE TABLE public.orders (id int8 PRIMARY KEY, name text);
                INSERT INTO public.orders VALUES (1, 'a'), (2, 'b');",
        change: "INSERT INTO public.orders VALUES (3, 'c');",
        expected_plan: "note    dropped _sqlload_load_id, _sqlload_loaded_at from the files (replaced by this load's)",
        expected_check: "id,name,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_sqlload_load_id,_sqlload_loaded_at|3|1|0",
    }];
    run_async(|| async move {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let server = start_seaweedfs().await?;
        create_bucket(&server, "example-bucket-test").await?;
        for test_case in &test_cases {
            let (plan, checked) = run_metadata_round_trip(
                &client,
                postgres.port,
                &server.endpoint,
                test_case,
                ROUND_TRIP_CHECK,
            )
            .await?;
            assert!(
                plan.contains(test_case.expected_plan),
                "{}: {plan}",
                test_case.description
            );
            assert_eq!(
                checked.as_deref(),
                Some(test_case.expected_check),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_files_source_when_loading_it_into_a_files_target_again_then_each_source_file_is_loaded_once()
-> Result<(), String> {
    let test_cases = [
        FilesToFilesTestCase {
            description: "append: a second load finds every file loaded, a third loads only the new day",
            ..APPEND
        },
        FilesToFilesTestCase {
            description: "append into S3 behaves the same",
            store: seaweedfs_store,
            ..APPEND
        },
        FilesToFilesTestCase {
            description: "append by day: a late file in an older day is skipped, a new file in the newest day loads once",
            settings: "write_strategy = \"append\"\ncursor_column = \"day\"\ndestination_path = \"PREFIX/out/{table}/day={cursor:date}/\"",
            runs: &[two_days, late_and_new, no_files],
            expected_held: &["6|1", "8|2", "8|2"],
            expected_reads: "reads   day >= 2026-11-02 (maximum in the newest load's files)",
            ..APPEND
        },
        FilesToFilesTestCase {
            description: "append in two commits: the next load finds both commits' files loaded",
            runs: &[two_batches, no_files],
            expected_held: &["2002|1", "2002|1"],
            ..APPEND
        },
        FilesToFilesTestCase {
            description: "replace: each load writes every row once, replacing the last load's files",
            settings: "write_strategy = \"replace\"",
            expected_held: &["6|1", "6|1", "8|1"],
            ..APPEND
        },
        FilesToFilesTestCase {
            description: "delete_insert by day: each load reads the target's newest day again and replaces its files, never doubling (the third load replaces the second's day and adds the new one)",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\ndestination_path = \"PREFIX/out/{table}/day={cursor:date}/\"",
            expected_held: &["6|1", "6|2", "8|2"],
            ..APPEND
        },
    ];
    run_async(|| async move {
        let server = start_seaweedfs().await?;
        create_bucket(&server, "example-bucket-test").await?;
        for test_case in &test_cases {
            let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
            let (held, plan) =
                run_files_to_files(&server.endpoint, directory.path(), test_case).await?;
            assert_eq!(
                (held, plan.contains(test_case.expected_reads)),
                (
                    test_case
                        .expected_held
                        .iter()
                        .map(|held| (*held).to_owned())
                        .collect::<Vec<String>>(),
                    true
                ),
                "{}: {plan}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_files_target_left_in_an_odd_state_when_a_files_source_loads_into_it_again_then_each_source_file_is_loaded_once()
-> Result<(), String> {
    let test_cases = [
        PlantedTestCase {
            description: "a load that crashed writing left a part and a record claiming the new day's file: the part and the record go, the file loads once, and the marker stays as a tombstone",
            planted: &[
                ("_sqlload/pending/crashed.00001", "writing"),
                ("part-crashed.00001-0.parquet", "partial"),
                (
                    "_sqlload/loaded/crashed.00001.json",
                    "{\"commits\":{\"crashed.00001\":{\"\":[[\"SOURCEday=2026-11-03/part-00000.parquet\",\"v1\"]]}}}",
                ),
            ],
            expected_state: "8|2 records:2 markers:crashed.00001=aborted",
            ..PLANTED
        },
        PlantedTestCase {
            description: "records an older SQLLoad wrote are never read as nothing loaded: the load stops, naming --reload",
            rewrite: old_record,
            expected_error: Some("format of an older SQLLoad"),
            expected_state: "6|1 records:1 markers:",
            ..PLANTED
        },
        PlantedTestCase {
            description: "--reload rewrites the table and records it in this format; the old record names a commit the table no longer holds, so it is never read again and the next load deletes it",
            rewrite: old_record,
            reload: true,
            expected_state: "8|1 records:2 markers:",
            ..PLANTED
        },
        PlantedTestCase {
            description: "a replace never reads the records, so records an older SQLLoad wrote don't block it, and the old one goes stale with its commit",
            strategy: "replace",
            rewrite: old_record,
            expected_state: "8|1 records:2 markers:",
            ..PLANTED
        },
        PlantedTestCase {
            description: "a load planned before another committed the new file stops instead of loading it twice",
            interleaved: 1,
            expected_error: Some("another load changed the table's files"),
            ..PLANTED
        },
    ];
    run_async(|| async move {
        for test_case in &test_cases {
            let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
            let (error, state) = run_planted(directory.path(), test_case).await?;
            let stopped = error.as_ref().map(|error| {
                test_case
                    .expected_error
                    .is_some_and(|phrase| error.contains(phrase))
            });
            assert_eq!(
                (stopped, state),
                (
                    test_case.expected_error.map(|_| true),
                    test_case.expected_state.to_owned()
                ),
                "{}: {error:?}",
                test_case.description
            );
        }
        Ok(())
    })
}
