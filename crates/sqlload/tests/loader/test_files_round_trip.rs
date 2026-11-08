use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;
use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::files_helpers::{late_and_new, no_files, run_files_to_files, third_day};
use crate::helpers::{
    local_store, run_async, run_metadata_round_trip, seaweedfs_store, two_batches, two_days,
};
use crate::test_types::{FilesToFilesTestCase, MetadataRoundTripTestCase};

/// The DuckDB table's columns in order, its rows, its distinct load ids, and the rows a load before `{stamp}` wrote.
const ROUND_TRIP_CHECK: &str = "SELECT (SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'orders') || '|' || count(*) || '|' || count(DISTINCT _sqlload_load_id) || '|' || count(*) FILTER (WHERE _sqlload_loaded_at < CAST('{stamp}' AS TIMESTAMPTZ)) FROM raw.orders";

const APPEND: FilesToFilesTestCase = FilesToFilesTestCase {
    description: "",
    store: local_store,
    settings: "write_strategy = \"append\"",
    runs: &[two_days, no_files, third_day],
    expected_held: &["6|1", "6|1", "8|2"],
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
    run_async(async {
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
    run_async(async {
        let server = start_seaweedfs().await?;
        create_bucket(&server, "example-bucket-test").await?;
        for test_case in &test_cases {
            let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
            let held = run_files_to_files(&server.endpoint, directory.path(), test_case).await?;
            assert_eq!(held, test_case.expected_held, "{}", test_case.description);
        }
        Ok(())
    })
}
