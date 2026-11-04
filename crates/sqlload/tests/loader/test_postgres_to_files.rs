use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;
use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::helpers::{local_store, run_async, run_files_target_scenario, seaweedfs_store};
use crate::test_types::{FilesTargetRun, FilesTargetScenarioTestCase};

const ORDERS: &str = "CREATE TABLE public.orders (id int8 PRIMARY KEY, name text, qty int4, placed_at timestamptz NOT NULL);
INSERT INTO public.orders VALUES (1, 'a', 1, '2026-11-03 10:00:00+00'), (2, 'b', 2, '2026-11-04 10:00:00+00');";

const APPEND_BY_DAY: &str = "write_strategy = \"append\"\ncursor_column = \"placed_at\"\nappend_cursor_inclusive = false\ndestination_path = \"PREFIX/{source}/{table}/day={cursor:date}/\"\n";

const DELETE_INSERT_BY_DAY: &str = "write_strategy = \"delete_insert\"\ncursor_column = \"placed_at\"\ndestination_path = \"PREFIX/{source}/{table}/day={cursor:date}/\"\n";

const OK: FilesTargetRun = FilesTargetRun {
    source_change: "",
    expected_error: "",
    expected_plan: "",
    reload: false,
};

const APPEND_RUNS: &[FilesTargetRun] = &[
    FilesTargetRun {
        expected_plan: "parquet (zstd)  partitions day={cursor:date}/",
        ..OK
    },
    FilesTargetRun {
        source_change: "INSERT INTO public.orders VALUES (3, 'c', 3, '2026-11-04 12:00:00+00');",
        expected_plan: "placed_at from 2026-11-04 10:00:00.000000+00 (maximum in the newest partition's files)",
        ..OK
    },
];

const REPLACE_RUNS: &[FilesTargetRun] = &[
    FilesTargetRun {
        expected_plan: "schema  first files, 6 columns",
        ..OK
    },
    FilesTargetRun {
        source_change: "DELETE FROM public.orders WHERE id = 1; UPDATE public.orders SET name = 'b2' WHERE id = 2;",
        expected_plan: "then every older file in the folder is deleted; not atomic",
        ..OK
    },
];

const DELETE_INSERT_RUNS: &[FilesTargetRun] = &[
    OK,
    FilesTargetRun {
        source_change: "UPDATE public.orders SET name = 'b2' WHERE id = 2; INSERT INTO public.orders VALUES (3, 'c', 3, '2026-11-04 12:00:00+00');",
        expected_plan: "placed_at from partition 2026-11-04 (the newest); rewrites whole partitions from 2026-11-04 00:00:00.000000+00",
        ..OK
    },
];

#[test]
fn given_postgres_tables_when_loading_into_files_then_each_strategy_writes_the_right_parquet_files()
-> Result<(), String> {
    let test_cases = [
        FilesTargetScenarioTestCase {
            description: "append into a directory adds files per day and never rewrites the old ones",
            store: local_store,
            setup: ORDERS,
            settings: APPEND_BY_DAY,
            runs: APPEND_RUNS,
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run1+run2 2:b,3:c",
        },
        FilesTargetScenarioTestCase {
            description: "append into S3 behaves the same",
            store: seaweedfs_store,
            setup: ORDERS,
            settings: APPEND_BY_DAY,
            runs: APPEND_RUNS,
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run1+run2 2:b,3:c",
        },
        FilesTargetScenarioTestCase {
            description: "replace in a directory writes the new set and deletes the old files",
            store: local_store,
            setup: ORDERS,
            settings: "",
            runs: REPLACE_RUNS,
            check_column: "name",
            expected_files: "- run2 2:b2",
        },
        FilesTargetScenarioTestCase {
            description: "replace in S3 behaves the same",
            store: seaweedfs_store,
            setup: ORDERS,
            settings: "",
            runs: REPLACE_RUNS,
            check_column: "name",
            expected_files: "- run2 2:b2",
        },
        FilesTargetScenarioTestCase {
            description: "delete_insert by day in a directory rewrites only the partitions it touches",
            store: local_store,
            setup: ORDERS,
            settings: DELETE_INSERT_BY_DAY,
            runs: DELETE_INSERT_RUNS,
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run2 2:b2,3:c",
        },
        FilesTargetScenarioTestCase {
            description: "delete_insert by day in S3 behaves the same",
            store: seaweedfs_store,
            setup: ORDERS,
            settings: DELETE_INSERT_BY_DAY,
            runs: DELETE_INSERT_RUNS,
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run2 2:b2,3:c",
        },
        FilesTargetScenarioTestCase {
            description: "--reload rewrites every partition",
            store: local_store,
            setup: ORDERS,
            settings: DELETE_INSERT_BY_DAY,
            runs: &[
                OK,
                FilesTargetRun {
                    reload: true,
                    expected_plan: "recreate from the whole source (--reload)",
                    ..OK
                },
            ],
            check_column: "name",
            expected_files: "day=2026-11-03 run2 1:a; day=2026-11-04 run2 2:b",
        },
        FilesTargetScenarioTestCase {
            description: "a new source column appears in the new files only",
            store: local_store,
            setup: ORDERS,
            settings: APPEND_BY_DAY,
            runs: &[
                OK,
                FilesTargetRun {
                    source_change: "ALTER TABLE public.orders ADD COLUMN tier text; INSERT INTO public.orders VALUES (3, 'c', 3, '2026-11-05 09:00:00+00', 'gold');",
                    expected_plan: "schema  new files add column  tier text -> Utf8",
                    ..OK
                },
            ],
            check_column: "tier",
            expected_files: "day=2026-11-03 run1 1:-; day=2026-11-04 run1 2:-; day=2026-11-05 run2 3:gold",
        },
        FilesTargetScenarioTestCase {
            description: "a type change is blocked because existing files cannot be altered",
            store: seaweedfs_store,
            setup: ORDERS,
            settings: APPEND_BY_DAY,
            runs: &[
                OK,
                FilesTargetRun {
                    source_change: "ALTER TABLE public.orders ALTER COLUMN qty TYPE text; INSERT INTO public.orders VALUES (3, 'c', 'x', '2026-11-05 09:00:00+00');",
                    expected_error: "qty is int in the existing files but text in the source; existing files cannot be altered",
                    ..OK
                },
            ],
            check_column: "qty",
            expected_files: "day=2026-11-03 run1 1:1; day=2026-11-04 run1 2:2",
        },
        FilesTargetScenarioTestCase {
            description: "merge is rejected at plan",
            store: local_store,
            setup: ORDERS,
            settings: "write_strategy = \"merge\"\ncursor_column = \"placed_at\"\ndestination_path = \"PREFIX/{source}/{table}/day={cursor:date}/\"\n",
            runs: &[FilesTargetRun {
                expected_error: "merge needs a table format",
                ..OK
            }],
            check_column: "name",
            expected_files: "",
        },
        FilesTargetScenarioTestCase {
            description: "an incremental load needs the cursor in the path",
            store: local_store,
            setup: ORDERS,
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"placed_at\"\n",
            runs: &[FilesTargetRun {
                expected_error: "the path needs {cursor} or {cursor:date} in its last folder",
                ..OK
            }],
            check_column: "name",
            expected_files: "",
        },
    ];
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let server = start_seaweedfs().await?;
        create_bucket(&server, "example-bucket-test").await?;
        for test_case in &test_cases {
            let directory =
                std::env::temp_dir().join(format!("sqlload-lake-{}", uuid::Uuid::new_v4()));
            let result = run_files_target_scenario(
                &client,
                postgres.port,
                &server.endpoint,
                &directory,
                test_case,
            )
            .await;
            let _ = std::fs::remove_dir_all(&directory);
            assert_eq!(
                result?.as_str(),
                test_case.expected_files,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
