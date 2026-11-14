use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;
use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::files_target_helpers::data_keys;
use crate::helpers::{
    local_store, partition_values, run_async, run_files_target_scenario, seaweedfs_store,
};
use crate::test_types::{
    CsvTargetTestCase, FilesTargetRun, FilesTargetScenarioTestCase, RoundTripTestCase,
};

const ORDERS: &str = "CREATE TABLE public.orders (id int8 PRIMARY KEY, name text, qty int4, placed_at timestamptz NOT NULL);
INSERT INTO public.orders VALUES (1, 'a', 1, '2026-11-03 10:00:00+00'), (2, 'b', 2, '2026-11-04 10:00:00+00');";

const APPEND_BY_DAY: &str = "write_strategy = \"append\"\ncursor_column = \"placed_at\"\ndestination_path = \"PREFIX/{source}/{table}/day={cursor:date}/\"\n";

const DELETE_INSERT_BY_DAY: &str = "write_strategy = \"delete_insert\"\ncursor_column = \"placed_at\"\ndestination_path = \"PREFIX/{source}/{table}/day={cursor:date}/\"\n";

const OK: FilesTargetRun = FilesTargetRun {
    source_change: "",
    expected_error: "",
    expected_plan: "",
    reload: false,
    settings: None,
    select: &[],
    leftovers: &[],
};

const APPEND_RUNS: &[FilesTargetRun] = &[
    FilesTargetRun {
        expected_plan: "parquet (zstd)  partitions day={cursor:date}/",
        ..OK
    },
    FilesTargetRun {
        source_change: "INSERT INTO public.orders VALUES (3, 'c', 3, '2026-11-04 12:00:00+00');",
        expected_plan: "placed_at after 2026-11-04 10:00:00.000000+00 (maximum in the newest load's files)",
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

const SWAPPED_IN: FilesTargetRun = FilesTargetRun {
    source_change: "DELETE FROM public.orders WHERE id = 1; UPDATE public.orders SET name = 'b2' WHERE id = 2;",
    expected_plan: "then the two folders are swapped in one rename and the old one is deleted",
    ..OK
};

const LOCAL_REPLACE_RUNS: &[FilesTargetRun] = &[
    FilesTargetRun {
        expected_plan: "schema  first files, 6 columns",
        ..OK
    },
    SWAPPED_IN,
];

const CSV_REPLACE_RUNS: &[FilesTargetRun] = &[
    FilesTargetRun {
        expected_plan: "csv (gzip)",
        ..OK
    },
    SWAPPED_IN,
];

const DELETE_INSERT_RUNS: &[FilesTargetRun] = &[
    OK,
    FilesTargetRun {
        source_change: "UPDATE public.orders SET name = 'b2' WHERE id = 2; INSERT INTO public.orders VALUES (3, 'c', 3, '2026-11-04 12:00:00+00');",
        expected_plan: "placed_at from partition 2026-11-04 (the newest); rewrites whole partitions from 2026-11-04 00:00:00.000000+00",
        ..OK
    },
];

/// `orders` reads `10 / d`, so a row with `d = 0` fails the read of the window holding it.
const ORDERS_BY_HOUR: &str = "CREATE TABLE public.orders_raw (id int8, name text, placed_at timestamptz NOT NULL, d int4);
CREATE VIEW public.orders AS SELECT id, name, placed_at, 10 / d AS v FROM public.orders_raw;
INSERT INTO public.orders_raw VALUES (1, 'a', '2026-11-03 01:00:00+00', 1), (2, 'b', '2026-11-03 07:00:00+00', 1);";

/// `batch` finer than the `{cursor:date}` partition.
const DELETE_INSERT_BY_DAY_IN_WINDOWS: &str = "write_strategy = \"delete_insert\"\ncursor_column = \"placed_at\"\nbatch_size = \"6h\"\ndestination_path = \"PREFIX/{source}/{table}/day={cursor:date}/\"\n";

const FAILS_IN_WINDOW_2: FilesTargetRun = FilesTargetRun {
    source_change: "UPDATE public.orders_raw SET name = 'a2' WHERE id = 1; UPDATE public.orders_raw SET d = 0 WHERE id = 2;",
    expected_error: "Postgres query failed",
    settings: Some(DELETE_INSERT_BY_DAY_IN_WINDOWS),
    ..OK
};

#[test]
fn given_a_batch_finer_than_the_partition_when_a_window_fails_then_the_partition_keeps_its_old_files()
-> Result<(), String> {
    let test_cases = [
        FilesTargetScenarioTestCase {
            description: "window 1 of the day committed nothing, so a failure on window 2 leaves the day as it was",
            store: local_store,
            setup: ORDERS_BY_HOUR,
            target_settings: "",
            settings: DELETE_INSERT_BY_DAY,
            runs: &[OK, FAILS_IN_WINDOW_2],
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a,2:b pending:1",
        },
        FilesTargetScenarioTestCase {
            description: "the next run replaces the whole day in one commit",
            store: local_store,
            setup: ORDERS_BY_HOUR,
            target_settings: "",
            settings: DELETE_INSERT_BY_DAY,
            runs: &[
                OK,
                FAILS_IN_WINDOW_2,
                FilesTargetRun {
                    source_change: "UPDATE public.orders_raw SET d = 1 WHERE id = 2;",
                    ..OK
                },
            ],
            check_column: "name",
            expected_files: "day=2026-11-03 run3 1:a2,2:b",
        },
    ];
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        for test_case in &test_cases {
            let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
            let (files, _) =
                run_files_target_scenario(&client, postgres.port, "", directory.path(), test_case)
                    .await?;
            assert_eq!(files, test_case.expected_files, "{}", test_case.description);
        }
        Ok(())
    })
}

#[test]
fn given_postgres_tables_when_loading_into_files_then_each_strategy_writes_the_right_parquet_files()
-> Result<(), String> {
    let test_cases = [
        FilesTargetScenarioTestCase {
            description: "append into a directory adds files per day and never rewrites the old ones",
            store: local_store,
            setup: ORDERS,
            target_settings: "",
            settings: APPEND_BY_DAY,
            runs: APPEND_RUNS,
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run1+run2 2:b,3:c",
        },
        FilesTargetScenarioTestCase {
            description: "a constant column is written into every file",
            store: local_store,
            setup: ORDERS,
            target_settings: "",
            settings: "write_strategy = \"append\"\ncursor_column = \"placed_at\"\ndestination_path = \"PREFIX/{source}/{table}/day={cursor:date}/\"\nconstant_columns = { tenant_id = \"t1\" }\n",
            runs: &[OK],
            check_column: "tenant_id",
            expected_files: "day=2026-11-03 run1 1:t1; day=2026-11-04 run1 2:t1",
        },
        FilesTargetScenarioTestCase {
            description: "append into S3 behaves the same",
            store: seaweedfs_store,
            setup: ORDERS,
            target_settings: "",
            settings: APPEND_BY_DAY,
            runs: APPEND_RUNS,
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run1+run2 2:b,3:c",
        },
        FilesTargetScenarioTestCase {
            description: "replace in a directory swaps the new table in and deletes the old files",
            store: local_store,
            setup: ORDERS,
            target_settings: "",
            settings: "write_strategy = \"replace\"
",
            runs: LOCAL_REPLACE_RUNS,
            check_column: "name",
            expected_files: "- run2 2:b2",
        },
        FilesTargetScenarioTestCase {
            description: "replace in S3 behaves the same",
            store: seaweedfs_store,
            setup: ORDERS,
            target_settings: "",
            settings: "write_strategy = \"replace\"
",
            runs: REPLACE_RUNS,
            check_column: "name",
            expected_files: "- run2 2:b2",
        },
        FilesTargetScenarioTestCase {
            description: "delete_insert by day in a directory rewrites only the partitions it touches",
            store: local_store,
            setup: ORDERS,
            target_settings: "",
            settings: DELETE_INSERT_BY_DAY,
            runs: DELETE_INSERT_RUNS,
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run2 2:b2,3:c",
        },
        FilesTargetScenarioTestCase {
            description: "delete_insert by day in S3 behaves the same",
            store: seaweedfs_store,
            setup: ORDERS,
            target_settings: "",
            settings: DELETE_INSERT_BY_DAY,
            runs: DELETE_INSERT_RUNS,
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run2 2:b2,3:c",
        },
        FilesTargetScenarioTestCase {
            description: "--reload rewrites every partition",
            store: local_store,
            setup: ORDERS,
            target_settings: "",
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
            target_settings: "",
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
            target_settings: "",
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
            target_settings: "",
            settings: "write_strategy = \"merge\"\nunique_key = \"{primary_key}\"\ncursor_column = \"placed_at\"\ndestination_path = \"PREFIX/{source}/{table}/day={cursor:date}/\"\n",
            runs: &[FilesTargetRun {
                expected_error: "merge needs a table format",
                ..OK
            }],
            check_column: "name",
            expected_files: "",
        },
        FilesTargetScenarioTestCase {
            description: "append with a date cursor is refused at plan, as for a warehouse",
            store: local_store,
            setup: "CREATE TABLE public.orders (id int8 PRIMARY KEY, name text, day date NOT NULL); INSERT INTO public.orders VALUES (1, 'a', '2026-11-03');",
            target_settings: "",
            settings: "write_strategy = \"append\"\ncursor_column = \"day\"\ndestination_path = \"PREFIX/{source}/{table}/day={cursor}/\"\n",
            runs: &[FilesTargetRun {
                expected_error: "write_strategy = \"append\" cannot follow the date cursor day",
                ..OK
            }],
            check_column: "name",
            expected_files: "",
        },
        FilesTargetScenarioTestCase {
            description: "append_cursor_inclusive = true into files warns that each run appends the maximum's rows again",
            store: local_store,
            setup: ORDERS,
            target_settings: "",
            settings: "write_strategy = \"append\"\ncursor_column = \"placed_at\"\nappend_cursor_inclusive = true\ndestination_path = \"PREFIX/{source}/{table}/day={cursor:date}/\"\n",
            runs: &[FilesTargetRun {
                expected_plan: "append_cursor_inclusive = true reads from the maximum placed_at itself",
                ..OK
            }],
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run1 2:b",
        },
        FilesTargetScenarioTestCase {
            description: "an incremental load needs the cursor in the path",
            store: local_store,
            setup: ORDERS,
            target_settings: "",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"placed_at\"\n",
            runs: &[FilesTargetRun {
                expected_error: "the path needs {cursor} or {cursor:date} in its last folder",
                ..OK
            }],
            check_column: "name",
            expected_files: "",
        },
        FilesTargetScenarioTestCase {
            description: "a selected table whose folder another configured table shares is blocked, with a hint to add {schema}",
            store: local_store,
            setup: "CREATE TABLE public.orders (id int8 PRIMARY KEY, name text); INSERT INTO public.orders VALUES (1, 'a');
                    CREATE SCHEMA sales; CREATE TABLE sales.orders (id int8 PRIMARY KEY, name text);",
            target_settings: "",
            settings: "write_strategy = \"replace\"
",
            runs: &[FilesTargetRun {
                select: &["public.orders"],
                expected_error: "both come from tables named alike in schemas public and sales, so add {schema} to the path",
                ..OK
            }],
            check_column: "name",
            expected_files: "",
        },
        FilesTargetScenarioTestCase {
            description: "a load that stopped while writing leaves a marker; the next plan reports it and the load deletes its files",
            store: local_store,
            setup: ORDERS,
            target_settings: "",
            settings: APPEND_BY_DAY,
            runs: &[
                OK,
                FilesTargetRun {
                    source_change: "INSERT INTO public.orders VALUES (3, 'c', 3, '2026-11-04 12:00:00+00');",
                    leftovers: &[
                        ("_sqlload/pending/deadbeef", "writing"),
                        ("day=2026-11-04/part-deadbeef-0.parquet", "half a file"),
                    ],
                    expected_plan: "cleanup load deadbeef stopped while writing; load deletes its 1 files first",
                    ..OK
                },
            ],
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run1+run2 2:b,3:c",
        },
        FilesTargetScenarioTestCase {
            description: "a load holding a live lease is running, not dead: the table is refused and its files kept; once its lease expires, the next load repairs it and loads",
            store: local_store,
            setup: ORDERS,
            target_settings: "",
            settings: APPEND_BY_DAY,
            runs: &[
                OK,
                FilesTargetRun {
                    source_change: "INSERT INTO public.orders VALUES (3, 'c', 3, '2026-11-04 12:00:00+00');",
                    leftovers: &[
                        (
                            "_sqlload/lease/00000000000000000001",
                            "running\t2026-11-08T09:00:00+00:00\t2099-01-01T00:00:00+00:00",
                        ),
                        ("_sqlload/pending/running", "writing"),
                    ],
                    expected_error: "the table is being loaded by load running (started 2026-11-08 09:00:00 UTC, lease until 2099-01-01 00:00:00 UTC); retry once it finishes",
                    ..OK
                },
                FilesTargetRun {
                    leftovers: &[
                        (
                            "_sqlload/lease/00000000000000000001",
                            "running\t2026-11-08T09:00:00+00:00\t2026-11-08T09:10:00+00:00",
                        ),
                        ("day=2026-11-04/part-running-0.parquet", "half a file"),
                    ],
                    expected_plan: "cleanup load running stopped while writing; load deletes its 1 files first",
                    ..OK
                },
            ],
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run1+run3 2:b,3:c",
        },
        FilesTargetScenarioTestCase {
            description: "a failed write keeps its marker, and the next load repairs it by load id",
            store: local_store,
            setup: "CREATE TABLE public.orders (id int8 PRIMARY KEY, name text, placed_at timestamptz);
                    INSERT INTO public.orders VALUES (1, 'a', '2026-11-03 10:00:00+00'), (2, 'b', NULL);",
            target_settings: "",
            settings: APPEND_BY_DAY,
            runs: &[
                FilesTargetRun {
                    expected_error: "placed_at is NULL in some rows",
                    ..OK
                },
                FilesTargetRun {
                    source_change: "UPDATE public.orders SET placed_at = '2026-11-04 10:00:00+00' WHERE id = 2;",
                    expected_plan: "stopped while writing; load deletes its 0 files first",
                    ..OK
                },
            ],
            check_column: "name",
            expected_files: "day=2026-11-03 run2 1:a; day=2026-11-04 run2 2:b",
        },
        FilesTargetScenarioTestCase {
            description: "a load that stopped while deleting what it replaced is finished by the next load",
            store: seaweedfs_store,
            setup: ORDERS,
            target_settings: "",
            settings: DELETE_INSERT_BY_DAY,
            runs: &[
                OK,
                FilesTargetRun {
                    leftovers: &[
                        ("day=2026-11-03/part-old-0.parquet", "replaced long ago"),
                        ("_sqlload/pending/feed", "replacing\nday=2026-11-03/part-old-0.parquet"),
                    ],
                    expected_plan: "cleanup load feed stopped while deleting the files it replaced; load finishes deleting 1 files first",
                    ..OK
                },
            ],
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run2 2:b",
        },
        FilesTargetScenarioTestCase {
            description: "a text cursor compares bytewise in Postgres too, so an ICU-collated column is not read again",
            store: local_store,
            setup: "CREATE TABLE public.orders (id int8 PRIMARY KEY, name text, code text COLLATE \"und-x-icu\" NOT NULL);
                    INSERT INTO public.orders VALUES (1, 'a', 'B'), (2, 'b', 'a');",
            target_settings: "",
            settings: "write_strategy = \"append\"\ncursor_column = \"code\"\nappend_cursor_inclusive = false\ndestination_path = \"PREFIX/{source}/{table}/code={cursor}/\"\n",
            runs: &[
                OK,
                FilesTargetRun {
                    expected_plan: "code after a (the newest partition)",
                    ..OK
                },
            ],
            check_column: "name",
            expected_files: "code=B run1 1:a; code=a run1 2:b",
        },
        FilesTargetScenarioTestCase {
            description: "the live schema reads only the newest load's footers, so max_footer_reads caps one load",
            store: local_store,
            setup: ORDERS,
            target_settings: "max_footer_reads = 1",
            settings: "write_strategy = \"append\"\n",
            runs: &[
                OK,
                OK,
                FilesTargetRun {
                    expected_plan: "footers 1 read from the newest partition (2 files listed, max_footer_reads 1)",
                    ..OK
                },
            ],
            check_column: "name",
            expected_files: "- run1+run2+run3 1:a,1:a,1:a,2:b,2:b,2:b",
        },
        FilesTargetScenarioTestCase {
            description: "a narrower source type that the files' type holds is written as the files' type",
            store: seaweedfs_store,
            setup: ORDERS,
            target_settings: "",
            settings: APPEND_BY_DAY,
            runs: &[
                OK,
                FilesTargetRun {
                    source_change: "ALTER TABLE public.orders ALTER COLUMN qty TYPE int2; INSERT INTO public.orders VALUES (3, 'c', 3, '2026-11-05 09:00:00+00');",
                    ..OK
                },
            ],
            check_column: "qty",
            expected_files: "day=2026-11-03 run1 1:1; day=2026-11-04 run1 2:2; day=2026-11-05 run2 3:3",
        },
        FilesTargetScenarioTestCase {
            description: "a populated table that is blocked is reported as blocked, not skipped as a new table",
            store: local_store,
            setup: ORDERS,
            target_settings: "",
            settings: APPEND_BY_DAY,
            runs: &[
                OK,
                FilesTargetRun {
                    settings: Some("write_strategy = \"merge\"\nunique_key = \"{primary_key}\"\ncursor_column = \"placed_at\"\nnew_tables = \"skip\"\ndestination_path = \"PREFIX/{source}/{table}/day={cursor:date}/\"\n"),
                    expected_error: "merge needs a table format",
                    ..OK
                },
            ],
            check_column: "name",
            expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run1 2:b",
        },
        FilesTargetScenarioTestCase {
            description: "a cursor_start the partitions cannot compare with is refused at plan",
            store: local_store,
            setup: ORDERS,
            target_settings: "",
            settings: "write_strategy = \"append\"\ncursor_column = \"placed_at\"\ncursor_start = \"soon\"\ndestination_path = \"PREFIX/{source}/{table}/day={cursor:date}/\"\n",
            runs: &[FilesTargetRun {
                expected_error: "cursor bound \"soon\": \"soon\" is not a date",
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
            let (summary, _) = result?;
            assert_eq!(
                summary.as_str(),
                test_case.expected_files,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_postgres_tables_when_loading_into_csv_files_then_each_strategy_writes_csv_that_reads_back()
-> Result<(), String> {
    let test_cases = [
        CsvTargetTestCase {
            description: "gzip CSV replace in a directory swaps the new table in",
            scenario: FilesTargetScenarioTestCase {
                description: "gzip CSV replace in a directory swaps the new table in",
                store: local_store,
                setup: ORDERS,
                target_settings: "format = \"csv\"\ncompression = \"gzip\"",
                settings: "write_strategy = \"replace\"
",
                runs: CSV_REPLACE_RUNS,
                check_column: "name",
                expected_files: "- run2 2:b2",
            },
            expected_keys: &["part-*.csv.gz"],
        },
        CsvTargetTestCase {
            description: "zstd CSV delete_insert by day in S3 rewrites only the partitions it touches",
            scenario: FilesTargetScenarioTestCase {
                description: "zstd CSV delete_insert by day in S3 rewrites only the partitions it touches",
                store: seaweedfs_store,
                setup: ORDERS,
                target_settings: "format = \"csv\"\ncompression = \"zstd\"",
                settings: DELETE_INSERT_BY_DAY,
                runs: DELETE_INSERT_RUNS,
                check_column: "name",
                expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run2 2:b2,3:c",
            },
            expected_keys: &[
                "day=2026-11-03/part-*.csv.zst",
                "day=2026-11-04/part-*.csv.zst",
            ],
        },
        CsvTargetTestCase {
            description: "a single file per partition keeps its name and is overwritten in S3",
            scenario: FilesTargetScenarioTestCase {
                description: "a single file per partition keeps its name and is overwritten in S3",
                store: seaweedfs_store,
                setup: ORDERS,
                target_settings: "format = \"csv\"\nsingle_file = true",
                settings: DELETE_INSERT_BY_DAY,
                runs: DELETE_INSERT_RUNS,
                check_column: "name",
                expected_files: "day=2026-11-03 run1 1:a; day=2026-11-04 run2 2:b2,3:c",
            },
            expected_keys: &["day=2026-11-03/orders.csv", "day=2026-11-04/orders.csv"],
        },
        CsvTargetTestCase {
            description: "a single-file replace in a directory is one file named after the table",
            scenario: FilesTargetScenarioTestCase {
                description: "a single-file replace in a directory is one file named after the table",
                store: local_store,
                setup: ORDERS,
                target_settings: "format = \"csv\"\ncompression = \"gzip\"\nsingle_file = true",
                settings: "write_strategy = \"replace\"
",
                runs: CSV_REPLACE_RUNS,
                check_column: "name",
                expected_files: "- run2 2:b2",
            },
            expected_keys: &["orders.csv.gz"],
        },
        CsvTargetTestCase {
            description: "an append by a cursor needs footers, which CSV has not",
            scenario: FilesTargetScenarioTestCase {
                description: "an append by a cursor needs footers, which CSV has not",
                store: local_store,
                setup: ORDERS,
                target_settings: "format = \"csv\"",
                settings: APPEND_BY_DAY,
                runs: &[FilesTargetRun {
                    expected_error: "reads the newest files' maximum from Parquet statistics, which CSV files lack",
                    ..OK
                }],
                check_column: "name",
                expected_files: "",
            },
            expected_keys: &[],
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
            let result = async {
                let (summary, table) = run_files_target_scenario(
                    &client,
                    postgres.port,
                    &server.endpoint,
                    &directory,
                    &test_case.scenario,
                )
                .await?;
                Ok::<(String, Vec<String>), String>((summary, data_keys(&table).await?))
            }
            .await;
            let _ = std::fs::remove_dir_all(&directory);
            let (summary, keys) = result?;
            assert_eq!(
                summary, test_case.scenario.expected_files,
                "{}",
                test_case.description
            );
            assert_eq!(keys, test_case.expected_keys, "{}", test_case.description);
        }
        Ok(())
    })
}

#[test]
fn given_partition_values_that_need_escaping_when_read_back_by_the_files_source_then_they_round_trip()
-> Result<(), String> {
    let test_cases = [RoundTripTestCase {
        description: "partition values round-trip through escaped folder names",
        scenario: FilesTargetScenarioTestCase {
            description: "values with a leading _ or ., a %, a / and an = are escaped in folder names",
            store: local_store,
            setup: "CREATE TABLE public.orders (id int8 PRIMARY KEY, name text, code text NOT NULL);
                    INSERT INTO public.orders VALUES (1, 'a', '_draft'), (2, 'b', '.hidden'), (3, 'c', '50%'), (4, 'd', 'a/b'), (5, 'e', 'k=v');",
            target_settings: "",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"code\"\ndestination_path = \"PREFIX/{source}/{table}/code={cursor}/\"\n",
            runs: &[OK],
            check_column: "name",
            expected_files: "code=%2Ehidden run1 2:b; code=%5Fdraft run1 1:a; code=50%25 run1 3:c; code=a%2Fb run1 4:d; code=k%3Dv run1 5:e",
        },
        partition_column: "code",
        expected_values: &[".hidden", "50%", "_draft", "a/b", "k=v"],
    }];
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        for test_case in &test_cases {
            let directory =
                std::env::temp_dir().join(format!("sqlload-lake-{}", uuid::Uuid::new_v4()));
            let result = async {
                let (summary, table) = run_files_target_scenario(
                    &client,
                    postgres.port,
                    "",
                    &directory,
                    &test_case.scenario,
                )
                .await?;
                let values = partition_values(&table, test_case.partition_column).await?;
                Ok::<(String, Vec<String>), String>((summary, values))
            }
            .await;
            let _ = std::fs::remove_dir_all(&directory);
            let (summary, values) = result?;
            assert_eq!(
                summary, test_case.scenario.expected_files,
                "{}",
                test_case.description
            );
            assert_eq!(
                values, test_case.expected_values,
                "{}: the files source reads back the values written",
                test_case.description
            );
        }
        Ok(())
    })
}
