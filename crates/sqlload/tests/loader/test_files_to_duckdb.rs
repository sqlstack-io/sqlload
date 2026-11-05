use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::helpers::{
    local_store, run_async, run_files_scenario, run_large_partition, seaweedfs_store,
};
use crate::test_types::{FilesRun, FilesScenarioTestCase, LargePartitionTestCase, ParquetFile};

const NOV_3: ParquetFile = ParquetFile {
    key: "day=2026-11-03/part-0.parquet",
    ids: &[1, 2],
    score: 1.0,
    extra: None,
};
const NOV_4: ParquetFile = ParquetFile {
    key: "day=2026-11-04/part-0.parquet",
    ids: &[3],
    score: 1.0,
    extra: Some("x"),
};
const NOV_4_REWRITTEN: ParquetFile = ParquetFile {
    key: "day=2026-11-04/part-0.parquet",
    ids: &[3, 4],
    score: 2.0,
    extra: Some("y"),
};
const NOV_5: ParquetFile = ParquetFile {
    key: "day=2026-11-05/part-0.parquet",
    ids: &[5],
    score: 1.0,
    extra: Some("z"),
};

/// A Spark-style escaped folder (`%3A`), which must be read as listed, not escaped again.
const NOV_6_ESCAPED: ParquetFile = ParquetFile {
    key: "day=2026-11-06/batch=10%3A00/part-0.parquet",
    ids: &[6],
    score: 1.0,
    extra: Some("w"),
};

const APPEND_RUNS: &[FilesRun] = &[
    FilesRun {
        files: &[NOV_3, NOV_4],
        expected_error: "",
    },
    FilesRun {
        files: &[NOV_5, NOV_6_ESCAPED],
        expected_error: "",
    },
];

/// Rows, distinct source files, and the newest `extra`; a column added in later files is NULL for older rows.
const APPEND_CHECK: &str = "SELECT count(*) || ',' || count(DISTINCT _sqlload_source_file) || ',' || count(extra) || ',' || max(day) FROM raw.scores";

/// `0` while the table was never created.
const NO_TABLE: &str =
    "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'scores'";

#[test]
fn given_parquet_files_when_loading_into_duckdb_then_new_files_and_partitions_load_once()
-> Result<(), String> {
    let test_cases = [
        FilesScenarioTestCase {
            description: "append from a directory loads each file once and adds columns from newer files",
            store: local_store,
            settings: "write_strategy = \"append\"\npartition_columns = { day = \"date\" }\n",
            runs: APPEND_RUNS,
            check_query: APPEND_CHECK,
            expected_result: "5,4,3,2026-11-06",
        },
        FilesScenarioTestCase {
            description: "append from S3 behaves the same",
            store: seaweedfs_store,
            settings: "write_strategy = \"append\"\npartition_columns = { day = \"date\" }\n",
            runs: APPEND_RUNS,
            check_query: APPEND_CHECK,
            expected_result: "5,4,3,2026-11-06",
        },
        FilesScenarioTestCase {
            description: "delete_insert on a partition cursor replaces the newest partition when its file is rewritten",
            store: local_store,
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\npartition_columns = { day = \"date\" }\n",
            runs: &[
                FilesRun {
                    files: &[NOV_3, NOV_4],
                    expected_error: "",
                },
                FilesRun {
                    files: &[NOV_4_REWRITTEN],
                    expected_error: "",
                },
            ],
            check_query: "SELECT string_agg(id || ':' || score, ',' ORDER BY id) FROM raw.scores",
            expected_result: "1:1.0,2:1.0,3:2.0,4:2.0",
        },
        FilesScenarioTestCase {
            description: "append stops when a loaded file was rewritten in place",
            store: seaweedfs_store,
            settings: "write_strategy = \"append\"\npartition_columns = { day = \"date\" }\n",
            runs: &[
                FilesRun {
                    files: &[NOV_3, NOV_4],
                    expected_error: "",
                },
                FilesRun {
                    files: &[NOV_4_REWRITTEN],
                    expected_error: "was rewritten in place",
                },
            ],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.scores",
            expected_result: "3",
        },
        FilesScenarioTestCase {
            description: "merge is blocked for files sources",
            store: local_store,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\npartition_columns = { day = \"date\" }\n",
            runs: &[FilesRun {
                files: &[NOV_3],
                expected_error: "merge is not supported for files sources",
            }],
            check_query: NO_TABLE,
            expected_result: "0",
        },
        FilesScenarioTestCase {
            description: "a source with no files is an error, never an empty load",
            store: local_store,
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\npartition_columns = { day = \"date\" }\n",
            runs: &[FilesRun {
                files: &[],
                expected_error: "no .parquet files found",
            }],
            check_query: NO_TABLE,
            expected_result: "0",
        },
        FilesScenarioTestCase {
            description: "a value that does not fit a pinned type fails the load instead of loading NULL",
            store: local_store,
            settings: "write_strategy = \"append\"\npartition_columns = { day = \"date\" }\n[sources.scores.tables.scores]\ncolumns = { extra = \"int\" }\n",
            runs: &[FilesRun {
                files: &[NOV_4],
                expected_error: "extra: cannot convert",
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.scores",
            expected_result: "0",
        },
        FilesScenarioTestCase {
            description: "a cursor that is not a partition column is blocked",
            store: local_store,
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"id\"\npartition_columns = { day = \"date\" }\n",
            runs: &[FilesRun {
                files: &[NOV_3],
                expected_error: "must be one of the partition_columns",
            }],
            check_query: NO_TABLE,
            expected_result: "0",
        },
    ];
    run_async(async {
        let server = start_seaweedfs().await?;
        create_bucket(&server, "example-bucket-test").await?;
        for test_case in test_cases {
            let directory =
                std::env::temp_dir().join(format!("sqlload-files-{}", uuid::Uuid::new_v4()));
            let result = run_files_scenario(&server.endpoint, &directory, &test_case).await;
            let _ = std::fs::remove_dir_all(&directory);
            assert_eq!(
                result?.as_deref(),
                Some(test_case.expected_result),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_partition_of_more_than_1000_files_when_delete_insert_loads_it_then_it_commits_once_with_its_new_columns()
-> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let test_cases = [LargePartitionTestCase {
        description: "every file of a 1,001-file partition loads once, with the column only its files have; the rewritten neighbour replaces only its own partition",
        files: 1001,
        expected_plan: "batch is ignored: a files source commits in batches of whole partitions",
        expected_result: "1003,1001,1002,7/8/9",
    }];
    run_async(async {
        for test_case in &test_cases {
            assert_eq!(
                run_large_partition(directory.path(), test_case)
                    .await?
                    .as_deref(),
                Some(test_case.expected_result),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_files_when_loading_them_then_each_row_carries_its_number_in_its_file() -> Result<(), String>
{
    let test_cases = [FilesScenarioTestCase {
        description: "row numbers start at 1 in each file",
        store: local_store,
        settings: "write_strategy = \"append\"\npartition_columns = { day = \"date\" }\n",
        runs: &[FilesRun {
            files: &[NOV_3, NOV_5],
            expected_error: "",
        }],
        check_query: "SELECT string_agg(id || ':' || _sqlload_source_row, ',' ORDER BY id) FROM raw.scores",
        expected_result: "1:1,2:2,5:1",
    }];
    run_async(async {
        for test_case in &test_cases {
            let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
            let result = run_files_scenario("", directory.path(), test_case).await?;
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
