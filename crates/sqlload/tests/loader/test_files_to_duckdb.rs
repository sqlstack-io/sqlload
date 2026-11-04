use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::helpers::{local_store, run_async, run_files_scenario, seaweedfs_store};
use crate::test_types::{FilesRun, FilesScenarioTestCase, ParquetFile};

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

const APPEND_RUNS: &[FilesRun] = &[
    FilesRun {
        files: &[NOV_3, NOV_4],
        expected_error: "",
    },
    FilesRun {
        files: &[NOV_5],
        expected_error: "",
    },
];

/// Rows, distinct source files, and the newest `extra`; a column added in later files is NULL for older rows.
const APPEND_CHECK: &str = "SELECT count(*) || ',' || count(DISTINCT _sqlload_source_file) || ',' || count(extra) || ',' || max(day) FROM raw.scores";

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
            expected_result: "4,3,2,2026-11-05",
        },
        FilesScenarioTestCase {
            description: "append from S3 behaves the same",
            store: seaweedfs_store,
            settings: "write_strategy = \"append\"\npartition_columns = { day = \"date\" }\n",
            runs: APPEND_RUNS,
            check_query: APPEND_CHECK,
            expected_result: "4,3,2,2026-11-05",
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
            description: "a cursor that is not a partition column is blocked",
            store: local_store,
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"id\"\npartition_columns = { day = \"date\" }\n",
            runs: &[FilesRun {
                files: &[NOV_3],
                expected_error: "must be one of the partition_columns",
            }],
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'scores'",
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
