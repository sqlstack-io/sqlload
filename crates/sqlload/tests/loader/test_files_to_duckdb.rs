use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::after_load_helpers::{run_after_load_case, run_archived_reload};
use crate::files_helpers::run_source_row_upgrade;
use crate::helpers::{MEGABYTE, run_peak_memory};
use crate::helpers::{
    int96_in_a_later_batch, local_store, run_async, run_file_set, run_files_scenario,
    run_large_partition, run_replaced_keys, seaweedfs_store,
};
use crate::merge_helpers::run_merge_case;
use crate::test_types::{
    AfterLoadStep, AfterLoadTestCase, ArchivedReloadTestCase, FileSetTestCase,
};
use crate::test_types::{FilesMergeTestCase, MergeFile, MergeStep, ReplacedKeysTestCase};
use crate::test_types::{FilesRun, FilesScenarioTestCase, LargePartitionTestCase, ParquetFile};
use crate::test_types::{PeakMemoryTestCase, SourceRowUpgradeTestCase};

/// Every row as `id:v`, then how many merged files the run log records.
pub(crate) const MERGE_CHECK: &str = "SELECT (SELECT string_agg(CAST(id AS VARCHAR) || ':' || v, ',' ORDER BY id) FROM raw.scores) || '|' || (SELECT CAST(count(*) AS VARCHAR) FROM raw._sqlload_runs WHERE table_name = 'scores' AND event = 'run')";

/// Every row as `id:v`, a NULL id as `NULL`, then how many merged files the run log records.
const NULL_MERGE_CHECK: &str = "SELECT (SELECT string_agg(COALESCE(CAST(id AS VARCHAR), 'NULL') || ':' || v, ',' ORDER BY id NULLS LAST) FROM raw.scores) || '|' || (SELECT CAST(count(*) AS VARCHAR) FROM raw._sqlload_runs WHERE table_name = 'scores' AND event = 'run')";

const NULL_FAIL_SETTINGS: &str =
    "write_strategy = \"merge\"\nunique_key = [\"id\"]\nnull_keys = \"fail\"\n";

/// Every row as `id:v`, the run log's events in order (each run with its file), and how many staging or build tables are left.
const RELOAD_CHECK: &str = "SELECT (SELECT string_agg(CAST(id AS VARCHAR) || ':' || v, ',' ORDER BY id) FROM raw.scores) || '|' || (SELECT string_agg(event || coalesce(':' || regexp_extract(json_extract_string(record_json, '$.files[0]'), '([a-z]+[.]csv)', 1), ''), ',' ORDER BY recorded_at) FROM raw._sqlload_runs WHERE table_name = 'scores') || '|' || (SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name LIKE '%sqlload_staging%')";

pub(crate) const MERGE_SETTINGS: &str = "write_strategy = \"merge\"\nunique_key = [\"id\"]\n";

pub(crate) const MERGE_FAIL_SETTINGS: &str =
    "write_strategy = \"merge\"\nunique_key = [\"id\"]\non_duplicate_keys = \"fail\"\n";

const UNORDERED_MERGE_SETTINGS: &str = "write_strategy = \"merge\"\nunique_key = [\"id\"]\nexclude_columns = [\"_sqlload_source_row\"]\n";

const UNORDERED_SORT_SETTINGS: &str = "write_strategy = \"merge\"\nunique_key = [\"id\"]\nexclude_columns = [\"_sqlload_source_row\"]\nnewest_by = \"v\"\n";

/// Monday's customers file: 42 once, and 43 twice, its newest row read first.
pub(crate) const MONDAY: &str = "id,v,day\n42,mon,2026-11-02\n43,a,2026-11-02\n43,b,2026-11-01\n";

pub(crate) const MERGE_SORT_SETTINGS: &str =
    "write_strategy = \"merge\"\nunique_key = [\"id\"]\nnewest_by = \"day\"\n";

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

const EU_NOV_3: ParquetFile = ParquetFile {
    key: "eu/2026-11-03_scores.parquet",
    ids: &[1, 2],
    score: 1.0,
    extra: None,
};
/// A compact date in the file name, stored as an ISO date.
const EU_NOV_4: ParquetFile = ParquetFile {
    key: "eu/20261104_scores.parquet",
    ids: &[3],
    score: 1.0,
    extra: None,
};
const EU_NOV_4_REWRITTEN: ParquetFile = ParquetFile {
    key: "eu/20261104_scores.parquet",
    ids: &[3, 4],
    score: 2.0,
    extra: None,
};
/// A file outside the template's layout.
const NOTES: ParquetFile = ParquetFile {
    key: "notes/readme.parquet",
    ids: &[9],
    score: 9.0,
    extra: None,
};

const PATH_RUNS: &[FilesRun] = &[
    FilesRun {
        files: &[EU_NOV_3, EU_NOV_4],
        expected_error: "",
    },
    FilesRun {
        files: &[EU_NOV_4_REWRITTEN],
        expected_error: "",
    },
];
const PATH_CHECK: &str = "SELECT string_agg(id || ':' || score || ':' || region || ':' || day || ':' || shop || ':' || day_count || ':' || as_of, ',' ORDER BY id) FROM raw.scores";
const PATH_RESULT: &str = "1:1.0:eu:2026-11-03:main:1:2026-11-08,2:1.0:eu:2026-11-03:main:1:2026-11-08,3:2.0:eu:2026-11-04:main:1:2026-11-08,4:2.0:eu:2026-11-04:main:1:2026-11-08";

/// Every row as `le_version:line:amount` (a NULL version as `NULL`), in order, then how many files the run log records.
const LE_CHECK: &str = "SELECT (SELECT string_agg(COALESCE(le_version, 'NULL') || ':' || line || ':' || CAST(amount AS VARCHAR), ',' ORDER BY le_version NULLS LAST, line) FROM raw.scores) || '|' || (SELECT CAST(count(*) AS VARCHAR) FROM raw._sqlload_runs WHERE table_name = 'scores' AND event = 'run')";

/// Every row as `le_version:line:amount`, in order, for a table without a run log.
const LE_ROWS: &str = "SELECT string_agg(le_version || ':' || line || ':' || CAST(amount AS VARCHAR), ',' ORDER BY le_version, line) FROM raw.scores";

const LE_SETTINGS: &str = "write_strategy = \"delete_insert\"\nreplace_key = [\"le_version\"]\n";

const LE_P01: MergeFile = MergeFile {
    key: "le_LE-FY27-P01.csv",
    contents: "le_version,line,amount\nLE-FY27-P01,A,1\nLE-FY27-P01,B,2\nLE-FY27-P01,C,3\n",
    modified_secs: 1_000,
};
const LE_P02: MergeFile = MergeFile {
    key: "le_LE-FY27-P02.csv",
    contents: "le_version,line,amount\nLE-FY27-P02,A,10\nLE-FY27-P02,B,20\n",
    modified_secs: 2_000,
};
/// P01 sent again corrected: line C removed, new amounts.
const LE_P01_RESENT: MergeFile = MergeFile {
    key: "le_LE-FY27-P01.csv",
    contents: "le_version,line,amount\nLE-FY27-P01,A,100\nLE-FY27-P01,B,200\n",
    modified_secs: 3_000,
};
/// P01 exported in two part files: lines A and B, then line C.
const LE_P01_PART_0: MergeFile = MergeFile {
    key: "part-0000.csv",
    contents: "le_version,line,amount\nLE-FY27-P01,A,1\nLE-FY27-P01,B,2\n",
    modified_secs: 1_000,
};
const LE_P01_PART_1: MergeFile = MergeFile {
    key: "part-0001.csv",
    contents: "le_version,line,amount\nLE-FY27-P01,C,3\n",
    modified_secs: 2_000,
};
/// The corrected P01 under another name, so it can arrive in the same run as the first.
const LE_P01_CORRECTED: MergeFile = MergeFile {
    key: "le_LE-FY27-P01_corrected.csv",
    contents: LE_P01_RESENT.contents,
    modified_secs: 3_000,
};

/// `delete_insert` on a cursor from the version folder each file lies in, so a version may come in several part files.
const LE_PARTS_SETTINGS: &str = "write_strategy = \"delete_insert\"\npath_columns = \"{file_version}/*.csv\"\ncursor_column = \"file_version\"\n";
const LE_P01_PART: MergeFile = MergeFile {
    key: "LE-FY27-P01/a.csv",
    contents: LE_P01.contents,
    modified_secs: 1_000,
};
const LE_P02_PART: MergeFile = MergeFile {
    key: "LE-FY27-P02/a.csv",
    contents: LE_P02.contents,
    modified_secs: 2_000,
};

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
            description: "on S3, on_empty = \"skip\" refuses an empty prefix before its table exists, as a mistyped path lists the same",
            store: seaweedfs_store,
            settings: "write_strategy = \"append\"\non_empty = \"skip\"\n",
            runs: &[FilesRun {
                files: &[],
                expected_error: "and scores was never loaded: an object store or SFTP server lists a folder that doesn't exist as empty, so on_empty = \"skip\" skips one only once its table exists",
            }],
            check_query: NO_TABLE,
            expected_result: "0",
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
            description: "merge reads parquet files directly, and a file rewritten in place merges again over its keys",
            store: local_store,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\npartition_columns = { day = \"date\" }\n",
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
            check_query: "SELECT CAST(count(*) AS VARCHAR) || ':' || CAST(sum(score) AS VARCHAR) || ':' || string_agg(COALESCE(extra, '-'), ',' ORDER BY id) FROM raw.scores",
            expected_result: "4:6.0:-,-,y,y",
        },
        FilesScenarioTestCase {
            description: "a files merge refuses a cursor_column, as its run log picks the files",
            store: local_store,
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\npartition_columns = { day = \"date\" }\ncursor_column = \"day\"\n",
            runs: &[FilesRun {
                files: &[NOV_3],
                expected_error: "does not apply to a files source's merge",
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
    run_async(|| async move {
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
        expected_plan: "batch_size is ignored: a files source commits in batches of whole partitions",
        expected_result: "1003,1001,1002,7/8/9",
    }];
    run_async(|| async move {
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
    run_async(|| async move {
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

#[test]
fn given_a_table_from_before_source_rows_when_loading_again_then_the_column_is_added_whatever_on_schema_change_says()
-> Result<(), String> {
    let test_cases = [
        SourceRowUpgradeTestCase {
            description: "by default",
            settings: "",
            new_file: true,
            expected_rows: "1:-,2:-,3:1",
        },
        SourceRowUpgradeTestCase {
            description: "on_schema_change = fail is not blocked: it is the loader's own column",
            settings: "on_schema_change = \"fail\"",
            new_file: true,
            expected_rows: "1:-,2:-,3:1",
        },
        SourceRowUpgradeTestCase {
            description: "with no new file the change is still made: the table is not skipped as up to date",
            settings: "",
            new_file: false,
            expected_rows: "1:-,2:-",
        },
    ];
    run_async(|| async move {
        for test_case in &test_cases {
            let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
            assert_eq!(
                run_source_row_upgrade(directory.path(), test_case)
                    .await?
                    .as_deref(),
                Some(test_case.expected_rows),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_files_in_plain_folders_when_path_columns_capture_a_date_then_delete_insert_replaces_by_it()
-> Result<(), String> {
    let test_cases = [
        FilesScenarioTestCase {
            description: "DuckDB reads the files itself, with the captured and constant values as literals",
            store: local_store,
            settings: concat!(
                "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\n",
                "path_columns = \"{region}/{day:date}_*.parquet\"\nconstant_columns = { shop = \"main\", day_count = 1, as_of = 2026-11-08 }\n",
            ),
            runs: PATH_RUNS,
            check_query: PATH_CHECK,
            expected_result: PATH_RESULT,
        },
        FilesScenarioTestCase {
            description: "SQLLoad's reader from S3 fills the same values",
            store: seaweedfs_store,
            settings: concat!(
                "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\ntest_loader_route = true\n",
                "path_columns = \"{region}/{day:date}_*.parquet\"\nconstant_columns = { shop = \"main\", day_count = 1, as_of = 2026-11-08 }\n",
            ),
            runs: PATH_RUNS,
            check_query: PATH_CHECK,
            expected_result: PATH_RESULT,
        },
        FilesScenarioTestCase {
            description: "a file outside the template fails the plan and nothing loads",
            store: local_store,
            settings: concat!(
                "write_strategy = \"append\"\n",
                "path_columns = \"{region}/{day:date}_*.parquet\"\nconstant_columns = { shop = \"main\", day_count = 1, as_of = 2026-11-08 }\n",
            ),
            runs: &[FilesRun {
                files: &[EU_NOV_3, NOTES],
                expected_error: "notes/readme.parquet does not match path_columns",
            }],
            check_query: NO_TABLE,
            expected_result: "0",
        },
        FilesScenarioTestCase {
            description: "a constant cannot be the cursor: it is the same in every file",
            store: local_store,
            settings: concat!(
                "write_strategy = \"delete_insert\"\ncursor_column = \"shop\"\n",
                "path_columns = \"{region}/{day:date}_*.parquet\"\nconstant_columns = { shop = \"main\", day_count = 1, as_of = 2026-11-08 }\n",
            ),
            runs: &[FilesRun {
                files: &[EU_NOV_3],
                expected_error: "cursor_column shop is a constant column",
            }],
            check_query: NO_TABLE,
            expected_result: "0",
        },
        FilesScenarioTestCase {
            description: "unmatched_paths = skip leaves it out",
            store: local_store,
            settings: concat!(
                "write_strategy = \"append\"\nunmatched_paths = \"skip\"\n",
                "path_columns = \"{region}/{day:date}_*.parquet\"\nconstant_columns = { shop = \"main\", day_count = 1, as_of = 2026-11-08 }\n",
            ),
            runs: &[FilesRun {
                files: &[EU_NOV_3, NOTES],
                expected_error: "",
            }],
            check_query: PATH_CHECK,
            expected_result: "1:1.0:eu:2026-11-03:main:1:2026-11-08,2:1.0:eu:2026-11-03:main:1:2026-11-08",
        },
        FilesScenarioTestCase {
            description: "a table's own constant_columns replace its source's, read by DuckDB itself",
            store: local_store,
            settings: concat!(
                "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\n",
                "path_columns = \"{region}/{day:date}_*.parquet\"\nconstant_columns = { shop = \"source\" }\n",
                "[sources.scores.tables.scores]\nconstant_columns = { shop = \"main\", day_count = 1, as_of = 2026-11-08 }\n",
            ),
            runs: PATH_RUNS,
            check_query: PATH_CHECK,
            expected_result: PATH_RESULT,
        },
        FilesScenarioTestCase {
            description: "a table's own constant_columns replace its source's, read by SQLLoad's reader from S3",
            store: seaweedfs_store,
            settings: concat!(
                "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\ntest_loader_route = true\n",
                "path_columns = \"{region}/{day:date}_*.parquet\"\nconstant_columns = { shop = \"source\" }\n",
                "[sources.scores.tables.scores]\nconstant_columns = { shop = \"main\", day_count = 1, as_of = 2026-11-08 }\n",
            ),
            runs: PATH_RUNS,
            check_query: PATH_CHECK,
            expected_result: PATH_RESULT,
        },
        FilesScenarioTestCase {
            description: "a table's constant replaces a file column of its name, as a files source always has",
            store: local_store,
            settings: concat!(
                "write_strategy = \"append\"\npartition_columns = { day = \"date\" }\n",
                "[sources.scores.tables.scores]\nconstant_columns = { extra = \"c\" }\n",
            ),
            runs: &[FilesRun {
                files: &[NOV_4, NOV_5],
                expected_error: "",
            }],
            check_query: "SELECT string_agg(id || ':' || extra, ',' ORDER BY id) FROM raw.scores",
            expected_result: "3:c,5:c",
        },
        FilesScenarioTestCase {
            description: "DuckDB reads the files itself and loads a renamed file column under its new name",
            store: local_store,
            settings: concat!(
                "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\n",
                "path_columns = \"{region}/{day:date}_*.parquet\"\nconstant_columns = { shop = \"main\", day_count = 1, as_of = 2026-11-08 }\n",
                "[sources.scores.tables.scores]\nrename = { score = \"points\" }\n",
            ),
            runs: PATH_RUNS,
            check_query: "SELECT string_agg(id || ':' || points || ':' || region || ':' || day || ':' || shop || ':' || day_count || ':' || as_of, ',' ORDER BY id) FROM raw.scores",
            expected_result: PATH_RESULT,
        },
    ];
    run_async(|| async move {
        let server = start_seaweedfs().await?;
        create_bucket(&server, "example-bucket-test").await?;
        for test_case in test_cases {
            let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
            let result = run_files_scenario(&server.endpoint, directory.path(), &test_case).await;
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
fn given_csv_files_when_merging_into_duckdb_then_the_newest_modified_file_wins_and_each_file_merges_once()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [
            FilesMergeTestCase {
                description: "a key repeated across files ends with the newest modified file's value, whatever the names",
                settings: MERGE_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[
                        MergeFile {
                            key: "c.csv",
                            contents: "id,v\n1,c\n2,c\n",
                            modified_secs: 1_000,
                        },
                        MergeFile {
                            key: "a.csv",
                            contents: "id,v\n1,a\n3,a\n",
                            modified_secs: 2_000,
                        },
                        MergeFile {
                            key: "b.csv",
                            contents: "id,v\n1,b\n",
                            modified_secs: 3_000,
                        },
                    ],
                    expected_error: "",
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_plan: "3 files, one commit each, oldest modified first (ties by path)",
                }],
                check_query: MERGE_CHECK,
                expected_result: "1:b,2:c,3:a|3",
            },
            FilesMergeTestCase {
                description: "files modified at the same time merge in path order",
                settings: MERGE_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[
                        MergeFile {
                            key: "b.csv",
                            contents: "id,v\n1,b\n",
                            modified_secs: 1_000,
                        },
                        MergeFile {
                            key: "a.csv",
                            contents: "id,v\n1,a\n",
                            modified_secs: 1_000,
                        },
                    ],
                    expected_error: "",
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_plan: "",
                }],
                check_query: MERGE_CHECK,
                expected_result: "1:b|2",
            },
            FilesMergeTestCase {
                description: "with order = \"path\" files merge in name order whatever their modified times, so a key repeated across files ends with the last name's value",
                settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\norder = \"path\"\n",
                steps: &[MergeStep {
                    before: &[],
                    files: &[
                        MergeFile {
                            key: "export_2026-11-12.csv",
                            contents: "id,v\n1,mon\n2,mon\n",
                            modified_secs: 3_000,
                        },
                        MergeFile {
                            key: "export_2026-11-13.csv",
                            contents: "id,v\n1,tue\n",
                            modified_secs: 2_000,
                        },
                        MergeFile {
                            key: "export_2026-11-14.csv",
                            contents: "id,v\n1,wed\n3,wed\n",
                            modified_secs: 1_000,
                        },
                    ],
                    expected_error: "",
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_plan: "3 files, one commit each, in path order",
                }],
                check_query: MERGE_CHECK,
                expected_result: "1:wed,2:mon,3:wed|3",
            },
            FilesMergeTestCase {
                description: "order = \"path\" on an append table, which loads files without an order, is refused at plan",
                settings: "write_strategy = \"append\"\norder = \"path\"\n",
                steps: &[MergeStep {
                    before: &[],
                    files: &[MergeFile {
                        key: "a.csv",
                        contents: "id,v\n1,a\n",
                        modified_secs: 1_000,
                    }],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "order = \"path\" orders the files a merge or a delete_insert by replace_key applies one at a time; append loads them without an order, so remove it",
                    expected_plan: "",
                }],
                check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'scores'",
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "by default a file repeating a key merges its last row of that key",
                settings: MERGE_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[MergeFile {
                        key: "a.csv",
                        contents: "id,v\n5,b\n6,c\n5,a\n",
                        modified_secs: 1_000,
                    }],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "",
                    expected_plan: "merge (unique_key: id; NULLs in keys match)",
                }],
                check_query: MERGE_CHECK,
                expected_result: "5:a,6:c|1",
            },
            FilesMergeTestCase {
                description: "with newest_by a file repeating a key merges its newest row, and Monday's file coming back after Tuesday's, rewritten, leaves Tuesday's row, the load saying so",
                settings: MERGE_SORT_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "mon.csv",
                            contents: MONDAY,
                            modified_secs: 1_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "merge (unique_key: id; newest by day",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "tue.csv",
                            contents: "id,v,day\n42,tue,2026-11-03\n",
                            modified_secs: 2_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "mon.csv",
                            contents: MONDAY,
                            modified_secs: 3_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "(1 older row skipped: newest_by day)",
                    },
                ],
                check_query: MERGE_CHECK,
                expected_result: "42:tue,43:a|3",
            },
            FilesMergeTestCase {
                description: "without _sqlload_source_row a file's rows have no order, so files merge as in 0.38.0: one without repeats merges, one repeating a key fails, and plan says why",
                settings: UNORDERED_MERGE_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "a.csv",
                            contents: "id,v\n1,a\n2,a\n",
                            modified_secs: 1_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "_sqlload_source_row is not written, so a file's rows have no order to pick the last of a repeated key by",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "b.csv",
                            contents: "id,v\n1,b\n1,bb\n",
                            modified_secs: 2_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "drop/b.csv: the file repeats its unique_key: 1 value of (id) appears in more than one row, such as (1)",
                        expected_plan: "",
                    },
                ],
                check_query: MERGE_CHECK,
                expected_result: "1:a,2:a|1",
            },
            FilesMergeTestCase {
                description: "newest_by without _sqlload_source_row is refused at plan, as ties need the file's row order",
                settings: UNORDERED_SORT_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[MergeFile {
                        key: "a.csv",
                        contents: "id,v\n1,a\n",
                        modified_secs: 1_000,
                    }],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "newest_by breaks ties by row order, which a files merge takes from _sqlload_source_row; stop excluding it",
                    expected_plan: "",
                }],
                check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'scores'",
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "with on_duplicate_keys = \"fail\" a file repeating a key no row has yet fails before it merges, naming the key",
                settings: MERGE_FAIL_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[MergeFile {
                        key: "a.csv",
                        contents: "id,v\n5,a\n5,b\n6,c\n",
                        modified_secs: 1_000,
                    }],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "drop/a.csv: the file repeats its unique_key: 1 value of (id) appears in more than one row, such as (5)",
                    expected_plan: "",
                }],
                check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.scores",
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "a failure in the second file keeps the first committed, names the file, and a rerun merges from it without merging the first again; a run with nothing new merges nothing",
                settings: MERGE_FAIL_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[
                            MergeFile {
                                key: "a.csv",
                                contents: "id,v\n1,a\n2,a\n",
                                modified_secs: 1_000,
                            },
                            MergeFile {
                                key: "b.csv",
                                contents: "id,v\n1,b\n1,bb\n",
                                modified_secs: 2_000,
                            },
                            MergeFile {
                                key: "c.csv",
                                contents: "id,v\n2,c\n",
                                modified_secs: 3_000,
                            },
                        ],
                        expected_error: "drop/b.csv: the file repeats its unique_key: 1 value of (id) appears in more than one row, such as (1)",
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "b.csv",
                            contents: "id,v\n1,b\n",
                            modified_secs: 2_000,
                        }],
                        expected_error: "",
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_plan: "2 files, one commit each",
                    },
                    MergeStep {
                        before: &[],
                        files: &[],
                        expected_error: "",
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_plan: "",
                    },
                ],
                check_query: MERGE_CHECK,
                expected_result: "1:b,2:c|3",
            },
            FilesMergeTestCase {
                description: "a table dropped, then created again by apply alone, merges every file again",
                settings: MERGE_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[
                            MergeFile {
                                key: "a.csv",
                                contents: "id,v\n1,a\n",
                                modified_secs: 1_000,
                            },
                            MergeFile {
                                key: "b.csv",
                                contents: "id,v\n1,b\n",
                                modified_secs: 2_000,
                            },
                        ],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &["DROP TABLE raw.scores"],
                        files: &[],
                        removed: &[],
                        loads: false,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "2 files, one commit each",
                    },
                ],
                check_query: MERGE_CHECK,
                expected_result: "1:b|4",
            },
            FilesMergeTestCase {
                description: "a file changed back to a version merged before merges again, as the newest",
                settings: MERGE_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "a.csv",
                            contents: "id,v\n1,a\n",
                            modified_secs: 1_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "a.csv",
                            contents: "id,v\n1,c\n",
                            modified_secs: 2_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "1 file, one commit each",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "a.csv",
                            contents: "id,v\n1,a\n",
                            modified_secs: 1_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "1 file, one commit each",
                    },
                ],
                check_query: MERGE_CHECK,
                expected_result: "1:a|3",
            },
            FilesMergeTestCase {
                description: "a merged file rewritten in place merges again, as the newest",
                settings: MERGE_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[
                            MergeFile {
                                key: "a.csv",
                                contents: "id,v\n1,a\n",
                                modified_secs: 1_000,
                            },
                            MergeFile {
                                key: "b.csv",
                                contents: "id,v\n1,b\n",
                                modified_secs: 2_000,
                            },
                        ],
                        expected_error: "",
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "a.csv",
                            contents: "id,v\n1,a2\n",
                            modified_secs: 3_000,
                        }],
                        expected_error: "",
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_plan: "1 file, one commit each",
                    },
                ],
                check_query: MERGE_CHECK,
                expected_result: "1:a2|3",
            },
            FilesMergeTestCase {
                description: "a row without an id in one file is updated by the next file's row without one, not inserted again, as a NULL in the key matches a NULL",
                settings: MERGE_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[
                        MergeFile {
                            key: "a.csv",
                            contents: "id,v\n1,a\n,a\n",
                            modified_secs: 1_000,
                        },
                        MergeFile {
                            key: "b.csv",
                            contents: "id,v\n,b\n,bb\n",
                            modified_secs: 2_000,
                        },
                    ],
                    expected_error: "",
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_plan: "merge (unique_key: id; NULLs in keys match)",
                }],
                check_query: NULL_MERGE_CHECK,
                expected_result: "1:a,NULL:bb|2",
            },
            FilesMergeTestCase {
                description: "with null_keys = \"fail\" a file holding a row without an id fails before it merges, naming the key, and the file before it stays merged",
                settings: NULL_FAIL_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[
                        MergeFile {
                            key: "a.csv",
                            contents: "id,v\n1,a\n",
                            modified_secs: 1_000,
                        },
                        MergeFile {
                            key: "b.csv",
                            contents: "id,v\n2,b\n,b\n",
                            modified_secs: 2_000,
                        },
                    ],
                    expected_error: "drop/b.csv: the file holds a NULL in its unique_key: 1 row(s) have no id, such as (NULL) of (id); null_keys = \"fail\" refuses a NULL in a key column",
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_plan: "",
                }],
                check_query: NULL_MERGE_CHECK,
                expected_result: "1:a|1",
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
fn given_a_merge_files_table_when_reloading_then_every_file_merges_again_in_order_into_a_new_table_swapped_in_with_its_records()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [
            FilesMergeTestCase {
                description: "--reload merges every file oldest first into a new table, records a reset and each file with the swap, and the next load reads none",
                settings: MERGE_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[
                            MergeFile {
                                key: "a.csv",
                                contents: "id,v\n1,a\n2,a\n",
                                modified_secs: 1_000,
                            },
                            MergeFile {
                                key: "b.csv",
                                contents: "id,v\n1,b\n",
                                modified_secs: 2_000,
                            },
                        ],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "c.csv",
                            contents: "id,v\n1,c\n",
                            modified_secs: 3_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: true,
                        expected_error: "",
                        expected_plan: "merge (unique_key: id; NULLs in keys match) into a new table, swapped in at the end (--reload)",
                    },
                    MergeStep {
                        before: &[],
                        files: &[],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "up to date: 0 new of 3 files in",
                    },
                ],
                check_query: RELOAD_CHECK,
                expected_result: "1:c,2:a|reset,run:a.csv,run:b.csv,reset,run:a.csv,run:b.csv,run:c.csv|0",
            },
            FilesMergeTestCase {
                description: "a --reload failing on its third file leaves the old table, its run log and no build table",
                settings: MERGE_FAIL_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[
                            MergeFile {
                                key: "a.csv",
                                contents: "id,v\n1,a\n2,a\n",
                                modified_secs: 1_000,
                            },
                            MergeFile {
                                key: "b.csv",
                                contents: "id,v\n1,b\n",
                                modified_secs: 2_000,
                            },
                        ],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "c.csv",
                            contents: "id,v\n3,c\n3,cc\n",
                            modified_secs: 3_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: true,
                        expected_error: "drop/c.csv: the file repeats its unique_key",
                        expected_plan: "",
                    },
                ],
                check_query: RELOAD_CHECK,
                expected_result: "1:b,2:a|reset,run:a.csv,run:b.csv|0",
            },
            FilesMergeTestCase {
                description: "a new file named as one move_to archived merges after the archived copy, so its newer rows win",
                settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\nafter_load = { move_to = \"done\" }\n",
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "export.csv",
                            contents: "id,v\n1,a\n",
                            modified_secs: 1_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "export.csv",
                            contents: "id,v\n1,b\n",
                            modified_secs: 2_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: true,
                        expected_error: "",
                        expected_plan: "",
                    },
                ],
                check_query: RELOAD_CHECK,
                expected_result: "1:b|reset,run:export.csv,reset,run:export.csv,run:export.csv|0",
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
fn given_files_appended_when_their_modified_times_are_shuffled_then_the_load_is_as_before_and_writes_no_run_log()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [FilesMergeTestCase {
            description: "append ignores modified times and keeps no run log",
            settings: "write_strategy = \"append\"\n",
            steps: &[MergeStep {
                before: &[],
                files: &[
                    MergeFile {
                        key: "a.csv",
                        contents: "id,v\n1,a\n",
                        modified_secs: 3_000,
                    },
                    MergeFile {
                        key: "b.csv",
                        contents: "id,v\n1,b\n",
                        modified_secs: 1_000,
                    },
                ],
                expected_error: "",
                removed: &[],
                loads: true,
                reload: false,
                expected_plan: "1 batch, up to 1,000 files or about 1 GB per commit",
            }],
            check_query: "SELECT (SELECT string_agg(CAST(id AS VARCHAR) || ':' || v, ',' ORDER BY v) FROM raw.scores) || '|' || (SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = '_sqlload_runs')",
            expected_result: "1:a,1:b|0",
        }];
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
fn given_a_resent_older_file_when_delete_insert_follows_a_path_cursor_then_the_table_fails_naming_the_fixes_instead_of_skipping_it()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [
            FilesMergeTestCase {
                description: "P01 sent again after P02 lies below the cursor read from P02, so the table fails naming replace_key, --start-cursor and --reload, and keeps its rows",
                settings: "write_strategy = \"delete_insert\"\npath_columns = \"le_{file_version}.csv\"\ncursor_column = \"file_version\"\n",
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[LE_P01],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "delete_insert (cursor: file_version)",
                    },
                    MergeStep {
                        before: &[],
                        files: &[LE_P02],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[LE_P01_RESENT],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "le_LE-FY27-P01.csv changed since it was loaded",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "but its file_version LE-FY27-P01 is below this load's range, which starts at LE-FY27-P02, so delete_insert would skip it and keep its old rows └── fix set replace_key to replace rows by a column the files hold (and drop cursor_column), load from it with --start-cursor LE-FY27-P01, or rebuild the table with --reload",
                        expected_plan: "",
                    },
                ],
                check_query: LE_ROWS,
                expected_result: "LE-FY27-P01:A:1,LE-FY27-P01:B:2,LE-FY27-P01:C:3,LE-FY27-P02:A:10,LE-FY27-P02:B:20",
            },
            FilesMergeTestCase {
                description: "a new part file of P01 added after P02 lies below the cursor in a partition the table holds, so the table fails naming it and the fixes, instead of never loading its rows",
                settings: LE_PARTS_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[LE_P01_PART],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[LE_P02_PART],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "LE-FY27-P01/b.csv",
                            contents: "le_version,line,amount\nLE-FY27-P01,D,4\n",
                            modified_secs: 3_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "drop/LE-FY27-P01/b.csv is new, but its file_version LE-FY27-P01 is below this load's range, which starts at LE-FY27-P02, and the table holds other files of file_version LE-FY27-P01, so delete_insert would skip it and never load its rows └── fix set replace_key to replace rows by a column the files hold (and drop cursor_column), load from it with --start-cursor LE-FY27-P01, or rebuild the table with --reload",
                        expected_plan: "",
                    },
                ],
                check_query: LE_ROWS,
                expected_result: "LE-FY27-P01:A:1,LE-FY27-P01:B:2,LE-FY27-P01:C:3,LE-FY27-P02:A:10,LE-FY27-P02:B:20",
            },
            FilesMergeTestCase {
                description: "a table without _sqlload_source_version can't tell a changed file below the cursor, so plan refuses it, naming the fix",
                settings: LE_PARTS_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[LE_P01_PART],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &["ALTER TABLE raw.scores DROP COLUMN _sqlload_source_version"],
                        files: &[LE_P02_PART],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "the table has no _sqlload_source_version column, so delete_insert cannot find a file changed or added below this load's range, which it skips, keeping the partition's old rows",
                        expected_plan: "",
                    },
                ],
                check_query: LE_ROWS,
                expected_result: "LE-FY27-P01:A:1,LE-FY27-P01:B:2,LE-FY27-P01:C:3",
            },
            FilesMergeTestCase {
                description: "excluding _sqlload_source_file from such a table is refused at plan, naming the column",
                settings: "write_strategy = \"delete_insert\"\npath_columns = \"{file_version}/*.csv\"\ncursor_column = \"file_version\"\nexclude_columns = [\"_sqlload_source_file\"]\n",
                steps: &[MergeStep {
                    before: &[],
                    files: &[LE_P01_PART],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "_sqlload_source_file must be loaded: delete_insert uses it to find a file changed or added below this load's range, which it would skip; stop excluding it",
                    expected_plan: "",
                }],
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
fn given_le_files_when_delete_insert_replaces_by_replace_key_then_each_version_holds_the_rows_of_its_newest_file()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [
            FilesMergeTestCase {
                description: "across runs, P01 sent again without line C replaces P01's rows whole, P02 stays, and a run with no new file loads nothing again",
                settings: LE_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[LE_P01],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "delete_insert (replace_key: le_version; NULLs in keys match)",
                    },
                    MergeStep {
                        before: &[],
                        files: &[LE_P02],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "1 file, one commit each, oldest modified first (ties by path), each recorded in the run log",
                    },
                    MergeStep {
                        before: &[],
                        files: &[LE_P01_RESENT],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "├── files   1 new of 2 listed",
                    },
                    MergeStep {
                        before: &[],
                        files: &[],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "reason  up to date: 0 new of 2 files in",
                    },
                ],
                check_query: LE_CHECK,
                expected_result: "LE-FY27-P01:A:100,LE-FY27-P01:B:200,LE-FY27-P02:A:10,LE-FY27-P02:B:20|3",
            },
            FilesMergeTestCase {
                description: "in one run, a corrected P01 modified after the first P01 fails, naming both files, as the run would keep only one of them; the files before it stay",
                settings: LE_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[LE_P01_CORRECTED, LE_P02, LE_P01],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "drop/le_LE-FY27-P01_corrected.csv: it holds the replace_key (le_version) value (LE-FY27-P01), which le_LE-FY27-P01.csv also holds, loaded earlier in this run; each file replaces the rows of the values it holds, so it would delete the rows le_LE-FY27-P01.csv loaded, as a value split across part files would keep only its last part. One file must hold all of a value's rows",
                    expected_plan: "",
                }],
                check_query: LE_CHECK,
                expected_result: "LE-FY27-P01:A:1,LE-FY27-P01:B:2,LE-FY27-P01:C:3,LE-FY27-P02:A:10,LE-FY27-P02:B:20|2",
            },
            FilesMergeTestCase {
                description: "a version split across two part files of one run fails on the later modified, naming both, whatever their names, instead of keeping only its lines",
                settings: LE_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[
                        MergeFile {
                            key: "b.csv",
                            contents: "le_version,line,amount\nLE-FY27-P05,A,1\nLE-FY27-P05,B,2\nLE-FY27-P05,C,3\n",
                            modified_secs: 1_000,
                        },
                        MergeFile {
                            key: "a.csv",
                            contents: "le_version,line,amount\nLE-FY27-P05,D,4\n",
                            modified_secs: 2_000,
                        },
                    ],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "drop/a.csv: it holds the replace_key (le_version) value (LE-FY27-P05), which b.csv also holds, loaded earlier in this run",
                    expected_plan: "",
                }],
                check_query: LE_CHECK,
                expected_result: "LE-FY27-P05:A:1,LE-FY27-P05:B:2,LE-FY27-P05:C:3|1",
            },
            FilesMergeTestCase {
                description: "a later run's file whose rows have no version replaces the rows without one, as a NULL in the key matches a NULL by default, once the file they came from has left the folder",
                settings: LE_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "a.csv",
                            contents: "le_version,line,amount\n,A,1\n,B,2\nLE-FY27-P01,A,5\n",
                            modified_secs: 1_000,
                        }],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[MergeFile {
                            key: "b.csv",
                            contents: "le_version,line,amount\n,C,3\n",
                            modified_secs: 2_000,
                        }],
                        removed: &["a.csv"],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                ],
                check_query: LE_CHECK,
                expected_result: "LE-FY27-P01:A:5,NULL:C:3|2",
            },
            FilesMergeTestCase {
                description: "with null_keys = \"fail\" a file holding a row without a version fails before it loads, naming replace_key, and the file before it stays",
                settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"le_version\"]\nnull_keys = \"fail\"\n",
                steps: &[MergeStep {
                    before: &[],
                    files: &[
                        LE_P02,
                        MergeFile {
                            key: "b.csv",
                            contents: "le_version,line,amount\n,C,3\n",
                            modified_secs: 3_000,
                        },
                    ],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "drop/b.csv: the file holds a NULL in its replace_key: 1 row(s) have no le_version, such as (NULL) of (le_version); null_keys = \"fail\" refuses a NULL in a key column",
                    expected_plan: "",
                }],
                check_query: LE_CHECK,
                expected_result: "LE-FY27-P02:A:10,LE-FY27-P02:B:20|1",
            },
            FilesMergeTestCase {
                description: "a version split across two part files fails on the second in the first run, and again when the run is retried, which loads it alone, keeping the first part's rows",
                settings: LE_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[LE_P01_PART_0, LE_P01_PART_1],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "drop/part-0001.csv: it holds the replace_key (le_version) value (LE-FY27-P01), which part-0000.csv also holds, loaded earlier in this run; each file replaces the rows of the values it holds, so it would delete the rows part-0000.csv loaded, as a value split across part files would keep only its last part. One file must hold all of a value's rows. Fix: put all the rows of each (le_version) value in one file, or remove whichever of the two files is outdated from the folder",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "drop/part-0001.csv: the table holds rows of its replace_key (le_version) values from part-0000.csv, which is still in the folder; with two files of the folder holding one value, which of them is current can't be told, so replacing the value by part-0001.csv would delete the rows part-0000.csv loaded, as a value split across part files would keep only its last part. One file must hold all of a value's rows. Fix: put all the rows of each (le_version) value in one file, or remove the outdated file, part-0000.csv or part-0001.csv, from the folder",
                        expected_plan: "",
                    },
                ],
                check_query: LE_CHECK,
                expected_result: "LE-FY27-P01:A:1,LE-FY27-P01:B:2|1",
            },
            FilesMergeTestCase {
                description: "a corrected P01 under another name fails while the old P01 is still in the folder, naming both, and replaces P01 whole once the old file is removed",
                settings: LE_SETTINGS,
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[LE_P01, LE_P02],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[LE_P01_CORRECTED],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "drop/le_LE-FY27-P01_corrected.csv: the table holds rows of its replace_key (le_version) values from le_LE-FY27-P01.csv, which is still in the folder",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[],
                        removed: &["le_LE-FY27-P01.csv"],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "├── files   1 new of 2 listed",
                    },
                ],
                check_query: LE_CHECK,
                expected_result: "LE-FY27-P01:A:100,LE-FY27-P01:B:200,LE-FY27-P02:A:10,LE-FY27-P02:B:20|3",
            },
            FilesMergeTestCase {
                description: "with after_load moving each file once it loads, P01 sent again into the folder still replaces P01's rows",
                settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"le_version\"]\nafter_load = { move_to = \"loaded\" }\n",
                steps: &[
                    MergeStep {
                        before: &[],
                        files: &[LE_P01, LE_P02],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[LE_P01_RESENT],
                        removed: &[],
                        loads: true,
                        reload: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                ],
                check_query: LE_CHECK,
                expected_result: "LE-FY27-P01:A:100,LE-FY27-P01:B:200,LE-FY27-P02:A:10,LE-FY27-P02:B:20|3",
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
fn given_replace_key_when_planning_a_table_it_does_not_suit_then_plan_refuses_it_naming_the_fix()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [
            FilesMergeTestCase {
                description: "replace_key with merge",
                settings: "write_strategy = \"merge\"\nunique_key = [\"line\"]\nreplace_key = [\"le_version\"]\n",
                steps: &[MergeStep {
                    before: &[],
                    files: &[LE_P01],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "replace_key is for write_strategy = \"delete_insert\", which replaces the rows holding the key values a load reads; merge does not delete by it └── fix set write_strategy = \"delete_insert\" to replace those rows whole, or drop replace_key (merge matches rows one by one on unique_key)",
                    expected_plan: "",
                }],
                check_query: NO_TABLE,
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "replace_key with append",
                settings: "write_strategy = \"append\"\nreplace_key = [\"le_version\"]\n",
                steps: &[MergeStep {
                    before: &[],
                    files: &[LE_P01],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "replace_key is for write_strategy = \"delete_insert\", which replaces the rows holding the key values a load reads; append does not delete by it └── fix set write_strategy = \"delete_insert\" to replace those rows whole, or drop replace_key",
                    expected_plan: "",
                }],
                check_query: NO_TABLE,
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "replace_key with replace",
                settings: "write_strategy = \"replace\"\nreplace_key = [\"le_version\"]\n",
                steps: &[MergeStep {
                    before: &[],
                    files: &[LE_P01],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "replace_key is for write_strategy = \"delete_insert\", which replaces the rows holding the key values a load reads; replace does not delete by it └── fix set write_strategy = \"delete_insert\" to replace those rows whole, or drop replace_key",
                    expected_plan: "",
                }],
                check_query: NO_TABLE,
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "a replace_key column the table doesn't have",
                settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"version\"]\n",
                steps: &[MergeStep {
                    before: &[],
                    files: &[LE_P01],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "replace_key column \"version\" is not a column of the table",
                    expected_plan: "",
                }],
                check_query: NO_TABLE,
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "delete_insert with neither a cursor nor a replace_key",
                settings: "write_strategy = \"delete_insert\"\n",
                steps: &[MergeStep {
                    before: &[],
                    files: &[LE_P01],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "write_strategy = \"delete_insert\" replaces a cursor range or the rows of the key values it reads, so it needs a cursor_column or a replace_key └── fix set cursor_column, or replace_key = [\"version\"]",
                    expected_plan: "",
                }],
                check_query: NO_TABLE,
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "a files source's replace_key excluding _sqlload_source_file, which it finds a value's other files by",
                settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"le_version\"]\nexclude_columns = [\"_sqlload_source_file\"]\n",
                steps: &[MergeStep {
                    before: &[],
                    files: &[LE_P01],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "_sqlload_source_file must be loaded: delete_insert uses it to find the files a replace_key value's rows come from; stop excluding it",
                    expected_plan: "",
                }],
                check_query: NO_TABLE,
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "a files source's replace_key with a cursor_column",
                settings: "write_strategy = \"delete_insert\"\nreplace_key = [\"le_version\"]\npath_columns = \"le_{file_version}.csv\"\ncursor_column = \"file_version\"\n",
                steps: &[MergeStep {
                    before: &[],
                    files: &[LE_P01],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "cursor_column file_version does not apply to a files source's delete_insert by replace_key, which reads each new or changed file once, oldest first, from the run log └── fix remove cursor_column",
                    expected_plan: "",
                }],
                check_query: NO_TABLE,
                expected_result: "0",
            },
            FilesMergeTestCase {
                description: "a cursor_column the files hold, which the plan points to replace_key",
                settings: "write_strategy = \"delete_insert\"\ncursor_column = \"le_version\"\n",
                steps: &[MergeStep {
                    before: &[],
                    files: &[LE_P01],
                    removed: &[],
                    loads: true,
                    reload: false,
                    expected_error: "cursor_column le_version must be one of the partition_columns or path_columns for a files source; to replace every row of each le_version value the files hold, set replace_key = [\"le_version\"] and drop cursor_column",
                    expected_plan: "",
                }],
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
fn given_delete_insert_by_replace_key_when_loading_with_json_then_each_table_reports_the_key_values_it_replaced()
-> Result<(), String> {
    let test_cases = [ReplacedKeysTestCase {
        description: "the first run reports both versions it loaded, the second only the version sent again",
        steps: &[&[LE_P01, LE_P02], &[LE_P01_RESENT]],
        expected_reports: &[
            r#"{"columns":["le_version"],"truncated":false,"values":["LE-FY27-P01","LE-FY27-P02"]}"#,
            r#"{"columns":["le_version"],"truncated":false,"values":["LE-FY27-P01"]}"#,
        ],
    }];
    for test_case in &test_cases {
        let reports = run_replaced_keys(test_case, LE_SETTINGS)?;
        assert_eq!(
            reports, test_case.expected_reports,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_partition_cursor_without_the_source_file_column_when_reloading_then_only_runs_that_check_below_the_range_refuse_it()
-> Result<(), String> {
    run_async(|| async move {
        let test_cases = [AfterLoadTestCase {
            description: "--reload rebuilds the table without finding files below a range, so it loads without _sqlload_source_file; a run from the table's maximum needs the column and is refused, naming it",
            settings: "write_strategy = \"delete_insert\"\npath_columns = \"{file_version}/*.csv\"\ncursor_column = \"file_version\"\nexclude_columns = [\"_sqlload_source_file\"]\n",
            linked: &[],
            steps: &[
                AfterLoadStep {
                    files: &[("drop/LE-FY27-P01/a.csv", LE_P01.contents)],
                    removed: &[],
                    args: &["--reload", "--yes"],
                    expected_exit: 0,
                    expected_report: r#"{"after_load":null,"problems":[],"rows":3,"skipped":[],"status":"loaded"}"#,
                },
                AfterLoadStep {
                    files: &[],
                    removed: &[],
                    args: &[],
                    expected_exit: 1,
                    expected_report: r#"{"after_load":null,"problems":["_sqlload_source_file must be loaded: delete_insert uses it to find a file changed or added below this load's range, which it would skip; stop excluding it"],"rows":null,"skipped":[],"status":null}"#,
                },
            ],
            expected_files: &[
                "drop/LE-FY27-P01/a.csv=le_version,line,amount\\nLE-FY27-P01,A,1\\nLE-FY27-P01,B,2\\nLE-FY27-P01,C,3\\n",
            ],
        }];
        for test_case in &test_cases {
            let files = run_after_load_case(test_case).await?;
            assert_eq!(files, test_case.expected_files, "{}", test_case.description);
        }
        Ok(())
    })
}

#[test]
fn given_a_load_when_it_reports_then_the_json_and_the_run_finished_event_carry_the_run_peak_memory()
-> Result<(), String> {
    let test_cases = [PeakMemoryTestCase {
        description: "a load of two tables reports its peak, and no table's without --memory-per-table",
        flags: &[],
        expected_tables: &[false, false],
    }];
    for test_case in &test_cases {
        let report = run_peak_memory(test_case)?;
        assert!(
            report.run >= MEGABYTE && report.finished >= MEGABYTE,
            "{}: {report:?}",
            test_case.description
        );
        assert_eq!(
            report.tables, test_case.expected_tables,
            "{}: {report:?}",
            test_case.description
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn given_memory_per_table_when_loading_then_each_table_reports_its_peak_in_the_json_and_its_table_done_event()
-> Result<(), String> {
    let test_cases = [PeakMemoryTestCase {
        description: "each table's peak, at most the run's, in the report and in table_done",
        flags: &["--memory-per-table"],
        expected_tables: &[true, true],
    }];
    for test_case in &test_cases {
        let report = run_peak_memory(test_case)?;
        assert!(
            report.run >= MEGABYTE && report.finished >= MEGABYTE,
            "{}: {report:?}",
            test_case.description
        );
        assert_eq!(
            report.tables, test_case.expected_tables,
            "{}: {report:?}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_later_batch_holding_a_column_duckdb_cannot_read_exactly_when_duckdb_reads_the_files_itself_then_the_load_stops_there()
-> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    let test_cases = [FileSetTestCase {
        description: "a batch of INT96 under embedded microseconds, which plan's sample of the newest files never saw, is refused by its own footers; the batch before it stays loaded",
        settings: "write_strategy = \"append\"\n",
        file_set: int96_in_a_later_batch,
        expected_error: "DuckDB cannot load this batch's files exactly by reading them itself, and they never go through SQLLoad instead: column at is held as",
        check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.scores",
        expected_result: "1000",
    }];
    run_async(|| async move {
        for test_case in &test_cases {
            assert_eq!(
                run_file_set(directory.path(), test_case).await?.as_deref(),
                Some(test_case.expected_result),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_an_append_parquet_table_with_move_to_when_reloading_then_each_archived_file_is_read_from_where_it_was_moved()
-> Result<(), String> {
    let test_cases = [ArchivedReloadTestCase {
        description: "a.parquet loaded and moved to done/, then a new drop/a.parquet: the reload reads each copy once, the archived one from done/",
        first: &[ParquetFile {
            key: "a.parquet",
            ids: &[1],
            score: 1.0,
            extra: None,
        }],
        later: &[ParquetFile {
            key: "a.parquet",
            ids: &[2],
            score: 2.0,
            extra: None,
        }],
        expected_plan: "route   read parquet into Arrow -> DuckDB takes Arrow (--reload also reads the files after_load moved to",
        expected_rows: "1:1.0,2:2.0",
    }];
    for test_case in &test_cases {
        let (plan, rows) = run_archived_reload(test_case)?;
        assert_eq!(rows, test_case.expected_rows, "{}", test_case.description);
        assert!(
            plan.contains(test_case.expected_plan),
            "{}:\n{plan}",
            test_case.description
        );
    }
    Ok(())
}
