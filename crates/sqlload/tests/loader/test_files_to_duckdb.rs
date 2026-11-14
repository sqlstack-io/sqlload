use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::files_helpers::run_source_row_upgrade;
use crate::helpers::{
    local_store, run_async, run_files_scenario, run_large_partition, run_merge_case,
    seaweedfs_store,
};
use crate::test_types::SourceRowUpgradeTestCase;
use crate::test_types::{FilesMergeTestCase, MergeFile, MergeStep};
use crate::test_types::{FilesRun, FilesScenarioTestCase, LargePartitionTestCase, ParquetFile};

/// Every row as `id:v`, then how many merged files the run log records.
pub(crate) const MERGE_CHECK: &str = "SELECT (SELECT string_agg(CAST(id AS VARCHAR) || ':' || v, ',' ORDER BY id) FROM raw.scores) || '|' || (SELECT CAST(count(*) AS VARCHAR) FROM raw._sqlload_runs WHERE table_name = 'scores' AND event = 'run')";

pub(crate) const MERGE_SETTINGS: &str = "write_strategy = \"merge\"\nunique_key = [\"id\"]\n";

pub(crate) const MERGE_FAIL_SETTINGS: &str =
    "write_strategy = \"merge\"\nunique_key = [\"id\"]\non_duplicate_keys = \"fail\"\n";

const UNORDERED_MERGE_SETTINGS: &str = "write_strategy = \"merge\"\nunique_key = [\"id\"]\nexclude_columns = [\"_sqlload_source_row\"]\n";

const UNORDERED_SORT_SETTINGS: &str = "write_strategy = \"merge\"\nunique_key = [\"id\"]\nexclude_columns = [\"_sqlload_source_row\"]\ndedup_sort = { v = \"asc\" }\n";

pub(crate) const MERGE_SORT_SETTINGS: &str =
    "write_strategy = \"merge\"\nunique_key = [\"id\"]\ndedup_sort = { v = \"asc\" }\n";

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
        expected_plan: "batch_size is ignored: a files source commits in batches of whole partitions",
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
    run_async(async {
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
    ];
    run_async(async {
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
    run_async(async {
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
                    loads: true,
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
                    loads: true,
                    expected_plan: "",
                }],
                check_query: MERGE_CHECK,
                expected_result: "1:b|2",
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
                    loads: true,
                    expected_error: "",
                    expected_plan: "merge (unique_key: id)",
                }],
                check_query: MERGE_CHECK,
                expected_result: "5:a,6:c|1",
            },
            FilesMergeTestCase {
                description: "with dedup_sort a file repeating a key merges the row that sorts first, and plan warns that a later file still overwrites it",
                settings: MERGE_SORT_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[
                        MergeFile {
                            key: "a.csv",
                            contents: "id,v\n5,b\n5,a\n5,c\n",
                            modified_secs: 1_000,
                        },
                        MergeFile {
                            key: "b.csv",
                            contents: "id,v\n6,z\n",
                            modified_secs: 2_000,
                        },
                    ],
                    loads: true,
                    expected_error: "",
                    expected_plan: "dedup_sort ranks repeated keys within one commit, and this table commits once per file (2 files)",
                }],
                check_query: MERGE_CHECK,
                expected_result: "5:a,6:z|2",
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
                        loads: true,
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
                        loads: true,
                        expected_error: "drop/b.csv: the file repeats its unique_key: 1 value of (id) appears in more than one row, such as (1)",
                        expected_plan: "",
                    },
                ],
                check_query: MERGE_CHECK,
                expected_result: "1:a,2:a|1",
            },
            FilesMergeTestCase {
                description: "dedup_sort without _sqlload_source_row is refused at plan, as ties need the file's row order",
                settings: UNORDERED_SORT_SETTINGS,
                steps: &[MergeStep {
                    before: &[],
                    files: &[MergeFile {
                        key: "a.csv",
                        contents: "id,v\n1,a\n",
                        modified_secs: 1_000,
                    }],
                    loads: true,
                    expected_error: "dedup_sort breaks ties by row order, which a files merge takes from _sqlload_source_row; stop excluding it",
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
                    loads: true,
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
                        loads: true,
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
                        loads: true,
                        expected_plan: "2 files, one commit each",
                    },
                    MergeStep {
                        before: &[],
                        files: &[],
                        expected_error: "",
                        loads: true,
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
                        loads: true,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &["DROP TABLE raw.scores"],
                        files: &[],
                        loads: false,
                        expected_error: "",
                        expected_plan: "",
                    },
                    MergeStep {
                        before: &[],
                        files: &[],
                        loads: true,
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
                        loads: true,
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
                        loads: true,
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
                        loads: true,
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
                        loads: true,
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
                        loads: true,
                        expected_plan: "1 file, one commit each",
                    },
                ],
                check_query: MERGE_CHECK,
                expected_result: "1:a2|3",
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
    run_async(async {
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
                loads: true,
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
