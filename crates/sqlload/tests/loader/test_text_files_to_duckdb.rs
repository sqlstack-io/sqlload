use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::helpers::{
    gzip, local_store, plain, run_async, run_text_files_scenario, seaweedfs_store, zstd,
};
use crate::test_types::{TextFile, TextFilesRun, TextFilesScenarioTestCase};

const EVENTS_NOV_3: TextFile = TextFile {
    key: "day=2026-11-03/part-0.ndjson.gz",
    contents: concat!(
        r#"{"id":1,"amount":1.5,"meta":{"tags":["a"],"n":1}}"#,
        "\n",
        r#"{"id":2,"amount":2,"meta":null}"#,
        "\n"
    ),
    encode: gzip,
};

/// A newer file adds `extra`.
const EVENTS_NOV_4: TextFile = TextFile {
    key: "day=2026-11-04/part-0.ndjson.gz",
    contents: concat!(
        r#"{"id":3,"amount":3.25,"meta":{"tags":["b"]},"extra":"x"}"#,
        "\n"
    ),
    encode: gzip,
};

const NDJSON_SETTINGS: &str = "format = \"ndjson\"\nwrite_strategy = \"append\"\npartition_columns = { day = \"date\" }\n[sources.scores.tables.scores]\ncolumns = { amount = \"decimal(10,2)\" }\n";

const NDJSON_RUNS: &[TextFilesRun] = &[
    TextFilesRun {
        files: &[EVENTS_NOV_3],
        expected_error: "",
    },
    TextFilesRun {
        files: &[EVENTS_NOV_4],
        expected_error: "",
    },
];

/// Rows, rows with `extra`, newest partition, the pinned decimal's sum, a value inside the nested JSON, and the JSON column's type.
const NDJSON_CHECK: &str = "SELECT count(*) || ',' || count(extra) || ',' || max(day) || ',' || sum(amount) || ',' || string_agg(json_extract_string(meta, '$.tags[0]'), '|' ORDER BY id) || ',' || (SELECT typeof(meta) FROM raw.scores LIMIT 1) FROM raw.scores";

const SCORES_NOV_3: TextFile = TextFile {
    key: "day=2026-11-03/part-0.csv.zst",
    contents: "id,name,seen_at\n1,a,2026-11-03T10:00:00Z\n2,\"b, c\",2026-11-03T11:00:00Z\n",
    encode: zstd,
};

/// A newer file reorders the columns and adds `score`.
const SCORES_NOV_4: TextFile = TextFile {
    key: "day=2026-11-04/part-0.csv.zst",
    contents: "seen_at,id,name,score\n2026-11-04T09:00:00+01:00,3,d,7\n",
    encode: zstd,
};

const CSV_SETTINGS: &str = "format = \"csv\"\nwrite_strategy = \"append\"\npartition_columns = { day = \"date\" }\n[sources.scores.tables.scores]\ncolumns = { id = \"text\" }\n";

const CSV_RUNS: &[TextFilesRun] = &[
    TextFilesRun {
        files: &[SCORES_NOV_3],
        expected_error: "",
    },
    TextFilesRun {
        files: &[SCORES_NOV_4],
        expected_error: "",
    },
];

/// Rows, names in id order (the quoted delimiter kept), the newest timestamp as epoch seconds, the added column's sum, newest partition, and the pinned column's type.
const CSV_CHECK: &str = "SELECT count(*) || ',' || string_agg(name, '|' ORDER BY id) || ',' || CAST(epoch(max(seen_at)) AS BIGINT) || ',' || sum(score) || ',' || max(day) || ',' || (SELECT typeof(id) FROM raw.scores LIMIT 1) FROM raw.scores";

/// `0` rows.
const NO_ROWS: &str = "SELECT CAST(count(*) AS VARCHAR) FROM raw.scores";

/// `0` while the table was never created.
const NO_TABLE: &str =
    "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'scores'";

#[test]
fn given_json_and_csv_files_when_loading_into_duckdb_then_rows_partitions_and_json_load()
-> Result<(), String> {
    let test_cases = [
        TextFilesScenarioTestCase {
            description: "gzip NDJSON from a directory: partitions, a pinned decimal, nested JSON, and a column added by a newer file",
            store: local_store,
            settings: NDJSON_SETTINGS,
            runs: NDJSON_RUNS,
            check_query: NDJSON_CHECK,
            expected_result: "3,1,2026-11-04,6.75,a|b,JSON",
        },
        TextFilesScenarioTestCase {
            description: "gzip NDJSON from S3 behaves the same",
            store: seaweedfs_store,
            settings: NDJSON_SETTINGS,
            runs: NDJSON_RUNS,
            check_query: NDJSON_CHECK,
            expected_result: "3,1,2026-11-04,6.75,a|b,JSON",
        },
        TextFilesScenarioTestCase {
            description: "zstd CSV from S3: partitions, a pinned text column, timestamps with offsets, and headers that change between files",
            store: seaweedfs_store,
            settings: CSV_SETTINGS,
            runs: CSV_RUNS,
            check_query: CSV_CHECK,
            expected_result: "3,a|b, c|d,1793779200,7,2026-11-04,VARCHAR",
        },
        TextFilesScenarioTestCase {
            description: "zstd CSV from a directory behaves the same",
            store: local_store,
            settings: CSV_SETTINGS,
            runs: CSV_RUNS,
            check_query: CSV_CHECK,
            expected_result: "3,a|b, c|d,1793779200,7,2026-11-04,VARCHAR",
        },
        TextFilesScenarioTestCase {
            description: "a malformed JSON line fails the plan with its file and line",
            store: seaweedfs_store,
            settings: "format = \"ndjson\"\nwrite_strategy = \"append\"\n",
            runs: &[TextFilesRun {
                files: &[TextFile {
                    key: "part-0.ndjson",
                    contents: "{\"id\":1}\n{\"id\":2,}\n",
                    encode: plain,
                }],
                expected_error: "part-0.ndjson: line 2: trailing comma",
            }],
            check_query: NO_TABLE,
            expected_result: "0",
        },
        TextFilesScenarioTestCase {
            description: "a CSV value that does not fit a pinned type fails the load with its line instead of loading NULL",
            store: local_store,
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\n[sources.scores.tables.scores]\ncolumns = { id = \"int\" }\n",
            runs: &[TextFilesRun {
                files: &[TextFile {
                    key: "part-0.csv.zst",
                    contents: "id,name\n1,a\nx,b\n",
                    encode: zstd,
                }],
                expected_error: "part-0.csv.zst: line 3: id: cannot convert",
            }],
            check_query: NO_ROWS,
            expected_result: "0",
        },
        TextFilesScenarioTestCase {
            description: "a source with no files of its format is an error, never an empty load",
            store: local_store,
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\n",
            runs: &[TextFilesRun {
                files: &[EVENTS_NOV_3],
                expected_error: "no .csv, .csv.gz, .csv.zst files found",
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
                std::env::temp_dir().join(format!("sqlload-text-files-{}", uuid::Uuid::new_v4()));
            let result = run_text_files_scenario(&server.endpoint, &directory, &test_case).await;
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
