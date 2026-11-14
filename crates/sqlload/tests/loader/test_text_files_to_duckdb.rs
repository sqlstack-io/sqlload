use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::helpers::{
    after_sample, gzip, local_store, plain, run_async, run_text_files_scenario, seaweedfs_store,
    zstd,
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
        expected_plan: "",
    },
    TextFilesRun {
        files: &[EVENTS_NOV_4],
        expected_error: "",
        expected_plan: "",
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
        expected_plan: "",
    },
    TextFilesRun {
        files: &[SCORES_NOV_4],
        expected_error: "",
        expected_plan: "",
    },
];

/// Rows, names in id order (the quoted delimiter kept), the newest timestamp as epoch seconds, the added column's sum, newest partition, and the pinned column's type.
const CSV_CHECK: &str = "SELECT count(*) || ',' || string_agg(name, '|' ORDER BY id) || ',' || CAST(epoch(max(seen_at)) AS BIGINT) || ',' || sum(score) || ',' || max(day) || ',' || (SELECT typeof(id) FROM raw.scores LIMIT 1) FROM raw.scores";

/// The table's columns, each record in load order, and the record column's type.
const RECORD_CHECK: &str = "SELECT (SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 'scores') || '|' || string_agg(day || ' ' || CAST(record AS VARCHAR), '|' ORDER BY day, _sqlload_source_row) || '|' || (SELECT typeof(record) FROM raw.scores LIMIT 1) FROM raw.scores";

/// The table's columns and each line in load order.
const LINE_CHECK: &str = "SELECT (SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_name = 'scores') || '|' || string_agg(CAST(line AS VARCHAR), '|' ORDER BY day, _sqlload_source_row) FROM raw.scores";

/// A key the 10,000 sampled lines never have.
const LATE_KEY: TextFile = TextFile {
    key: "part-0.ndjson.gz",
    contents: "{\"id\":10000,\"coupon_code\":\"x\"}\n",
    encode: after_sample,
};

/// One record with an `email` key.
const EMAIL: TextFile = TextFile {
    key: "part-0.ndjson",
    contents: "{\"id\":1,\"email\":\"a@example.com\"}\n",
    encode: plain,
};

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
            description: "record shape, gzip NDJSON: each object whole in one JSON column beside its partition, a key added by a newer file kept without a new column",
            store: local_store,
            settings: "format = \"ndjson\"\nshape = \"record\"\nwrite_strategy = \"append\"\npartition_columns = { day = \"date\" }\n",
            runs: NDJSON_RUNS,
            check_query: RECORD_CHECK,
            expected_result: "record,day,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_sqlload_load_id,_sqlload_loaded_at|2026-11-03 {\"id\":1,\"amount\":1.5,\"meta\":{\"tags\":[\"a\"],\"n\":1}}|2026-11-03 {\"id\":2,\"amount\":2,\"meta\":null}|2026-11-04 {\"id\":3,\"amount\":3.25,\"meta\":{\"tags\":[\"b\"]},\"extra\":\"x\"}|JSON",
        },
        TextFilesScenarioTestCase {
            description: "record shape, zstd CSV from S3: each line an array of its fields as text, headers skipped unchecked",
            store: seaweedfs_store,
            settings: "format = \"csv\"\nshape = \"record\"\nrecord_column = \"line\"\nwrite_strategy = \"append\"\npartition_columns = { day = \"date\" }\n",
            runs: CSV_RUNS,
            check_query: LINE_CHECK,
            expected_result: "line,day,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_sqlload_load_id,_sqlload_loaded_at|[\"1\",\"a\",\"2026-11-03T10:00:00Z\"]|[\"2\",\"b, c\",\"2026-11-03T11:00:00Z\"]|[\"2026-11-04T09:00:00+01:00\",\"3\",\"d\",\"7\"]",
        },
        TextFilesScenarioTestCase {
            description: "a malformed JSON line fails the plan with its file and line",
            store: seaweedfs_store,
            settings: "format = \"ndjson\"\nwrite_strategy = \"append\"\n",
            runs: &[TextFilesRun {
                expected_plan: "",
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
                expected_plan: "",
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
                expected_plan: "",
                files: &[EVENTS_NOV_3],
                expected_error: "no .csv, .csv.gz, .csv.zst files found",
            }],
            check_query: NO_TABLE,
            expected_result: "0",
        },
        TextFilesScenarioTestCase {
            description: "a key first seen after the schema sample fails the load with its line",
            store: seaweedfs_store,
            settings: "format = \"ndjson\"\nwrite_strategy = \"append\"\n",
            runs: &[TextFilesRun {
                files: &[LATE_KEY],
                expected_error: "part-0.ndjson.gz: line 10001: key coupon_code is not in the sampled schema; pin it in columns or add it to exclude_columns",
                expected_plan: "",
            }],
            check_query: NO_ROWS,
            expected_result: "0",
        },
        TextFilesScenarioTestCase {
            description: "a key outside the sample that exclude_columns matches is dropped",
            store: local_store,
            settings: "format = \"ndjson\"\nwrite_strategy = \"append\"\nexclude_columns = [\"coupon_code\"]\n",
            runs: &[TextFilesRun {
                files: &[LATE_KEY],
                expected_error: "",
                expected_plan: "",
            }],
            check_query: NO_ROWS,
            expected_result: "10001",
        },
        TextFilesScenarioTestCase {
            description: "a key outside the sample can be pinned",
            store: local_store,
            settings: "format = \"ndjson\"\nwrite_strategy = \"append\"\n[sources.scores.tables.scores]\ncolumns = { coupon_code = \"text\" }\n",
            runs: &[TextFilesRun {
                files: &[LATE_KEY],
                expected_error: "",
                expected_plan: "note    pinned column coupon_code was not seen in the sampled files (check the name)",
            }],
            check_query: "SELECT count(*) || ',' || count(coupon_code) || ',' || max(coupon_code) FROM raw.scores",
            expected_result: "10001,1,x",
        },
        TextFilesScenarioTestCase {
            description: "plan notes what the destination's type loses",
            store: local_store,
            settings: "format = \"ndjson\"\nwrite_strategy = \"append\"\n[sources.scores.tables.scores]\ncolumns = { coupon_code = \"varchar(8)\" }\n",
            runs: &[TextFilesRun {
                files: &[LATE_KEY],
                expected_error: "",
                expected_plan: "-> VARCHAR  (the length limit is not enforced)",
            }],
            check_query: "SELECT count(*) || ',' || count(coupon_code) || ',' || max(coupon_code) FROM raw.scores",
            expected_result: "10001,1,x",
        },
        TextFilesScenarioTestCase {
            description: "a key outside the sample that is pinned and masked loads masked",
            store: local_store,
            settings: "format = \"ndjson\"\nwrite_strategy = \"append\"\n[sources.scores.tables.scores]\ncolumns = { coupon_code = \"text\" }\nmask = { coupon_code = \"hash\" }\n",
            runs: &[TextFilesRun {
                files: &[LATE_KEY],
                expected_error: "",
                expected_plan: "note    pinned column coupon_code was not seen in the sampled files",
            }],
            check_query: "SELECT count(coupon_code) || ',' || (max(coupon_code) = sha256('x')) FROM raw.scores",
            expected_result: "1,true",
        },
        TextFilesScenarioTestCase {
            description: "a pinned and masked typo of a sampled key fails the plan instead of loading the real key unmasked",
            store: local_store,
            settings: "format = \"ndjson\"\nwrite_strategy = \"append\"\n[sources.scores.tables.scores]\ncolumns = { emial = \"text\" }\nmask = { emial = \"hash\" }\n",
            runs: &[TextFilesRun {
                files: &[EMAIL],
                expected_error: "pinned column emial was not seen in the sampled files; did you mean \"email\"? (pin a name exactly as it appears in the files)",
                expected_plan: "",
            }],
            check_query: NO_TABLE,
            expected_result: "0",
        },
        TextFilesScenarioTestCase {
            description: "a pinned name differing from a sampled key only in case fails the plan",
            store: local_store,
            settings: "format = \"ndjson\"\nwrite_strategy = \"append\"\n[sources.scores.tables.scores]\ncolumns = { Email = \"text\" }\n",
            runs: &[TextFilesRun {
                files: &[EMAIL],
                expected_error: "pinned column Email was not seen in the sampled files; did you mean \"email\"?",
                expected_plan: "",
            }],
            check_query: NO_TABLE,
            expected_result: "0",
        },
        TextFilesScenarioTestCase {
            description: "a misspelt mask on a JSON source fails the plan with the nearest name, never adding a column",
            store: local_store,
            settings: "format = \"ndjson\"\nwrite_strategy = \"append\"\n[sources.scores.tables.scores]\nmask = { emial = \"hash\" }\n",
            runs: &[TextFilesRun {
                files: &[TextFile {
                    key: "part-0.ndjson",
                    contents: "{\"id\":1,\"email\":\"a@example.com\"}\n",
                    encode: plain,
                }],
                expected_error: "mask \"emial\" is not a column of the source table (names are case-sensitive); did you mean \"email\"?",
                expected_plan: "",
            }],
            check_query: NO_TABLE,
            expected_result: "0",
        },
        TextFilesScenarioTestCase {
            description: "a value outside the sample that does not fit the inferred type fails with its line",
            store: seaweedfs_store,
            settings: "format = \"ndjson\"\nwrite_strategy = \"append\"\n",
            runs: &[TextFilesRun {
                files: &[TextFile {
                    key: "part-0.ndjson.gz",
                    contents: "{\"id\":\"10000\"}\n",
                    encode: after_sample,
                }],
                expected_error: "part-0.ndjson.gz: line 10001: id: \"10000\" does not fit int, the type inferred from sampled values",
                expected_plan: "",
            }],
            check_query: NO_ROWS,
            expected_result: "0",
        },
        TextFilesScenarioTestCase {
            description: "a CSV quoted field the file never closes fails instead of loading one merged row",
            store: seaweedfs_store,
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\n",
            runs: &[TextFilesRun {
                files: &[TextFile {
                    key: "part-0.csv",
                    contents: "a,b\n1,\"abc\n2,x\n3,y\n",
                    encode: plain,
                }],
                expected_error: "part-0.csv: line 2: unterminated quoted field",
                expected_plan: "",
            }],
            check_query: NO_TABLE,
            expected_result: "0",
        },
        TextFilesScenarioTestCase {
            description: "append on a partition cursor reads from the newest partition, so a new file in it loads, and plan says from",
            store: local_store,
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\ncursor_column = \"day\"\npartition_columns = { day = \"date\" }\n",
            runs: &[
                TextFilesRun {
                    files: &[TextFile {
                        key: "day=2026-11-03/a.csv",
                        contents: "id\n1\n",
                        encode: plain,
                    }],
                    expected_error: "",
                    expected_plan: "",
                },
                TextFilesRun {
                    files: &[TextFile {
                        key: "day=2026-11-03/b.csv",
                        contents: "id\n2\n",
                        encode: plain,
                    }],
                    expected_error: "",
                    expected_plan: "reads   day >= 2026-11-03 (warehouse maximum)",
                },
            ],
            check_query: "SELECT string_agg(id || ':' || day, ',' ORDER BY id) FROM raw.scores",
            expected_result: "1:2026-11-03,2:2026-11-03",
        },
        TextFilesScenarioTestCase {
            description: "a headerless CSV of one record without a final line break loads it",
            store: local_store,
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\ncsv = { header = false }\n",
            runs: &[TextFilesRun {
                files: &[TextFile {
                    key: "part-0.csv",
                    contents: "1,2",
                    encode: plain,
                }],
                expected_error: "",
                expected_plan: "",
            }],
            check_query: "SELECT string_agg(column_1 || ',' || column_2, ';') FROM raw.scores",
            expected_result: "1,2",
        },
        TextFilesScenarioTestCase {
            description: "int in one file and float in another load as float; the plan counts files of other formats",
            store: seaweedfs_store,
            settings: "format = \"ndjson\"\nwrite_strategy = \"append\"\n",
            runs: &[TextFilesRun {
                files: &[
                    TextFile {
                        key: "a.ndjson",
                        contents: "{\"v\":1}\n",
                        encode: plain,
                    },
                    TextFile {
                        key: "b.ndjson",
                        contents: "{\"v\":1.5}\n",
                        encode: plain,
                    },
                    TextFile {
                        key: "notes.txt",
                        contents: "not data",
                        encode: plain,
                    },
                ],
                expected_error: "",
                expected_plan: "files   2 new of 2 listed, 1 skipped (names that do not fit ndjson",
            }],
            check_query: "SELECT sum(v) || ',' || (SELECT typeof(v) FROM raw.scores LIMIT 1) FROM raw.scores",
            expected_result: "2.5,DOUBLE",
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
