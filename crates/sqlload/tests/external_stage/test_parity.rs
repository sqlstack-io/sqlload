use sqlload_testkit::parity::main::run_cases::run_cases;
use sqlload_testkit::parity::models::{ParityCase, ParityFiles, Side};

use crate::test_types::ParityTestCase;

const APPEND: &str = "write_strategy = \"append\"";
const LOADER: &str = "write_strategy = \"append\"\nshaping = \"loader\"";
const HASHED: &str =
    "write_strategy = \"append\"\n[sources.SOURCE.tables.SOURCE]\nmask = { note = \"hash\" }";
const HASHED_LOADER: &str = "write_strategy = \"append\"\nshaping = \"loader\"\n[sources.SOURCE.tables.SOURCE]\nmask = { note = \"hash\" }";
const JSON: &str =
    "write_strategy = \"append\"\n[sources.SOURCE.tables.SOURCE]\ncolumns = { note = \"json\" }";
const JSON_LOADER: &str = "write_strategy = \"append\"\nshaping = \"loader\"\n[sources.SOURCE.tables.SOURCE]\ncolumns = { note = \"json\" }";
const JSON_STAGING: &str = "write_strategy = \"replace\"\nshaping = \"loader\"\n[sources.SOURCE.tables.SOURCE]\ncolumns = { note = \"json\" }";
const BY_DAY: &str = "write_strategy = \"delete_insert\"\ncursor_column = \"day\"";
const BY_DAY_LOADER: &str =
    "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\nshaping = \"loader\"";
const REPLACE: &str = "write_strategy = \"replace\"";
const REPLACE_LOADER: &str = "write_strategy = \"replace\"\nshaping = \"loader\"";
const PARTITIONS: &str = "day = \"date\", n = \"int\", label = \"text\"";

const SAME: &str = "same rows";
const BOTH_FAIL: &str = "both fail";

const DIRECT: Side = Side {
    target: "direct",
    settings: APPEND,
};
const BASE: ParityCase = ParityCase {
    name: "",
    files: ParityFiles::EveryType,
    partition_columns: "",
    left: DIRECT,
    right: Side {
        settings: LOADER,
        ..DIRECT
    },
    left_is_direct: true,
};

#[test]
fn given_the_same_files_when_snowflake_and_sqlload_each_load_them_then_rows_and_failures_match()
-> Result<(), String> {
    let test_cases = [
        ParityTestCase {
            description: "every candidate type, with NULLs and edge values, and the source columns",
            case: ParityCase {
                name: "types",
                ..BASE
            },
            expected_outcome: SAME,
        },
        ParityTestCase {
            description: "typed partition values, source file, version and row number across files",
            case: ParityCase {
                name: "partitions",
                files: ParityFiles::Partitioned,
                partition_columns: PARTITIONS,
                ..BASE
            },
            expected_outcome: SAME,
        },
        ParityTestCase {
            description: "delete_insert by partition",
            case: ParityCase {
                name: "by_day",
                files: ParityFiles::Partitioned,
                partition_columns: PARTITIONS,
                left: Side {
                    settings: BY_DAY,
                    ..DIRECT
                },
                right: Side {
                    settings: BY_DAY_LOADER,
                    ..DIRECT
                },
                ..BASE
            },
            expected_outcome: SAME,
        },
        ParityTestCase {
            description: "replace",
            case: ParityCase {
                name: "replaced",
                files: ParityFiles::Partitioned,
                partition_columns: PARTITIONS,
                left: Side {
                    settings: REPLACE,
                    ..DIRECT
                },
                right: Side {
                    settings: REPLACE_LOADER,
                    ..DIRECT
                },
                ..BASE
            },
            expected_outcome: SAME,
        },
        ParityTestCase {
            description: "hash masking in a temporary table: byte-identical digests",
            case: ParityCase {
                name: "hashed",
                files: ParityFiles::Hashed,
                left: Side {
                    settings: HASHED,
                    ..DIRECT
                },
                right: Side {
                    settings: HASHED_LOADER,
                    ..DIRECT
                },
                ..BASE
            },
            expected_outcome: SAME,
        },
        ParityTestCase {
            description: "JSON of every shape, parsed by the direct COPY",
            case: ParityCase {
                name: "json_direct",
                files: ParityFiles::Json,
                left: Side {
                    settings: JSON,
                    ..DIRECT
                },
                right: Side {
                    settings: JSON_STAGING,
                    ..DIRECT
                },
                ..BASE
            },
            expected_outcome: SAME,
        },
        ParityTestCase {
            description: "decision 70: JSON parsed by the staged route's COPY against staging and PARSE_JSON",
            case: ParityCase {
                name: "json_staged",
                files: ParityFiles::Json,
                left: Side {
                    settings: JSON_LOADER,
                    ..DIRECT
                },
                right: Side {
                    settings: JSON_STAGING,
                    ..DIRECT
                },
                left_is_direct: false,
                ..BASE
            },
            expected_outcome: SAME,
        },
        ParityTestCase {
            description: "text that is not JSON fails both ways",
            case: ParityCase {
                name: "json_bad",
                files: ParityFiles::BadJson,
                left: Side {
                    settings: JSON,
                    ..DIRECT
                },
                right: Side {
                    settings: JSON_LOADER,
                    ..DIRECT
                },
                left_is_direct: false,
                ..BASE
            },
            expected_outcome: BOTH_FAIL,
        },
        ParityTestCase {
            description: "a partition value that is not a date fails both ways",
            case: ParityCase {
                name: "partition_bad",
                files: ParityFiles::BadPartition,
                partition_columns: "day = \"date\"",
                left_is_direct: false,
                ..BASE
            },
            expected_outcome: BOTH_FAIL,
        },
        ParityTestCase {
            description: "Snowpipe Streaming into a transient staging table gives the staged route's rows",
            case: ParityCase {
                name: "streaming",
                left: Side {
                    settings: LOADER,
                    ..DIRECT
                },
                right: Side {
                    target: "streaming",
                    settings: APPEND,
                },
                left_is_direct: false,
                ..BASE
            },
            expected_outcome: SAME,
        },
        ParityTestCase {
            description: "batched INSERT gives the staged route's rows",
            case: ParityCase {
                name: "inserted",
                left: Side {
                    settings: LOADER,
                    ..DIRECT
                },
                right: Side {
                    target: "insert",
                    settings: APPEND,
                },
                left_is_direct: false,
                ..BASE
            },
            expected_outcome: SAME,
        },
    ];
    let cases: Vec<ParityCase> = test_cases.iter().map(|test_case| test_case.case).collect();
    let outcomes = run_cases(&cases)?;
    for test_case in &test_cases {
        let outcome = outcomes
            .get(test_case.case.name)
            .map_or(test_case.expected_outcome, String::as_str);
        assert_eq!(
            outcome, test_case.expected_outcome,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
