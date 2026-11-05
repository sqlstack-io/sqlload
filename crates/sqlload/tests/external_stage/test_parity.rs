use sqlload_testkit::parity::constants::SELF_TEST_PASSED;
use sqlload_testkit::parity::main::comparison_sql::comparison_sql;
use sqlload_testkit::parity::models::{ComparedColumn, ParityCase, ParityFiles, Side};

use crate::helpers::all_outcomes;
use crate::test_types::{ComparisonTestCase, ParityTestCase};

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
            description: "the comparison itself: rows differing only at microseconds are reported, identical ones are not",
            case: ParityCase {
                name: "self_test",
                files: ParityFiles::ComparisonSelfTest,
                left_is_direct: false,
                ..BASE
            },
            expected_outcome: SELF_TEST_PASSED,
        },
        ParityTestCase {
            description: "NaN and both infinities, in both float widths",
            case: ParityCase {
                name: "floats",
                files: ParityFiles::FloatEdges,
                ..BASE
            },
            expected_outcome: SAME,
        },
        ParityTestCase {
            description: "each timestamp unit with sub-millisecond digits, before and after 1970",
            case: ParityCase {
                name: "timestamps",
                files: ParityFiles::TimestampEdges,
                ..BASE
            },
            expected_outcome: SAME,
        },
        ParityTestCase {
            description: "decimals at the edges of precision and scale",
            case: ParityCase {
                name: "decimals",
                files: ParityFiles::DecimalEdges,
                ..BASE
            },
            expected_outcome: SAME,
        },
        ParityTestCase {
            description: "the minimum and maximum of each integer width",
            case: ParityCase {
                name: "integers",
                files: ParityFiles::IntegerEdges,
                ..BASE
            },
            expected_outcome: SAME,
        },
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
    let outcomes = all_outcomes(&test_cases)?;
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

#[test]
fn given_two_tables_when_comparing_them_then_values_compare_typed_and_only_variants_as_text() {
    let columns = [
        ComparedColumn {
            name: "AT",
            data_type: "TIMESTAMP_NTZ",
        },
        ComparedColumn {
            name: "V",
            data_type: "VARIANT",
        },
    ];
    let test_cases = [ComparisonTestCase {
        description: "a timestamp goes as itself, a VARIANT as text, and row counts count",
        tables: ("L", "R"),
        expected_counts: "SELECT (SELECT COUNT(*) FROM (SELECT \"AT\", TO_VARCHAR(\"V\") AS \"V\" FROM L MINUS SELECT \"AT\", TO_VARCHAR(\"V\") AS \"V\" FROM R)) || ' ' || (SELECT COUNT(*) FROM (SELECT \"AT\", TO_VARCHAR(\"V\") AS \"V\" FROM R MINUS SELECT \"AT\", TO_VARCHAR(\"V\") AS \"V\" FROM L)) || ' ' || (SELECT COUNT(*) FROM L) || ' ' || (SELECT COUNT(*) FROM R)",
    }];
    for test_case in test_cases {
        assert_eq!(
            comparison_sql(test_case.tables, &columns).counts,
            test_case.expected_counts,
            "{}",
            test_case.description
        );
    }
}
