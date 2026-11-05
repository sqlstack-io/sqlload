use crate::column_types::models::ColumnType;
use crate::adapters::snowflake::tests::helpers::{copies, selecting_copy};
use crate::adapters::snowflake::tests::test_types::{CopyStagedTestCase, SelectingCopyTestCase};

const ONE_FILE: &str = "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\" FROM @~/sqlload/load-1/00001/ FILES = ('part-00001.parquet') FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = ABORT_STATEMENT FORCE = TRUE";

#[test]
fn given_a_batchs_staged_files_when_copying_them_then_one_copy_names_up_to_1000_files_with_force() {
    let test_cases = [
        CopyStagedTestCase {
            description: "one file, by column name, always FORCE, stopping at the first bad value",
            files: 1,
            expected_copies: (1, ONE_FILE),
        },
        CopyStagedTestCase {
            description: "1,000 files are one COPY",
            files: 1000,
            expected_copies: (
                1,
                "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\" FROM @~/sqlload/load-1/00001/ FILES = ('part-00001.parquet', 'part-00002.parquet'",
            ),
        },
        CopyStagedTestCase {
            description: "1,001 files take two",
            files: 1001,
            expected_copies: (2, "COPY INTO"),
        },
    ];
    for test_case in test_cases {
        let (count, first) = copies(test_case.files);
        assert_eq!(
            count, test_case.expected_copies.0,
            "{}",
            test_case.description
        );
        assert!(
            first.starts_with(test_case.expected_copies.1),
            "{}: {first}",
            test_case.description
        );
    }
}

#[test]
fn given_a_table_holding_json_when_copying_staged_files_then_the_copy_parses_it() {
    let test_cases = [SelectingCopyTestCase {
        description: "JSON the files hold as text is parsed; other columns are cast; every option stays",
        columns: vec![("id", ColumnType::Int), ("profile", ColumnType::Json)],
        expected_copy: "COPY INTO \"RAW\".\"ORDERS\" (\"ID\", \"PROFILE\") FROM (SELECT CAST(GET($1, 'id') AS NUMBER(38,0)), PARSE_JSON(CAST(GET($1, 'profile') AS VARCHAR)) FROM @\"RAW\".%\"ORDERS\"/sqlload/l/00001/) FILES = ('part-00001.parquet') FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE) ON_ERROR = ABORT_STATEMENT FORCE = TRUE",
    }];
    for test_case in test_cases {
        assert_eq!(
            selecting_copy(&test_case.columns),
            test_case.expected_copy,
            "{}",
            test_case.description
        );
    }
}
