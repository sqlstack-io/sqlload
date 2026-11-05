use crate::snowflake_destination::tests::helpers::copies;
use crate::snowflake_destination::tests::test_types::CopyStagedTestCase;

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
