use crate::snowflake_source::_helpers::unload::rows_unloaded;
use crate::snowflake_source::tests::test_types::RowsUnloadedTestCase;
use crate::sql_api::models::SqlRows;

#[test]
fn given_a_copy_into_result_when_counting_then_a_missing_count_fails_instead_of_reading_nothing() {
    let test_cases = [
        RowsUnloadedTestCase {
            description: "the count is read by name, whatever the column order",
            columns: &["input_bytes", "rows_unloaded", "output_bytes"],
            row: &[Some("10"), Some("1200"), Some("9")],
            expected_rows: Ok(1200),
        },
        RowsUnloadedTestCase {
            description: "a result without the count is an error, not zero rows",
            columns: &["status"],
            row: &[Some("Copy executed with 0 files processed.")],
            expected_rows: Err("COPY INTO reported no rows_unloaded"),
        },
        RowsUnloadedTestCase {
            description: "a count that is not a number is an error",
            columns: &["rows_unloaded"],
            row: &[Some("many")],
            expected_rows: Err(
                "COPY INTO reported rows_unloaded \"many\": invalid digit found in string",
            ),
        },
    ];
    for test_case in test_cases {
        let result = SqlRows {
            columns: test_case
                .columns
                .iter()
                .map(|column| (*column).to_owned())
                .collect(),
            rows: vec![
                test_case
                    .row
                    .iter()
                    .map(|value| value.map(str::to_owned))
                    .collect(),
            ],
        };
        assert_eq!(
            rows_unloaded(&result).map_err(|error| error.to_string()),
            test_case.expected_rows.map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
