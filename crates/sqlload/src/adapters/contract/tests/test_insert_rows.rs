use crate::adapters::contract::tests::helpers::inserted;
use crate::adapters::contract::tests::test_types::InsertRowsTestCase;

const START: &str = "INSERT INTO \"RAW\".\"APP__ACCOUNTS\" (\"ID\", \"NOTE\", \"GONE\") SELECT CAST(column1 AS NUMBER(38,0)), CAST(column2 AS VARCHAR), CAST(column3 AS VARCHAR) FROM VALUES ('1', 'xx', NULL)";

#[test]
fn given_rows_when_inserting_them_then_statements_cast_every_value_and_stay_within_snowflakes_limits()
 {
    let test_cases = [
        InsertRowsTestCase {
            description: "values are cast, JSON staying text in the staging table, NULL staying NULL",
            rows: 1,
            value_bytes: 2,
            expected_statements: 1,
            expected_start: START,
        },
        InsertRowsTestCase {
            description: "16,384 rows fit one VALUES",
            rows: 16_384,
            value_bytes: 2,
            expected_statements: 1,
            expected_start: "INSERT INTO",
        },
        InsertRowsTestCase {
            description: "one more takes another",
            rows: 16_385,
            value_bytes: 2,
            expected_statements: 2,
            expected_start: "INSERT INTO",
        },
        InsertRowsTestCase {
            description: "about 900 KiB of SQL closes a statement before the row count does",
            rows: 3,
            value_bytes: 500 * 1024,
            expected_statements: 3,
            expected_start: "INSERT INTO",
        },
        InsertRowsTestCase {
            description: "no rows, no statements",
            rows: 0,
            value_bytes: 2,
            expected_statements: 0,
            expected_start: "",
        },
    ];
    for test_case in test_cases {
        let statements = inserted(test_case.rows, test_case.value_bytes);
        assert_eq!(
            statements.len(),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
        assert!(
            statements
                .first()
                .map_or("", String::as_str)
                .starts_with(test_case.expected_start),
            "{}: {:?}",
            test_case.description,
            statements
                .first()
                .map(|first| first.chars().take(240).collect::<String>())
        );
    }
}
