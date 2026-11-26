use crate::adapters::postgres::_helpers::write_checks::{bound, text_array};
use crate::adapters::postgres::tests::test_types::{BoundTestCase, TextArrayTestCase};

#[test]
fn given_a_query_with_placeholders_when_bound_for_a_callers_session_then_each_takes_its_value_once()
{
    let test_cases = [
        BoundTestCase {
            description: "each placeholder takes its value, wherever it repeats",
            sql: "WHERE n = $1 AND c = $2 OR n2 = $1",
            values: &["'raw'", "'orders'"],
            expected: Ok("WHERE n = 'raw' AND c = 'orders' OR n2 = 'raw'"),
        },
        BoundTestCase {
            description: "a value holding a placeholder's text is not filled again",
            sql: "WHERE n = $1 AND c = $2",
            values: &["'$2'", "'orders'"],
            expected: Ok("WHERE n = '$2' AND c = 'orders'"),
        },
        BoundTestCase {
            description: "a dollar sign not before a digit stays",
            sql: "SELECT $sqlload$x$sqlload$, $1",
            values: &["1"],
            expected: Ok("SELECT $sqlload$x$sqlload$, 1"),
        },
        BoundTestCase {
            description: "a placeholder without a value fails",
            sql: "WHERE n = $3",
            values: &["'raw'"],
            expected: Err("$3 has no value"),
        },
    ];
    for test_case in test_cases {
        let values: Vec<String> = test_case
            .values
            .iter()
            .map(|value| (*value).to_owned())
            .collect();
        assert_eq!(
            bound(test_case.sql, &values),
            test_case.expected.map(str::to_owned).map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_postgres_array_as_text_when_read_then_its_elements_come_apart() {
    let test_cases = [
        TextArrayTestCase {
            description: "an empty array has no elements",
            text: "{}",
            expected_elements: &[],
        },
        TextArrayTestCase {
            description: "plain names split at commas",
            text: "{customer_id,order_id}",
            expected_elements: &["customer_id", "order_id"],
        },
        TextArrayTestCase {
            description: "quoted names keep their commas, spaces and escaped quotes",
            text: "{\"a,b\",\"c d\",\"e\\\"f\"}",
            expected_elements: &["a,b", "c d", "e\"f"],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            text_array(test_case.text),
            Ok(test_case
                .expected_elements
                .iter()
                .map(|element| (*element).to_owned())
                .collect()),
            "{}",
            test_case.description
        );
    }
}
