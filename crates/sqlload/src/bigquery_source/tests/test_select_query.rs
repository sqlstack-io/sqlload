use crate::bigquery_source::_helpers::query::select_query;
use crate::bigquery_source::models::BigQueryReadRequest;
use crate::bigquery_source::tests::helpers::orders;
use crate::bigquery_source::tests::test_types::SelectQueryTestCase;

#[test]
fn given_a_table_or_query_when_selecting_it_with_a_cursor_filter_then_the_filter_stays_outside_and_an_instant_follows_the_table()
 {
    let table = orders();
    let test_cases = [
        SelectQueryTestCase {
            description: "a final semicolon is dropped and a trailing comment ends before the parenthesis",
            sql: Some("  SELECT 1 AS id -- latest only\n;\n"),
            as_of: None,
            expected_query: "SELECT  FROM (SELECT 1 AS id -- latest only\n) WHERE `id` > 3",
        },
        SelectQueryTestCase {
            description: "a plain query is wrapped as it is",
            sql: Some("SELECT id FROM marts.orders"),
            as_of: None,
            expected_query: "SELECT  FROM (SELECT id FROM marts.orders\n) WHERE `id` > 3",
        },
        SelectQueryTestCase {
            description: "a table read as of an instant names it right after the table, before the filter",
            sql: None,
            as_of: Some(1_763_500_000_000),
            expected_query: "SELECT  FROM `p.marts.orders` FOR SYSTEM_TIME AS OF TIMESTAMP_MILLIS(1763500000000) WHERE `id` > 3",
        },
        SelectQueryTestCase {
            description: "a table read as it is now names no instant",
            sql: None,
            as_of: None,
            expected_query: "SELECT  FROM `p.marts.orders` WHERE `id` > 3",
        },
    ];
    for test_case in test_cases {
        let request = BigQueryReadRequest {
            table: &table,
            sql: test_case.sql,
            columns: &[],
            filter: Some("`id` > 3"),
            filtered: false,
        };
        assert_eq!(
            select_query("p", &request, test_case.as_of),
            test_case.expected_query,
            "{}",
            test_case.description
        );
    }
}
