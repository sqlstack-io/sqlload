use crate::bigquery_source::_helpers::query::select_query;
use crate::bigquery_source::models::BigQueryReadRequest;
use crate::bigquery_source::tests::helpers::orders;
use crate::bigquery_source::tests::test_types::SelectQueryTestCase;

#[test]
fn given_a_query_table_when_wrapping_it_with_a_cursor_filter_then_the_filter_stays_outside_the_query()
 {
    let table = orders();
    let test_cases = [
        SelectQueryTestCase {
            description: "a final semicolon is dropped and a trailing comment ends before the parenthesis",
            sql: "  SELECT 1 AS id -- latest only\n;\n",
            expected_query: "SELECT  FROM (SELECT 1 AS id -- latest only\n) WHERE `id` > 3",
        },
        SelectQueryTestCase {
            description: "a plain query is wrapped as it is",
            sql: "SELECT id FROM marts.orders",
            expected_query: "SELECT  FROM (SELECT id FROM marts.orders\n) WHERE `id` > 3",
        },
    ];
    for test_case in test_cases {
        let request = BigQueryReadRequest {
            table: &table,
            sql: Some(test_case.sql),
            columns: &[],
            filter: Some("`id` > 3"),
        };
        assert_eq!(
            select_query("p", &request),
            test_case.expected_query,
            "{}",
            test_case.description
        );
    }
}
