use crate::bigquery_source::_helpers::query::count_query;
use crate::bigquery_source::tests::helpers::orders;
use crate::bigquery_source::tests::test_types::CountQueryTestCase;

#[test]
fn given_a_null_cursor_condition_when_counting_its_rows_then_the_table_or_query_is_read_as_its_rows_are()
 {
    let table = orders();
    let test_cases = [
        CountQueryTestCase {
            description: "a table is counted by its project, dataset and table in one pair of backticks",
            sql: None,
            filter: "`updated_at` IS NULL",
            expected_query: "SELECT COUNT(*) FROM `p.marts.orders` WHERE `updated_at` IS NULL",
        },
        CountQueryTestCase {
            description: "a query table is counted as a subquery, a coalesced cursor's NULL being every column's",
            sql: Some("SELECT * FROM marts.orders -- all\n;"),
            filter: "COALESCE(`updated_at`, `created_at`) IS NULL",
            expected_query: "SELECT COUNT(*) FROM (SELECT * FROM marts.orders -- all\n) WHERE COALESCE(`updated_at`, `created_at`) IS NULL",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            count_query("p", (&table, test_case.sql), test_case.filter),
            test_case.expected_query,
            "{}",
            test_case.description
        );
    }
}
