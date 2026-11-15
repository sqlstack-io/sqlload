use crate::snowflake_source::_helpers::unload_query::count_query;
use crate::snowflake_source::tests::helpers::events;
use crate::snowflake_source::tests::test_types::CountQueryTestCase;

#[test]
fn given_a_null_cursor_condition_when_counting_its_rows_then_the_table_or_query_is_read_as_its_unload_reads_it()
 {
    let table = events();
    let test_cases = [
        CountQueryTestCase {
            description: "a table is counted by its quoted schema and name",
            sql: None,
            filter: "\"SEEN_AT\" IS NULL",
            expected_query: "SELECT COUNT(*) FROM \"MARTS\".\"EVENTS\" WHERE \"SEEN_AT\" IS NULL",
        },
        CountQueryTestCase {
            description: "a query table is counted as a subquery, a coalesced cursor's NULL being every column's",
            sql: Some("SELECT * FROM MARTS.EVENTS -- all\n;"),
            filter: "COALESCE(\"SEEN_AT\", \"CREATED_AT\") IS NULL",
            expected_query: "SELECT COUNT(*) FROM (SELECT * FROM MARTS.EVENTS -- all\n) WHERE COALESCE(\"SEEN_AT\", \"CREATED_AT\") IS NULL",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            count_query(&table, test_case.sql, test_case.filter),
            test_case.expected_query,
            "{}",
            test_case.description
        );
    }
}
