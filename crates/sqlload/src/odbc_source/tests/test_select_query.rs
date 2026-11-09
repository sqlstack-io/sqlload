use crate::odbc_source::_helpers::query::select_query;
use crate::odbc_source::main::read_columns::read_columns;
use crate::odbc_source::models::OdbcReadRequest;
use crate::odbc_source::tests::helpers::table;
use crate::odbc_source::tests::test_types::SelectQueryTestCase;

#[test]
fn given_an_odbc_read_when_writing_its_select_then_names_are_quoted_and_queries_aliased() {
    let test_cases = [
        SelectQueryTestCase {
            description: "a table in a schema, a quote in a name doubled, and the filter after WHERE",
            table: table("PUB", "order line", &["id", "say \"hi\""]),
            sql: None,
            filter: Some("\"id\" >= '5'"),
            expected_query: "SELECT \"id\", \"say \"\"hi\"\"\" FROM \"PUB\".\"order line\" WHERE \"id\" >= '5'",
        },
        SelectQueryTestCase {
            description: "a driver without schemas names the table alone",
            table: table("", "items", &["id"]),
            sql: None,
            filter: None,
            expected_query: "SELECT \"id\" FROM \"items\"",
        },
        SelectQueryTestCase {
            description: "a query table: its final semicolon dropped, closed on its own line after a trailing comment, under an alias",
            table: table("", "recent", &["id"]),
            sql: Some("SELECT id FROM t -- newest;\n"),
            filter: Some("\"id\" > '1'"),
            expected_query: "SELECT \"id\" FROM (SELECT id FROM t -- newest\n) sqlload_query WHERE \"id\" > '1'",
        },
    ];
    for test_case in test_cases {
        let columns = read_columns(&test_case.table, &Default::default()).unwrap_or_default();
        let request = OdbcReadRequest {
            table: &test_case.table,
            sql: test_case.sql,
            columns: &columns,
            filter: test_case.filter,
            parameters: &[],
            batch_rows: 10,
        };
        assert_eq!(
            select_query(&request),
            test_case.expected_query,
            "{}",
            test_case.description
        );
    }
}
