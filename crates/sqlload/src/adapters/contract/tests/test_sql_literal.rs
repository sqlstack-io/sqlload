use crate::adapters::contract::tests::test_types::SqlLiteralTestCase;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;

#[test]
fn given_text_when_quoting_a_literal_then_the_warehouse_reads_it_back_unchanged() {
    let test_cases = [
        SqlLiteralTestCase {
            description: "Snowflake doubles backslashes and quotes",
            text: "a\\'; DROP",
            warehouse: Warehouse::Snowflake,
            expected_literal: "'a\\\\''; DROP'",
        },
        SqlLiteralTestCase {
            description: "DuckDB keeps backslashes and doubles quotes",
            text: "a\\'b",
            warehouse: Warehouse::DuckDb,
            expected_literal: "'a\\''b'",
        },
        SqlLiteralTestCase {
            description: "Databricks escapes backslashes and quotes with a backslash, since '' joins two literals",
            text: "a\\'b",
            warehouse: Warehouse::Databricks,
            expected_literal: "'a\\\\\\'b'",
        },
        SqlLiteralTestCase {
            description: "BigQuery escapes backslashes and quotes with a backslash, having no '' escape",
            text: "a\\'b",
            warehouse: Warehouse::BigQuery,
            expected_literal: "'a\\\\\\'b'",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            sql_adapter(test_case.warehouse).render_loader_value_literal(test_case.text),
            test_case.expected_literal,
            "{}",
            test_case.description
        );
    }
}
