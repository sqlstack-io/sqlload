use crate::column_types::models::Warehouse;
use crate::adapters::contract::main::sql_literal::sql_literal;
use crate::adapters::contract::tests::test_types::SqlLiteralTestCase;

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
    ];
    for test_case in test_cases {
        assert_eq!(
            sql_literal(test_case.text, test_case.warehouse),
            test_case.expected_literal,
            "{}",
            test_case.description
        );
    }
}
