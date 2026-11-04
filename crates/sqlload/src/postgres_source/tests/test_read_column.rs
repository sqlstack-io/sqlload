use crate::column_types::models::ColumnType;
use crate::postgres_source::_helpers::type_map::read_column;
use crate::postgres_source::models::PgType;
use crate::postgres_source::tests::helpers::source_column;
use crate::postgres_source::tests::test_types::ReadColumnTestCase;

#[test]
fn given_a_source_type_and_output_when_planning_the_read_then_the_select_expression_fits() {
    let test_cases = [
        ReadColumnTestCase {
            description: "an int8 is read natively",
            pg_type: PgType::Int8,
            output: ColumnType::Int,
            expected_result: Ok("\"value\""),
        },
        ReadColumnTestCase {
            description: "json and jsonb are read through jsonb, which drops duplicate keys",
            pg_type: PgType::Json,
            output: ColumnType::Json,
            expected_result: Ok("\"value\"::jsonb::text"),
        },
        ReadColumnTestCase {
            description: "an array is read as JSON",
            pg_type: PgType::Array,
            output: ColumnType::Json,
            expected_result: Ok("to_jsonb(\"value\")::text"),
        },
        ReadColumnTestCase {
            description: "any type can be pinned to text",
            pg_type: PgType::Other,
            output: ColumnType::Text,
            expected_result: Ok("\"value\"::text"),
        },
        ReadColumnTestCase {
            description: "an int can be pinned to a decimal",
            pg_type: PgType::Int4,
            output: ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
            expected_result: Ok("\"value\"::numeric"),
        },
        ReadColumnTestCase {
            description: "a timestamptz pinned without a zone is converted to UTC",
            pg_type: PgType::TimestampTz,
            output: ColumnType::TimestampNtz,
            expected_result: Ok("(\"value\" AT TIME ZONE 'UTC')"),
        },
        ReadColumnTestCase {
            description: "a timestamp without a zone cannot become timestamp_tz",
            pg_type: PgType::Timestamp,
            output: ColumnType::TimestampTz,
            expected_result: Err("cannot be read as timestamp_tz"),
        },
        ReadColumnTestCase {
            description: "text cannot become an int",
            pg_type: PgType::Text,
            output: ColumnType::Int,
            expected_result: Err("cannot be read as int"),
        },
    ];
    for test_case in test_cases {
        let result = read_column(&source_column(test_case.pg_type), &test_case.output);
        assert_eq!(
            result.as_ref().ok().map(|read| read.expression.as_str()),
            test_case.expected_result.ok(),
            "{}",
            test_case.description
        );
        assert!(
            result
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_result.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
