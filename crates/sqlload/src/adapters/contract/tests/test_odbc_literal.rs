use crate::adapters::contract::main::odbc_literal::odbc_literal;
use crate::adapters::contract::tests::test_types::OdbcLiteralTestCase;
use crate::column_types::models::ColumnType;

#[test]
fn given_a_cursor_bound_when_writing_it_for_an_odbc_source_then_dates_and_wall_clock_times_are_escape_literals()
 {
    let test_cases = [
        OdbcLiteralTestCase {
            description: "a millisecond bound keeps exactly three fractional digits",
            value: "2026-01-31 23:59:59.997000",
            column_type: ColumnType::TimestampNtz,
            expected_literal: Some("{ts '2026-01-31 23:59:59.997'}"),
        },
        OdbcLiteralTestCase {
            description: "half a second is written as milliseconds",
            value: "2026-01-31 23:59:59.500000",
            column_type: ColumnType::TimestampNtz,
            expected_literal: Some("{ts '2026-01-31 23:59:59.500'}"),
        },
        OdbcLiteralTestCase {
            description: "a microsecond bound keeps all six digits",
            value: "2026-01-31 23:59:59.000001",
            column_type: ColumnType::TimestampNtz,
            expected_literal: Some("{ts '2026-01-31 23:59:59.000001'}"),
        },
        OdbcLiteralTestCase {
            description: "a whole second has no fraction",
            value: "2026-02-01 00:00:00.000000",
            column_type: ColumnType::TimestampNtz,
            expected_literal: Some("{ts '2026-02-01 00:00:00'}"),
        },
        OdbcLiteralTestCase {
            description: "a bound written with a T reads the same",
            value: "2026-02-01T08:30:00.25",
            column_type: ColumnType::TimestampNtz,
            expected_literal: Some("{ts '2026-02-01 08:30:00.250'}"),
        },
        OdbcLiteralTestCase {
            description: "a date is a {d} literal",
            value: "2026-02-01",
            column_type: ColumnType::Date,
            expected_literal: Some("{d '2026-02-01'}"),
        },
        OdbcLiteralTestCase {
            description: "a zoned timestamp has no escape, since {ts} carries no offset",
            value: "2026-01-31 23:59:59.997000+00",
            column_type: ColumnType::TimestampTz,
            expected_literal: None,
        },
        OdbcLiteralTestCase {
            description: "a value that is not a timestamp is never put inside an escape",
            value: "2026-01-31'} OR 1=1 --",
            column_type: ColumnType::TimestampNtz,
            expected_literal: None,
        },
        OdbcLiteralTestCase {
            description: "a date cursor given a timestamp has no escape",
            value: "2026-02-01 00:00:00",
            column_type: ColumnType::Date,
            expected_literal: None,
        },
        OdbcLiteralTestCase {
            description: "an integer cursor has no escape",
            value: "42",
            column_type: ColumnType::Int,
            expected_literal: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            odbc_literal(test_case.value, &test_case.column_type).as_deref(),
            test_case.expected_literal,
            "{}",
            test_case.description
        );
    }
}
