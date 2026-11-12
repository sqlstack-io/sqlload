use crate::adapters::contract::main::cursor_text::cursor_text;
use crate::adapters::contract::tests::test_types::CursorTextTestCase;
use crate::column_types::models::ColumnType;

#[test]
fn given_a_cursor_flag_when_normalising_then_every_system_reads_the_same_bound() {
    let test_cases = [
        CursorTextTestCase {
            description: "a zoned cursor converts an offset to UTC",
            text: "2026-11-04T05:00:00+05:00",
            column_type: ColumnType::TimestampTz,
            expected_text: Ok("2026-11-04 00:00:00.000000+00"),
        },
        CursorTextTestCase {
            description: "a zoned cursor reads a date as UTC midnight",
            text: "2026-11-04",
            column_type: ColumnType::TimestampTz,
            expected_text: Ok("2026-11-04 00:00:00.000000+00"),
        },
        CursorTextTestCase {
            description: "a wall-clock cursor accepts Z",
            text: "2026-11-04T10:00:00Z",
            column_type: ColumnType::TimestampNtz,
            expected_text: Ok("2026-11-04 10:00:00.000000"),
        },
        CursorTextTestCase {
            description: "a wall-clock cursor rejects a non-zero offset",
            text: "2026-11-04T10:00:00+05:00",
            column_type: ColumnType::TimestampNtz,
            expected_text: Err("has a UTC offset"),
        },
        CursorTextTestCase {
            description: "a wall-clock cursor reads an ODBC {ts} literal with milliseconds",
            text: "{ts '2026-01-31 23:59:59.997'}",
            column_type: ColumnType::TimestampNtz,
            expected_text: Ok("2026-01-31 23:59:59.997000"),
        },
        CursorTextTestCase {
            description: "an ODBC literal reads in any letter case and spacing",
            text: " { TS'2026-01-31 23:59:59' } ",
            column_type: ColumnType::TimestampNtz,
            expected_text: Ok("2026-01-31 23:59:59.000000"),
        },
        CursorTextTestCase {
            description: "a date cursor reads an ODBC {d} literal",
            text: "{d '2026-02-01'}",
            column_type: ColumnType::Date,
            expected_text: Ok("2026-02-01"),
        },
        CursorTextTestCase {
            description: "a zoned cursor reads an ODBC {ts} literal as UTC",
            text: "{ts '2026-01-31 23:59:59.5'}",
            column_type: ColumnType::TimestampTz,
            expected_text: Ok("2026-01-31 23:59:59.500000+00"),
        },
        CursorTextTestCase {
            description: "an ODBC literal missing its closing quote is refused",
            text: "{ts '2026-01-31 23:59:59}",
            column_type: ColumnType::TimestampNtz,
            expected_text: Err("is not an RFC 3339 timestamp or a date"),
        },
        CursorTextTestCase {
            description: "a text cursor keeps a value that looks like an ODBC literal as it is",
            text: "{d 'abc'}",
            column_type: ColumnType::Text,
            expected_text: Ok("{d 'abc'}"),
        },
        CursorTextTestCase {
            description: "a date cursor needs a date",
            text: "2026-11-04T10:00:00Z",
            column_type: ColumnType::Date,
            expected_text: Err("is not a date"),
        },
        CursorTextTestCase {
            description: "an integer cursor passes through",
            text: "42",
            column_type: ColumnType::Int,
            expected_text: Ok("42"),
        },
        CursorTextTestCase {
            description: "an untyped --start-cursor that is not an integer fails for an integer cursor, never reaching SQL",
            text: "1; DROP TABLE t",
            column_type: ColumnType::Int,
            expected_text: Err("is not an integer"),
        },
        CursorTextTestCase {
            description: "a timestamp given to an integer cursor fails",
            text: "2026-11-04",
            column_type: ColumnType::SmallInt,
            expected_text: Err("is not an integer"),
        },
        CursorTextTestCase {
            description: "an integer given to a timestamp cursor fails",
            text: "50",
            column_type: ColumnType::TimestampTz,
            expected_text: Err("is not an RFC 3339 timestamp or a date"),
        },
        CursorTextTestCase {
            description: "a decimal cursor takes a number as written",
            text: "2.50",
            column_type: ColumnType::Decimal {
                precision: 4,
                scale: 2,
            },
            expected_text: Ok("2.50"),
        },
        CursorTextTestCase {
            description: "a date given to a decimal cursor fails at plan, not at read",
            text: "2026-11-04",
            column_type: ColumnType::Decimal {
                precision: 4,
                scale: 2,
            },
            expected_text: Err("is not a number, which a decimal(4,2) cursor needs"),
        },
        CursorTextTestCase {
            description: "a date given to a float cursor fails",
            text: "2026-11-04",
            column_type: ColumnType::Float,
            expected_text: Err("is not a number"),
        },
        CursorTextTestCase {
            description: "a float cursor takes exponent notation as written",
            text: "1e3",
            column_type: ColumnType::Float,
            expected_text: Ok("1e3"),
        },
        CursorTextTestCase {
            description: "a float cursor refuses NaN",
            text: "NaN",
            column_type: ColumnType::Float,
            expected_text: Err("is not a number"),
        },
        CursorTextTestCase {
            description: "a time cursor takes a time",
            text: "10:30:00",
            column_type: ColumnType::Time,
            expected_text: Ok("10:30:00"),
        },
        CursorTextTestCase {
            description: "a time cursor needs a time",
            text: "2026-11-04",
            column_type: ColumnType::Time,
            expected_text: Err("is not a time"),
        },
    ];
    for test_case in test_cases {
        let text = cursor_text(test_case.text, &test_case.column_type);
        assert_eq!(
            text.as_deref().ok(),
            test_case.expected_text.ok(),
            "{}",
            test_case.description
        );
        assert!(
            text.err()
                .unwrap_or_default()
                .contains(test_case.expected_text.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
