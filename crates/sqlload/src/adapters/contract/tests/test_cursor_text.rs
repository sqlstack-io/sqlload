use crate::column_types::models::ColumnType;
use crate::adapters::contract::main::cursor_text::cursor_text;
use crate::adapters::contract::tests::test_types::CursorTextTestCase;

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
