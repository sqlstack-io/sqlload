use crate::batch_plan::tests::helpers::{owned_windows, windows};
use crate::batch_plan::tests::test_types::SplitWindowsTestCase;
use crate::column_types::models::ColumnType;
use crate::project::models::BatchStep;

const DAY: std::time::Duration = std::time::Duration::from_secs(86_400);

#[test]
fn given_a_cursor_range_when_splitting_by_batch_then_windows_cover_it_in_order() {
    let test_cases = [
        SplitWindowsTestCase {
            description: "date windows run to the end of today when no end is given",
            from: Some("2026-11-03"),
            to: None,
            inclusive: true,
            step: BatchStep::Duration(DAY),
            cursor_type: Some(ColumnType::Date),
            expected_windows: Ok(&[
                "2026-11-03..2026-11-04",
                "2026-11-04..2026-11-05",
                "2026-11-05..2026-11-06",
            ]),
        },
        SplitWindowsTestCase {
            description: "timestamp windows end now, the last one shorter",
            from: Some("2026-11-04 12:00:00.000000+00"),
            to: None,
            inclusive: true,
            step: BatchStep::Duration(DAY),
            cursor_type: Some(ColumnType::TimestampTz),
            expected_windows: Ok(&["2026-11-04 12:00:00.000000+00..2026-11-05 10:30:00.000000+00"]),
        },
        SplitWindowsTestCase {
            description: "an end cursor bounds the last window; the first keeps an exclusive start",
            from: Some("2026-11-01 00:00:00.000000"),
            to: Some("2026-11-02 12:00:00.000000"),
            inclusive: false,
            step: BatchStep::Duration(DAY),
            cursor_type: Some(ColumnType::TimestampNtz),
            expected_windows: Ok(&[
                "(2026-11-01 00:00:00.000000..2026-11-02 00:00:00.000000",
                "2026-11-02 00:00:00.000000..2026-11-02 12:00:00.000000",
            ]),
        },
        SplitWindowsTestCase {
            description: "integer windows step to the end cursor",
            from: Some("0"),
            to: Some("250"),
            inclusive: true,
            step: BatchStep::Integer(100),
            cursor_type: Some(ColumnType::Int),
            expected_windows: Ok(&["0..100", "100..200", "200..250"]),
        },
        SplitWindowsTestCase {
            description: "a Python table's unknown cursor type is read from the start's text",
            from: Some("2026-11-04"),
            to: Some("2026-11-06"),
            inclusive: true,
            step: BatchStep::Duration(DAY),
            cursor_type: None,
            expected_windows: Ok(&["2026-11-04..2026-11-05", "2026-11-05..2026-11-06"]),
        },
        SplitWindowsTestCase {
            description: "a range without a start cannot be split",
            from: None,
            to: None,
            inclusive: true,
            step: BatchStep::Duration(DAY),
            cursor_type: Some(ColumnType::Date),
            expected_windows: Err(
                "batch needs a start to split the cursor range: set cursor_start for the first load",
            ),
        },
        SplitWindowsTestCase {
            description: "an integer batch needs an end",
            from: Some("0"),
            to: None,
            inclusive: true,
            step: BatchStep::Integer(10),
            cursor_type: Some(ColumnType::Int),
            expected_windows: Err(
                "an integer batch needs --end-cursor-* to know where the last window ends",
            ),
        },
        SplitWindowsTestCase {
            description: "a date cursor's batch is whole days",
            from: Some("2026-11-04"),
            to: None,
            inclusive: true,
            step: BatchStep::Duration(std::time::Duration::from_secs(3600)),
            cursor_type: Some(ColumnType::Date),
            expected_windows: Err("a date cursor's batch must be whole days, not 1h"),
        },
        SplitWindowsTestCase {
            description: "a duration batch on an integer cursor is refused",
            from: Some("1"),
            to: None,
            inclusive: true,
            step: BatchStep::Duration(DAY),
            cursor_type: Some(ColumnType::Int),
            expected_windows: Err(
                "a duration batch needs a date or timestamp cursor, but the cursor is int; give an integer step",
            ),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            windows(&test_case),
            owned_windows(test_case.expected_windows),
            "{}",
            test_case.description
        );
    }
}
