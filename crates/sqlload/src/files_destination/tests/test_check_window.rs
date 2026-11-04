use crate::column_types::models::ColumnType;
use crate::files_destination::main::check_window::check_window;
use crate::files_destination::models::PartitionBy;
use crate::files_destination::tests::test_types::CheckWindowTestCase;

#[test]
fn given_cursor_bounds_when_checking_then_bounds_that_cannot_meet_the_partitions_are_named() {
    let test_cases = [
        CheckWindowTestCase {
            description: "timestamp bounds compare by their date",
            by: PartitionBy::Date,
            cursor_type: ColumnType::TimestampTz,
            bounds: (
                Some("2026-11-03 00:00:00.000000+00"),
                Some("2026-11-05 00:00:00.000000+00"),
            ),
            expected_problem: "",
        },
        CheckWindowTestCase {
            description: "a cursor_start that is no date is caught at plan",
            by: PartitionBy::Value,
            cursor_type: ColumnType::Date,
            bounds: (Some("soon"), None),
            expected_problem: "cursor bound \"soon\": \"soon\" is not a date",
        },
        CheckWindowTestCase {
            description: "an end that is no integer is caught at plan",
            by: PartitionBy::Value,
            cursor_type: ColumnType::Int,
            bounds: (None, Some("ten")),
            expected_problem: "is not an integer",
        },
    ];
    for test_case in test_cases {
        let problem = check_window(test_case.by, &test_case.cursor_type, test_case.bounds)
            .err()
            .unwrap_or_default();
        assert!(
            problem.contains(test_case.expected_problem)
                && problem.is_empty() == test_case.expected_problem.is_empty(),
            "{}: {problem}",
            test_case.description
        );
    }
}
