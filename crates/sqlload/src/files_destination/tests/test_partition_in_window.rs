use crate::column_types::models::ColumnType;
use crate::files_destination::main::partition_in_window::partition_in_window;
use crate::files_destination::models::PartitionBy;
use crate::files_destination::tests::test_types::PartitionInWindowTestCase;

#[test]
fn given_a_partition_when_checking_the_window_then_from_is_inclusive_and_to_exclusive() {
    let test_cases = [
        PartitionInWindowTestCase {
            description: "a day at the start of a timestamp window is inside",
            value: "2026-11-03",
            by: PartitionBy::Date,
            cursor_type: ColumnType::TimestampTz,
            bounds: (Some("2026-11-03 00:00:00.000000+00"), None),
            expected_inside: Ok(true),
        },
        PartitionInWindowTestCase {
            description: "the day a window ends at is outside",
            value: "2026-11-05",
            by: PartitionBy::Date,
            cursor_type: ColumnType::TimestampTz,
            bounds: (None, Some("2026-11-05 00:00:00.000000+00")),
            expected_inside: Ok(false),
        },
        PartitionInWindowTestCase {
            description: "a day before the window is outside",
            value: "2026-11-02",
            by: PartitionBy::Date,
            cursor_type: ColumnType::Date,
            bounds: (Some("2026-11-03"), None),
            expected_inside: Ok(false),
        },
        PartitionInWindowTestCase {
            description: "int partitions compare as numbers",
            value: "10",
            by: PartitionBy::Value,
            cursor_type: ColumnType::Int,
            bounds: (Some("9"), Some("11")),
            expected_inside: Ok(true),
        },
        PartitionInWindowTestCase {
            description: "an open window holds every partition",
            value: "anything",
            by: PartitionBy::Value,
            cursor_type: ColumnType::Text,
            bounds: (None, None),
            expected_inside: Ok(true),
        },
        PartitionInWindowTestCase {
            description: "a value that does not parse is an error",
            value: "ten",
            by: PartitionBy::Value,
            cursor_type: ColumnType::Int,
            bounds: (Some("9"), None),
            expected_inside: Err("is not an integer"),
        },
    ];
    for test_case in test_cases {
        let inside = partition_in_window(
            test_case.value,
            test_case.by,
            &test_case.cursor_type,
            test_case.bounds,
        );
        assert_eq!(
            inside.as_ref().ok(),
            test_case.expected_inside.as_ref().ok(),
            "{}",
            test_case.description
        );
        assert!(
            inside
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_inside.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
