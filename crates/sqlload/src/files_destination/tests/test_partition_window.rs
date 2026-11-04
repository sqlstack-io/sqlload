use std::time::Duration;

use crate::column_types::models::ColumnType;
use crate::files_destination::main::partition_window::partition_window;
use crate::files_destination::models::{PartitionBy, PartitionWindowRequest};
use crate::files_destination::tests::test_types::PartitionWindowTestCase;

const HOUR: Option<Duration> = Some(Duration::from_secs(3600));
const DAY: Option<Duration> = Some(Duration::from_secs(86_400));

#[test]
fn given_existing_partitions_when_choosing_the_window_then_it_covers_whole_partitions() {
    let test_cases = [
        PartitionWindowTestCase {
            description: "a timestamp cursor by date starts at the newest partition's midnight",
            by: PartitionBy::Date,
            cursor_type: ColumnType::TimestampTz,
            newest: Some("2026-11-04"),
            lookback: None,
            flags: (None, None),
            cursor_start: None,
            reload: false,
            expected_from: Ok(Some("2026-11-04 00:00:00.000000+00")),
            expected_to: None,
            expected_note: "updated_at from partition 2026-11-04 (the newest)",
        },
        PartitionWindowTestCase {
            description: "a lookback into the previous day rewrites that whole day",
            by: PartitionBy::Date,
            cursor_type: ColumnType::TimestampTz,
            newest: Some("2026-11-04"),
            lookback: HOUR,
            flags: (None, None),
            cursor_start: None,
            reload: false,
            expected_from: Ok(Some("2026-11-03 00:00:00.000000+00")),
            expected_to: None,
            expected_note: "minus lookback 1h",
        },
        PartitionWindowTestCase {
            description: "a wall-clock cursor has no offset",
            by: PartitionBy::Date,
            cursor_type: ColumnType::TimestampNtz,
            newest: Some("2026-11-04"),
            lookback: None,
            flags: (None, None),
            cursor_start: None,
            reload: false,
            expected_from: Ok(Some("2026-11-04 00:00:00.000000")),
            expected_to: None,
            expected_note: "rewrites whole partitions from 2026-11-04 00:00:00.000000",
        },
        PartitionWindowTestCase {
            description: "cursor flags inside a day widen to the whole days they touch",
            by: PartitionBy::Date,
            cursor_type: ColumnType::TimestampTz,
            newest: Some("2026-11-04"),
            lookback: None,
            flags: (
                Some("2026-11-02 10:00:00.000000+00"),
                Some("2026-11-03 06:00:00.000000+00"),
            ),
            cursor_start: None,
            reload: false,
            expected_from: Ok(Some("2026-11-02 00:00:00.000000+00")),
            expected_to: Some("2026-11-04 00:00:00.000000+00"),
            expected_note: "(--start-cursor)",
        },
        PartitionWindowTestCase {
            description: "an end at midnight already ends a partition",
            by: PartitionBy::Date,
            cursor_type: ColumnType::TimestampTz,
            newest: None,
            lookback: None,
            flags: (None, Some("2026-11-03 00:00:00.000000+00")),
            cursor_start: None,
            reload: false,
            expected_from: Ok(None),
            expected_to: Some("2026-11-03 00:00:00.000000+00"),
            expected_note: "first run, everything",
        },
        PartitionWindowTestCase {
            description: "a date cursor moves back whole days",
            by: PartitionBy::Value,
            cursor_type: ColumnType::Date,
            newest: Some("2026-11-04"),
            lookback: DAY,
            flags: (None, None),
            cursor_start: None,
            reload: false,
            expected_from: Ok(Some("2026-11-03")),
            expected_to: None,
            expected_note: "minus lookback 1day",
        },
        PartitionWindowTestCase {
            description: "an int cursor starts at the newest partition's value",
            by: PartitionBy::Value,
            cursor_type: ColumnType::Int,
            newest: Some("42"),
            lookback: None,
            flags: (None, None),
            cursor_start: None,
            reload: false,
            expected_from: Ok(Some("42")),
            expected_to: None,
            expected_note: "from partition 42",
        },
        PartitionWindowTestCase {
            description: "an int cursor takes no lookback",
            by: PartitionBy::Value,
            cursor_type: ColumnType::Int,
            newest: Some("42"),
            lookback: HOUR,
            flags: (None, None),
            cursor_start: None,
            reload: false,
            expected_from: Err("lookback needs a timestamp or date cursor"),
            expected_to: None,
            expected_note: "",
        },
        PartitionWindowTestCase {
            description: "a reload starts at cursor_start",
            by: PartitionBy::Date,
            cursor_type: ColumnType::Date,
            newest: None,
            lookback: None,
            flags: (None, None),
            cursor_start: Some("2026-01-01"),
            reload: true,
            expected_from: Ok(Some("2026-01-01")),
            expected_to: None,
            expected_note: "reload, from cursor_start 2026-01-01",
        },
    ];
    for test_case in test_cases {
        let request = PartitionWindowRequest {
            by: test_case.by,
            cursor_column: "updated_at",
            cursor_type: &test_case.cursor_type,
            newest: test_case.newest,
            lookback: test_case.lookback,
            start: test_case.flags.0,
            end: test_case.flags.1,
            cursor_start: test_case.cursor_start,
            reload: test_case.reload,
        };
        let window = partition_window(&request);
        let from = window.as_ref().map(|window| window.from.as_deref());
        let to = window.as_ref().ok().and_then(|window| window.to.as_deref());
        let note = window
            .as_ref()
            .map(|window| window.note.clone())
            .unwrap_or_default();
        assert_eq!(
            from.as_ref().ok(),
            test_case.expected_from.as_ref().ok(),
            "{}",
            test_case.description
        );
        assert!(
            from.err()
                .cloned()
                .unwrap_or_default()
                .contains(test_case.expected_from.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
        assert_eq!(to, test_case.expected_to, "{}", test_case.description);
        assert!(
            note.contains(test_case.expected_note),
            "{}: {note}",
            test_case.description
        );
    }
}
