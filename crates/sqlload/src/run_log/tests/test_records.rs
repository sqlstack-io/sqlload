use crate::column_types::models::ColumnType;
use crate::run_log::main::later_cursor::later_cursor;
use crate::run_log::tests::helpers::{current_run_id, finished};
use crate::run_log::tests::test_types::{
    CurrentRunTestCase, FinishedRecordTestCase, LaterCursorTestCase,
};

#[test]
fn given_run_log_records_when_deriving_the_state_then_the_newest_non_backfill_record_holds_it() {
    let test_cases = [
        CurrentRunTestCase {
            description: "no records, no state",
            records: &[],
            raw: &[],
            expected_run: Ok(None),
        },
        CurrentRunTestCase {
            description: "the newest record wins, whatever order the log returns them in",
            records: &[
                ("b", "run", "2026-11-04T10:00:02.000000Z"),
                ("a", "run", "2026-11-04T10:00:01.000000Z"),
            ],
            raw: &[],
            expected_run: Ok(Some("b")),
        },
        CurrentRunTestCase {
            description: "a backfill is logged but never moves the state",
            records: &[
                ("a", "run", "2026-11-04T10:00:01.000000Z"),
                ("b", "backfill", "2026-11-04T10:00:02.000000Z"),
            ],
            raw: &[],
            expected_run: Ok(Some("a")),
        },
        CurrentRunTestCase {
            description: "a reset and a reconciliation are state like runs",
            records: &[
                ("a", "run", "2026-11-04T10:00:01.000000Z"),
                ("b", "reconcile", "2026-11-04T10:00:02.000000Z"),
                ("c", "reset", "2026-11-04T10:00:03.000000Z"),
            ],
            raw: &[],
            expected_run: Ok(Some("c")),
        },
        CurrentRunTestCase {
            description: "a record that does not parse is an error, never skipped",
            records: &[("a", "run", "2026-11-04T10:00:01.000000Z")],
            raw: &["{\"run_id\": \"broken\"}"],
            expected_run: Err("a run log record does not parse"),
        },
    ];
    for test_case in test_cases {
        let current = current_run_id(&test_case);
        assert_eq!(
            current.as_ref().ok().cloned(),
            test_case
                .expected_run
                .ok()
                .map(|run| run.map(str::to_owned)),
            "{}",
            test_case.description
        );
        assert!(
            current
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_run.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_two_cursor_positions_when_comparing_then_the_later_one_is_kept() {
    let test_cases = [
        LaterCursorTestCase {
            description: "integers compare as numbers, not text",
            left: Some("9"),
            right: Some("10"),
            column_type: ColumnType::Int,
            expected_cursor: Some("10"),
        },
        LaterCursorTestCase {
            description: "decimals compare as numbers",
            left: Some("12.50"),
            right: Some("3.00"),
            column_type: ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
            expected_cursor: Some("12.50"),
        },
        LaterCursorTestCase {
            description: "timestamps compare as their fixed-width text",
            left: Some("2026-01-02 00:00:00.000000+00"),
            right: Some("2026-01-01 23:59:59.999999+00"),
            column_type: ColumnType::TimestampTz,
            expected_cursor: Some("2026-01-02 00:00:00.000000+00"),
        },
        LaterCursorTestCase {
            description: "a missing side takes the other",
            left: None,
            right: Some("2026-01-01"),
            column_type: ColumnType::Date,
            expected_cursor: Some("2026-01-01"),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            later_cursor(
                test_case.left.map(str::to_owned),
                test_case.right.map(str::to_owned),
                &test_case.column_type
            )
            .as_deref(),
            test_case.expected_cursor,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_finished_run_when_completing_its_record_then_the_position_is_the_datas_maximum() {
    let test_cases = [
        FinishedRecordTestCase {
            description: "rows below the data's maximum keep it",
            before: Some("2026-01-05"),
            loaded: Some("2026-01-03"),
            cursor_type: Some(ColumnType::Date),
            expected_record: (Some("2026-01-05"), "20261105T010203000004Z-load-2.json"),
        },
        FinishedRecordTestCase {
            description: "newer rows move it",
            before: Some("2026-01-05"),
            loaded: Some("2026-01-07"),
            cursor_type: Some(ColumnType::Date),
            expected_record: (Some("2026-01-07"), "20261105T010203000004Z-load-2.json"),
        },
        FinishedRecordTestCase {
            description: "a table without a cursor logs no cursor",
            before: None,
            loaded: None,
            cursor_type: None,
            expected_record: (None, "20261105T010203000004Z-load-2.json"),
        },
    ];
    for test_case in test_cases {
        let (cursor, name) = finished(&test_case);
        assert_eq!(
            (cursor.as_deref(), name.as_str()),
            test_case.expected_record,
            "{}",
            test_case.description
        );
    }
}
