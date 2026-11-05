use std::cmp::Ordering;

use crate::column_types::models::ColumnType;
use crate::run_log::main::cursor_order::cursor_order;
use crate::run_log::main::later_cursor::later_cursor;
use crate::run_log::main::next_stamp::next_stamp;
use crate::run_log::tests::helpers::{current_run_id, finished};
use crate::run_log::tests::test_types::{
    CurrentRunTestCase, CursorOrderTestCase, FinishedRecordTestCase, LaterCursorTestCase,
    NextStampTestCase,
};

#[test]
fn given_run_log_records_when_deriving_the_state_then_the_newest_non_backfill_record_holds_it() {
    let test_cases = [
        CurrentRunTestCase {
            description: "no records, no state",
            records: &[],
            raw: &[],
            expected_run: (None, &[]),
        },
        CurrentRunTestCase {
            description: "the newest record wins, whatever order the log returns them in",
            records: &[
                ("b", "run", "2026-11-04T10:00:02.000000Z"),
                ("a", "run", "2026-11-04T10:00:01.000000Z"),
            ],
            raw: &[],
            expected_run: (Some("b"), &[]),
        },
        CurrentRunTestCase {
            description: "records stamped alike order by run id, the same on every read",
            records: &[
                ("b", "run", "2026-11-04T10:00:01.000000Z"),
                ("a", "reset", "2026-11-04T10:00:01.000000Z"),
            ],
            raw: &[],
            expected_run: (Some("b"), &[]),
        },
        CurrentRunTestCase {
            description: "a backfill is logged but never moves the state",
            records: &[
                ("a", "run", "2026-11-04T10:00:01.000000Z"),
                ("b", "backfill", "2026-11-04T10:00:02.000000Z"),
            ],
            raw: &[],
            expected_run: (Some("a"), &[]),
        },
        CurrentRunTestCase {
            description: "a reset and a reconciliation are state like runs",
            records: &[
                ("a", "run", "2026-11-04T10:00:01.000000Z"),
                ("b", "reconcile", "2026-11-04T10:00:02.000000Z"),
                ("c", "reset", "2026-11-04T10:00:03.000000Z"),
            ],
            raw: &[],
            expected_run: (Some("c"), &[]),
        },
        CurrentRunTestCase {
            description: "a record that does not parse is set aside by run id, the others still read",
            records: &[("a", "run", "2026-11-04T10:00:01.000000Z")],
            raw: &["{\"run_id\": \"broken\"}", "not json"],
            expected_run: (Some("a"), &["broken: does not parse", "-: does not parse"]),
        },
        CurrentRunTestCase {
            description: "a record whose time is not RFC 3339 cannot be ordered, so it is set aside",
            records: &[
                ("a", "run", "2026-11-04T10:00:01.000000Z"),
                ("undated", "run", "zzzz"),
            ],
            raw: &[],
            expected_run: (
                Some("a"),
                &["undated: recorded_at \"zzzz\" is not an RFC 3339 time"],
            ),
        },
        CurrentRunTestCase {
            description: "a record from before runs committed in batches (format 1) still reads",
            records: &[("a", "run", "2026-11-04T10:00:01.000000Z")],
            raw: &[
                "{\"format_version\": 1, \"run_id\": \"old\", \"event\": \"run\", \"source\": \"shop\", \"source_table\": \"orders\", \"table_name\": \"shop__orders\", \"cursor_column\": null, \"cursor_before\": null, \"cursor_after\": null, \"state\": null, \"row_count\": 1, \"files\": [], \"note\": null, \"recorded_at\": \"2026-11-04T10:00:02.000000Z\"}",
            ],
            expected_run: (Some("old"), &[]),
        },
        CurrentRunTestCase {
            description: "a record a newer sqlload wrote is never guessed at",
            records: &[("a", "run", "2026-11-04T10:00:01.000000Z")],
            raw: &["{\"format_version\": 3, \"run_id\": \"future\", \"event\": \"run\"}"],
            expected_run: (
                Some("a"),
                &["future: written by a newer sqlload (format_version 3, this one reads 2)"],
            ),
        },
    ];
    for test_case in test_cases {
        let (current, unreadable) = current_run_id(&test_case);
        assert_eq!(
            (current.as_deref(), unreadable),
            (
                test_case.expected_run.0,
                test_case
                    .expected_run
                    .1
                    .iter()
                    .map(|text| (*text).to_owned())
                    .collect::<Vec<_>>()
            ),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_two_cursor_texts_when_ordering_them_then_they_compare_as_their_type() {
    let test_cases = [
        CursorOrderTestCase {
            description: "a float written two ways is one value",
            left: "2",
            right: "2.0",
            column_type: ColumnType::Float,
            expected_order: Ordering::Equal,
        },
        CursorOrderTestCase {
            description: "integers above 2^53 that differ by one stay apart",
            left: "9007199254740993",
            right: "9007199254740992",
            column_type: ColumnType::Int,
            expected_order: Ordering::Greater,
        },
        CursorOrderTestCase {
            description: "decimals compare exactly, trailing zeros aside",
            left: "12345678901234567890.10",
            right: "12345678901234567890.1",
            column_type: ColumnType::Decimal {
                precision: 38,
                scale: 2,
            },
            expected_order: Ordering::Equal,
        },
        CursorOrderTestCase {
            description: "decimals beyond f64 precision that differ stay apart",
            left: "12345678901234567890.01",
            right: "12345678901234567890.02",
            column_type: ColumnType::Decimal {
                precision: 38,
                scale: 2,
            },
            expected_order: Ordering::Less,
        },
        CursorOrderTestCase {
            description: "negative decimals order below positive ones",
            left: "-3.5",
            right: "0.1",
            column_type: ColumnType::Decimal {
                precision: 18,
                scale: 1,
            },
            expected_order: Ordering::Less,
        },
        CursorOrderTestCase {
            description: "timestamps compare as their fixed-width text",
            left: "2026-01-02 00:00:00.000000+00",
            right: "2026-01-02 00:00:00.000000+00",
            column_type: ColumnType::TimestampTz,
            expected_order: Ordering::Equal,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            cursor_order(test_case.left, test_case.right, &test_case.column_type),
            test_case.expected_order,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_log_stamped_ahead_of_the_clock_when_stamping_a_record_then_it_follows_the_newest() {
    let test_cases = [
        NextStampTestCase {
            description: "a clock ahead of the log keeps its time",
            now: "2026-11-04T10:00:02.000000Z",
            newest: Some("2026-11-04T10:00:01.000000Z"),
            expected_stamp: "2026-11-04T10:00:02.000000Z",
        },
        NextStampTestCase {
            description: "a clock behind the log stamps one microsecond after its newest record",
            now: "2026-11-04T09:00:00.000000Z",
            newest: Some("2026-11-04T10:00:01.999999Z"),
            expected_stamp: "2026-11-04T10:00:02.000000Z",
        },
        NextStampTestCase {
            description: "a clock equal to the newest record still moves past it",
            now: "2026-11-04T10:00:01.000000Z",
            newest: Some("2026-11-04T10:00:01.000000Z"),
            expected_stamp: "2026-11-04T10:00:01.000001Z",
        },
        NextStampTestCase {
            description: "an empty log keeps the clock's time",
            now: "2026-11-04T10:00:01.000000Z",
            newest: None,
            expected_stamp: "2026-11-04T10:00:01.000000Z",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            next_stamp(test_case.now, test_case.newest),
            test_case.expected_stamp,
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
            description: "integers above 2^53 keep the larger exactly",
            left: Some("9007199254740993"),
            right: Some("9007199254740992"),
            column_type: ColumnType::Int,
            expected_cursor: Some("9007199254740993"),
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
