use std::time::Duration;

use chrono::{TimeZone, Utc};

use crate::adapters::contract::main::cursor_from_now::cursor_from_now;
use crate::adapters::contract::models::NowShift;
use crate::adapters::contract::tests::test_types::CursorFromNowTestCase;
use crate::column_types::models::ColumnType;

const DAY: u64 = 86_400;

#[test]
fn given_a_window_counted_from_now_when_rendering_its_bounds_then_the_last_day_is_read()
-> Result<(), String> {
    let test_cases = [
        CursorFromNowTestCase {
            description: "a date window starts whole days back",
            column_type: ColumnType::Date,
            shift: NowShift::Back(Duration::from_secs(7 * DAY)),
            expected_text: Ok("2026-11-01"),
        },
        CursorFromNowTestCase {
            description: "a date window's end is the day after the last day, since the end bound is exclusive",
            column_type: ColumnType::Date,
            shift: NowShift::Ahead(Duration::from_secs(7 * DAY)),
            expected_text: Ok("2026-11-16"),
        },
        CursorFromNowTestCase {
            description: "part of a day counts as a whole day back",
            column_type: ColumnType::Date,
            shift: NowShift::Back(Duration::from_secs(DAY + 1)),
            expected_text: Ok("2026-11-06"),
        },
        CursorFromNowTestCase {
            description: "a zero lookahead still reads today",
            column_type: ColumnType::Date,
            shift: NowShift::Ahead(Duration::ZERO),
            expected_text: Ok("2026-11-09"),
        },
        CursorFromNowTestCase {
            description: "a zoned cursor is the instant in UTC",
            column_type: ColumnType::TimestampTz,
            shift: NowShift::Back(Duration::from_secs(3_600)),
            expected_text: Ok("2026-11-08 22:30:00.000000+00"),
        },
        CursorFromNowTestCase {
            description: "a wall-clock cursor has no offset",
            column_type: ColumnType::TimestampNtz,
            shift: NowShift::Ahead(Duration::from_secs(7_200)),
            expected_text: Ok("2026-11-09 01:30:00.000000"),
        },
        CursorFromNowTestCase {
            description: "an integer cursor has no time to count from",
            column_type: ColumnType::Int,
            shift: NowShift::Ahead(Duration::ZERO),
            expected_text: Err(
                "lookahead and lookback_from = \"now\" count from the time of the run, so they need a date or timestamp cursor, not int",
            ),
        },
    ];
    let now = Utc
        .with_ymd_and_hms(2026, 11, 8, 23, 30, 0)
        .single()
        .ok_or("not a time")?;
    for test_case in test_cases {
        assert_eq!(
            cursor_from_now(now, &test_case.column_type, test_case.shift),
            test_case
                .expected_text
                .map(str::to_owned)
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
