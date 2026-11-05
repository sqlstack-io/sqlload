use std::time::Duration;

use crate::adapters::contract::main::cursor_bound::cursor_bound;
use crate::adapters::contract::models::CursorValue;
use crate::adapters::contract::tests::test_types::CursorBoundTestCase;

const HOUR: Option<Duration> = Some(Duration::from_secs(3600));

#[test]
fn given_a_cursor_maximum_when_making_the_lower_bound_then_lookback_and_zone_are_applied() {
    let test_cases = [
        CursorBoundTestCase {
            description: "an instant prints in UTC with an offset",
            raw: "1793786523123456",
            value: CursorValue::EpochMicros { zoned: true },
            lookback: None,
            expected_bound: Ok("2026-11-04 10:02:03.123456+00"),
        },
        CursorBoundTestCase {
            description: "a wall-clock time has no offset and moves back by the lookback",
            raw: "1793786523000000",
            value: CursorValue::EpochMicros { zoned: false },
            lookback: HOUR,
            expected_bound: Ok("2026-11-04 09:02:03.000000"),
        },
        CursorBoundTestCase {
            description: "a date moves back by whole days, rounding the lookback up",
            raw: "2026-11-04",
            value: CursorValue::Date,
            lookback: HOUR,
            expected_bound: Ok("2026-11-03"),
        },
        CursorBoundTestCase {
            description: "plain values pass through",
            raw: "a\\b'c",
            value: CursorValue::Plain,
            lookback: None,
            expected_bound: Ok("a\\b'c"),
        },
        CursorBoundTestCase {
            description: "a malformed epoch is an error",
            raw: "2026-11-04",
            value: CursorValue::EpochMicros { zoned: true },
            lookback: None,
            expected_bound: Err("not a whole number of microseconds"),
        },
    ];
    for test_case in test_cases {
        let bound = cursor_bound(test_case.raw, test_case.value, test_case.lookback);
        assert_eq!(
            bound.as_deref().ok(),
            test_case.expected_bound.ok(),
            "{}",
            test_case.description
        );
        assert!(
            bound
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_bound.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
