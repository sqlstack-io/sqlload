use std::time::Duration;

use crate::delivery::_helpers::stage_paths::expired_dates;
use crate::delivery::tests::test_types::ExpiredDatesTestCase;

const MARCH_10_NOON: i64 = 1_710_072_000_000;

#[test]
fn given_a_retention_when_listing_expired_folders_then_only_older_dates_are_returned() {
    let test_cases = [
        ExpiredDatesTestCase {
            description: "seven days keeps a week and removes the days before it",
            retain_days: 7,
            lookback_days: 3,
            expected_dates: &["2024-03-02", "2024-03-01", "2024-02-29"],
        },
        ExpiredDatesTestCase {
            description: "a zero retention still keeps today and yesterday",
            retain_days: 0,
            lookback_days: 1,
            expected_dates: &["2024-03-08"],
        },
    ];
    for test_case in &test_cases {
        let dates = expired_dates(
            MARCH_10_NOON,
            Duration::from_secs(test_case.retain_days * 86_400),
            Duration::from_secs(test_case.lookback_days * 86_400),
        );
        assert_eq!(dates, test_case.expected_dates, "{}", test_case.description);
    }
}
