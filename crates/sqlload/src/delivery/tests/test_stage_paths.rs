use std::time::Duration;

use crate::delivery::_helpers::stage_paths::{FolderValues, expired_dates, render_folder};
use crate::delivery::tests::test_types::{ExpiredDatesTestCase, RenderFolderTestCase};

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

#[test]
fn given_a_path_template_when_rendering_then_placeholders_become_utc_folders() {
    let test_cases = [
        RenderFolderTestCase {
            description: "the default layout is one Hive date folder per topic",
            template: "{topic}/date={date}/",
            expected_folder: "source.shop.orders/date=2024-03-10/",
        },
        RenderFolderTestCase {
            description: "hourly partitions under the table, with a trailing slash added",
            template: "/{table}/date={date}/hour={hour}",
            expected_folder: "SHOP__ORDERS/date=2024-03-10/hour=12/",
        },
        RenderFolderTestCase {
            description: "the stream name is the lower-case table",
            template: "{stream}/{date}/",
            expected_folder: "shop__orders/2024-03-10/",
        },
    ];
    let values = FolderValues {
        topic: "source.shop.orders",
        table: "SHOP__ORDERS",
        stream: "shop__orders",
        now_ms: MARCH_10_NOON,
    };
    for test_case in &test_cases {
        assert_eq!(
            render_folder(test_case.template, &values),
            test_case.expected_folder,
            "{}",
            test_case.description
        );
    }
}
