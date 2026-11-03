use std::time::Duration;

use crate::config::main::parse_config::parse_config;
use crate::config::main::stream_settings::stream_settings;
use crate::config::tests::helpers::{BASE, lookup};
use crate::config::tests::test_types::StreamSettingsTestCase;

const OVERRIDES: &str = r#"
[staging]
kind = "none"

[batching]
max_rows = 1000
max_age = "60s"

[[topics]]
match = "orders.*"
max_rows = 10

[[topics]]
match = "orders.returns"
max_age = "5s"
"#;

#[test]
fn given_topic_overrides_when_resolving_settings_then_later_matches_win() -> Result<(), String> {
    let test_cases = [
        StreamSettingsTestCase {
            description: "a topic matching one entry takes its row limit",
            topic: "orders.created",
            expected_max_rows: 10,
            expected_max_age: Duration::from_secs(60),
        },
        StreamSettingsTestCase {
            description: "a topic matching two entries takes both, in order",
            topic: "orders.returns",
            expected_max_rows: 10,
            expected_max_age: Duration::from_secs(5),
        },
        StreamSettingsTestCase {
            description: "a topic matching nothing keeps the global values",
            topic: "inventory.levels",
            expected_max_rows: 1000,
            expected_max_age: Duration::from_secs(60),
        },
    ];
    let config =
        parse_config(&format!("{BASE}{OVERRIDES}"), &lookup).map_err(|error| error.to_string())?;
    for test_case in &test_cases {
        let settings = stream_settings(&config, test_case.topic);
        assert_eq!(
            settings.max_rows, test_case.expected_max_rows,
            "{}",
            test_case.description
        );
        assert_eq!(
            settings.max_age, test_case.expected_max_age,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
