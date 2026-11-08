use std::time::Duration;

use crate::http_source::_helpers::oauth::renewal_delay;
use crate::http_source::tests::test_types::RenewalTestCase;

#[test]
fn given_a_token_responses_expires_in_when_planning_its_renewal_then_it_renews_early_and_never_overflows()
 {
    let year = 365 * 24 * 60 * 60;
    let test_cases = [
        RenewalTestCase {
            description: "an hour renews a minute early",
            expires_in: "3600",
            expected_delay: Ok(Some(Duration::from_secs(3540))),
        },
        RenewalTestCase {
            description: "a short lifetime renews a tenth early",
            expires_in: "2",
            expected_delay: Ok(Some(Duration::from_millis(1800))),
        },
        RenewalTestCase {
            description: "seconds as text, as some servers send them",
            expires_in: "\"3599\"",
            expected_delay: Ok(Some(Duration::from_secs(3539))),
        },
        RenewalTestCase {
            description: "Long.MAX_VALUE is capped at a year",
            expires_in: "9223372036854775807",
            expected_delay: Ok(Some(Duration::from_secs(year - 60))),
        },
        RenewalTestCase {
            description: "past Duration's range is capped at a year",
            expires_in: "1e20",
            expected_delay: Ok(Some(Duration::from_secs(year - 60))),
        },
        RenewalTestCase {
            description: "zero means no lifetime given",
            expires_in: "0",
            expected_delay: Ok(None),
        },
        RenewalTestCase {
            description: "a negative lifetime means none given",
            expires_in: "-5",
            expected_delay: Ok(None),
        },
        RenewalTestCase {
            description: "null means none given",
            expires_in: "null",
            expected_delay: Ok(None),
        },
        RenewalTestCase {
            description: "text that is not a number fails",
            expires_in: "\"soon\"",
            expected_delay: Err("expires_in \"soon\" is not a number of seconds"),
        },
        RenewalTestCase {
            description: "an object fails",
            expires_in: "{\"s\": 1}",
            expected_delay: Err("expires_in {\"s\":1} is not a number of seconds"),
        },
    ];
    for test_case in test_cases {
        let value: serde_json::Value =
            serde_json::from_str(test_case.expires_in).unwrap_or_else(|_| unreachable!());
        let delay = renewal_delay(Some(&value));
        assert_eq!(
            delay,
            test_case.expected_delay.map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
