use std::time::Duration;

use crate::http_source::tests::helpers::{LIMIT_INTERVAL, read_ahead_turns};
use crate::http_source::tests::test_types::LimiterTestCase;

#[test]
fn given_requests_waiting_under_a_rate_limit_when_some_are_aborted_while_waiting_then_no_two_go_closer_than_the_rate_allows()
 {
    let test_cases = [
        LimiterTestCase {
            description: "seven requests, none aborted: each takes its turn",
            aborted: &[],
            expected_turns: 7,
        },
        LimiterTestCase {
            description: "two read-ahead requests aborted while they wait: the later requests still wait their turns",
            aborted: &[1, 2],
            expected_turns: 5,
        },
    ];
    for test_case in test_cases {
        let went = read_ahead_turns(test_case.aborted);
        let gaps: Vec<Duration> = went.windows(2).map(|pair| pair[1] - pair[0]).collect();
        assert_eq!(
            (went.len(), gaps.iter().all(|gap| *gap >= LIMIT_INTERVAL)),
            (test_case.expected_turns, true),
            "{}: {went:?}",
            test_case.description
        );
    }
}
