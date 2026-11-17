use std::time::Duration;

use crate::delivery::errors::DeliveryError;
use crate::exporter::_helpers::stream_task::{AfterFailure, after_failure};
use crate::exporter::tests::test_types::AfterFailureTestCase;
use crate::http::models::RetryPolicy;

#[test]
fn given_a_failed_delivery_when_deciding_the_next_step_then_retries_back_off_and_stop_after_four_minutes()
 {
    let refused = DeliveryError::Retry("open stream was refused".to_owned());
    let test_cases = [
        AfterFailureTestCase {
            description: "the first retry waits the first delay",
            error: refused.clone(),
            elapsed: Duration::ZERO,
            attempt: 1,
            expected_step: AfterFailure::Wait(Duration::from_millis(250)),
        },
        AfterFailureTestCase {
            description: "later retries double the delay",
            error: refused.clone(),
            elapsed: Duration::from_secs(2),
            attempt: 4,
            expected_step: AfterFailure::Wait(Duration::from_secs(2)),
        },
        AfterFailureTestCase {
            description: "the delay stops growing at 30 seconds",
            error: refused.clone(),
            elapsed: Duration::from_secs(200),
            attempt: 20,
            expected_step: AfterFailure::Wait(Duration::from_secs(30)),
        },
        AfterFailureTestCase {
            description: "a retry at exactly four minutes still waits",
            error: refused.clone(),
            elapsed: Duration::from_secs(240),
            attempt: 14,
            expected_step: AfterFailure::Wait(Duration::from_secs(30)),
        },
        AfterFailureTestCase {
            description: "a retry past four minutes stops the stream instead of looping",
            error: refused.clone(),
            elapsed: Duration::from_millis(240_001),
            attempt: 15,
            expected_step: AfterFailure::Stop(
                "gave up delivering a batch after 240s: delivery failed, will retry: open stream was refused"
                    .to_owned(),
            ),
        },
        AfterFailureTestCase {
            description: "a fatal error stops at once",
            error: DeliveryError::Fatal("invalid table name".to_owned()),
            elapsed: Duration::ZERO,
            attempt: 1,
            expected_step: AfterFailure::Stop("invalid table name".to_owned()),
        },
        AfterFailureTestCase {
            description: "a reset reopens from the committed offsets, even late",
            error: DeliveryError::Reset("channel invalidated".to_owned()),
            elapsed: Duration::from_secs(600),
            attempt: 30,
            expected_step: AfterFailure::Reset,
        },
    ];
    for test_case in &test_cases {
        assert_eq!(
            after_failure(
                &test_case.error,
                test_case.elapsed,
                test_case.attempt,
                &RetryPolicy::default()
            ),
            test_case.expected_step,
            "{}",
            test_case.description
        );
    }
}
