use crate::external_stage::_helpers::step_failure::step_failure;
use crate::external_stage::main::is_not_found::is_not_found;
use crate::external_stage::models::StepFailure;
use crate::external_stage::tests::helpers::api_status;
use crate::external_stage::tests::test_types::StepFailureTestCase;
use crate::http::errors::ApiError;

#[test]
fn given_a_probe_error_when_classifying_it_then_only_privileges_and_missing_objects_pass_a_step_over()
 {
    let test_cases = [
        StepFailureTestCase {
            description: "003001 insufficient privileges",
            error: api_status(200, "003001"),
            expected_unusable: true,
        },
        StepFailureTestCase {
            description: "002003 does not exist or not authorized",
            error: api_status(422, "002003"),
            expected_unusable: true,
        },
        StepFailureTestCase {
            description: "002043 object does not exist, or operation cannot be performed",
            error: api_status(422, "002043"),
            expected_unusable: true,
        },
        StepFailureTestCase {
            description: "Snowpipe Streaming's host refusing",
            error: api_status(403, ""),
            expected_unusable: true,
        },
        StepFailureTestCase {
            description: "Snowpipe Streaming's host without the object",
            error: api_status(404, ""),
            expected_unusable: true,
        },
        StepFailureTestCase {
            description: "001003 a syntax error",
            error: api_status(422, "001003"),
            expected_unusable: false,
        },
        StepFailureTestCase {
            description: "000606 no active warehouse",
            error: api_status(422, "000606"),
            expected_unusable: false,
        },
        StepFailureTestCase {
            description: "390112 an expired session",
            error: api_status(401, "390112"),
            expected_unusable: false,
        },
        StepFailureTestCase {
            description: "a server failure",
            error: api_status(503, ""),
            expected_unusable: false,
        },
        StepFailureTestCase {
            description: "throttling",
            error: api_status(429, ""),
            expected_unusable: false,
        },
        StepFailureTestCase {
            description: "the network",
            error: ApiError::Transport("connection reset".to_owned()),
            expected_unusable: false,
        },
        StepFailureTestCase {
            description: "a login failure",
            error: ApiError::Auth("login failed".to_owned()),
            expected_unusable: false,
        },
        StepFailureTestCase {
            description: "a response that makes no sense",
            error: ApiError::Protocol("only S3-backed internal stages".to_owned()),
            expected_unusable: false,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            matches!(step_failure(&test_case.error), StepFailure::Unusable(_)),
            test_case.expected_unusable,
            "{}",
            test_case.description
        );
        assert_eq!(
            is_not_found(&test_case.error),
            test_case.description.contains("not exist"),
            "{}",
            test_case.description
        );
    }
}
