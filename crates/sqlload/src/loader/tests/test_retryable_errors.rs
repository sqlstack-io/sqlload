use crate::http::errors::ApiError;
use crate::loader::errors::LoaderError;
use crate::loader::tests::helpers::{http_store, io_store, status, store, unbuildable_request};
use crate::loader::tests::test_types::RetryableErrorTestCase;
use crate::python_sources::errors::ReadError;

#[test]
fn given_a_failed_load_when_classifying_it_then_only_failures_a_rerun_may_pass_are_retryable() {
    let test_cases = [
        RetryableErrorTestCase {
            description: "a service that is unavailable",
            error: || status(503, ""),
            expected_retryable: true,
        },
        RetryableErrorTestCase {
            description: "throttling",
            error: || status(429, "rateLimitExceeded"),
            expected_retryable: true,
        },
        RetryableErrorTestCase {
            description: "a 500 with no code",
            error: || status(500, ""),
            expected_retryable: true,
        },
        RetryableErrorTestCase {
            description: "a ClickHouse timeout answered as 500",
            error: || status(500, "159"),
            expected_retryable: true,
        },
        RetryableErrorTestCase {
            description: "a ClickHouse syntax error answered as 500 is not",
            error: || status(500, "62"),
            expected_retryable: false,
        },
        RetryableErrorTestCase {
            description: "a failed statement reported as 400 is not",
            error: || status(400, "invalidQuery"),
            expected_retryable: false,
        },
        RetryableErrorTestCase {
            description: "a refused credential is not",
            error: || status(401, ""),
            expected_retryable: false,
        },
        RetryableErrorTestCase {
            description: "a missing permission is not",
            error: || status(403, "accessDenied"),
            expected_retryable: false,
        },
        RetryableErrorTestCase {
            description: "a request that never reached the service",
            error: || LoaderError::Snowflake(ApiError::Transport("connection reset".to_owned())),
            expected_retryable: true,
        },
        RetryableErrorTestCase {
            description: "an answer that could not be read is not",
            error: || LoaderError::Snowflake(ApiError::Protocol("not JSON".to_owned())),
            expected_retryable: false,
        },
        RetryableErrorTestCase {
            description: "an object store connection reset after its own retries",
            error: || io_store(std::io::ErrorKind::ConnectionReset),
            expected_retryable: true,
        },
        RetryableErrorTestCase {
            description: "an object store timeout",
            error: || io_store(std::io::ErrorKind::TimedOut),
            expected_retryable: true,
        },
        RetryableErrorTestCase {
            description: "a missing object is not",
            error: || {
                store(object_store::Error::NotFound {
                    path: "k".to_owned(),
                    source: "gone".into(),
                })
            },
            expected_retryable: false,
        },
        RetryableErrorTestCase {
            description: "an object store refusing the request for another reason is not",
            error: || io_store(std::io::ErrorKind::PermissionDenied),
            expected_retryable: false,
        },
        RetryableErrorTestCase {
            description: "an object store client that could not connect",
            error: || http_store(object_store::client::HttpErrorKind::Connect),
            expected_retryable: true,
        },
        RetryableErrorTestCase {
            description: "an object store answer that could not be decoded is not",
            error: || http_store(object_store::client::HttpErrorKind::Decode),
            expected_retryable: false,
        },
        RetryableErrorTestCase {
            description: "a request that cannot be built, such as a credential with a line break, is not",
            error: unbuildable_request,
            expected_retryable: false,
        },
        RetryableErrorTestCase {
            description: "a table another run is loading",
            error: || LoaderError::Transient("the table is being loaded by load x".to_owned()),
            expected_retryable: true,
        },
        RetryableErrorTestCase {
            description: "a column that appeared mid-load",
            error: || LoaderError::Replan("new column".to_owned()),
            expected_retryable: true,
        },
        RetryableErrorTestCase {
            description: "an sl.http job that outlasted its retries",
            error: || {
                LoaderError::Read(ReadError::transient(
                    "HTTP 503 (after 4 attempts)".to_owned(),
                ))
            },
            expected_retryable: true,
        },
        RetryableErrorTestCase {
            description: "a Python table that raised for any other reason is not",
            error: || LoaderError::Read(ReadError::from("ValueError: bad row".to_owned())),
            expected_retryable: false,
        },
        RetryableErrorTestCase {
            description: "data or configuration problems are not",
            error: || LoaderError::Invalid("column id: cannot cast".to_owned()),
            expected_retryable: false,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            (test_case.error)().is_retryable(),
            test_case.expected_retryable,
            "{}",
            test_case.description
        );
    }
}
