use crate::adapters::clickhouse::_helpers::session::runner::exception_code;
use crate::adapters::clickhouse::tests::test_types::ExceptionCodeTestCase;

#[test]
fn given_a_failure_a_callers_session_reports_when_reading_its_code_then_the_exceptions_code_picks_the_fallback()
 {
    let test_cases = [
        ExceptionCodeTestCase {
            description: "the server's exception text leads with its code",
            message: "Code: 48. DB::Exception: EXCHANGE TABLES is not supported for Ordinary databases. (NOT_IMPLEMENTED)",
            expected_code: Some("48"),
        },
        ExceptionCodeTestCase {
            description: "a client's own prefix before it keeps the code readable",
            message: "HTTPDriver for http://localhost:8123 received ClickHouse error code 344\n Code: 344. DB::Exception: Lightweight delete is not supported",
            expected_code: Some("344"),
        },
        ExceptionCodeTestCase {
            description: "a failure that is no ClickHouse exception has no code",
            message: "connection refused",
            expected_code: None,
        },
        ExceptionCodeTestCase {
            description: "the prefix without digits is no code",
            message: "Code: unknown",
            expected_code: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            exception_code(test_case.message).as_deref(),
            test_case.expected_code,
            "{}",
            test_case.description
        );
    }
}
