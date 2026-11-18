use crate::adapters::snowflake::tests::helpers::unloaded_requests_for;
use crate::adapters::snowflake::tests::test_types::UnloadedRequestsTestCase;

#[test]
fn given_an_unload_loaded_inside_snowflake_when_sending_its_statements_then_they_fit_the_script_limit_and_a_direct_append_never_commits_part()
 {
    let test_cases = [
        UnloadedRequestsTestCase {
            description: "a few COPYs go in one request that commits only when every unloaded row loads",
            copies: Some((3, 100_000)),
            direct: false,
            expected_requests: Ok(1),
            expected_checks_count: true,
        },
        UnloadedRequestsTestCase {
            description: "COPYs past the script limit are split, each request committing its part into the staging table",
            copies: Some((20, 120_000)),
            direct: false,
            expected_requests: Ok(2),
            expected_checks_count: false,
        },
        UnloadedRequestsTestCase {
            description: "a direct append past the limit fails before loading anything",
            copies: Some((20, 120_000)),
            direct: true,
            expected_requests: Err("the unload wrote 20000 files"),
            expected_checks_count: false,
        },
        UnloadedRequestsTestCase {
            description: "a direct append within the limit goes in one counted request",
            copies: Some((3, 100_000)),
            direct: true,
            expected_requests: Ok(1),
            expected_checks_count: true,
        },
        UnloadedRequestsTestCase {
            description: "an INSERT ... SELECT is one request",
            copies: None,
            direct: true,
            expected_requests: Ok(1),
            expected_checks_count: false,
        },
    ];
    for test_case in &test_cases {
        let requests = unloaded_requests_for(test_case);
        match (&requests, test_case.expected_requests) {
            (Ok(requests), Ok(expected)) => {
                assert_eq!(requests.len(), expected, "{}", test_case.description);
                assert_eq!(
                    requests[0].contains("IF (loaded = "),
                    test_case.expected_checks_count,
                    "{}",
                    test_case.description
                );
            }
            (Err(error), Err(expected)) => assert!(
                error.starts_with(expected),
                "{}: {error}",
                test_case.description
            ),
            _ => panic!("{}: {requests:?}", test_case.description),
        }
    }
}
