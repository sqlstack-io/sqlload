use crate::http_source::_helpers::job::job_problems;
use crate::http_source::models::HttpJob;
use crate::http_source::tests::test_types::JobProblemsTestCase;

#[test]
fn given_a_job_definition_when_checking_it_then_every_problem_is_found_before_any_request() {
    let test_cases = [
        JobProblemsTestCase {
            description: "a paginate with a next link and a selector is fine",
            job: r#"{"kind":"paginate","client":{"connection":"shop","retries":3,"rate_limit":"10/s"},"path":"/orders","paginator":{"kind":"next_link","pointer":"$.next"},"select":"$.items"}"#,
            expected_problems: &[],
        },
        JobProblemsTestCase {
            description: "a bad selector, rate and status are all reported",
            job: r#"{"kind":"paginate","client":{"connection":"shop","retries":0,"rate_limit":"fast"},"path":"/orders","paginator":{"kind":"single"},"select":"items","on_status":{"999":"skip"}}"#,
            expected_problems: &[
                "select \"items\" is not a path such as \"$\", \"$.items\" or \"$.items[*].id\"; use dotted field names after \"$\"",
                "rate_limit \"fast\" is not a rate such as \"10/s\", \"600/m\" or \"1000/h\"",
                "on_status 999 is not an HTTP status",
            ],
        },
        JobProblemsTestCase {
            description: "a paginate path with a placeholder needs fan_out",
            job: r#"{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/orders/{id}","paginator":{"kind":"single"}}"#,
            expected_problems: &[
                "paginate path /orders/{id} has the placeholder {id}; use fan_out to fill it",
            ],
        },
        JobProblemsTestCase {
            description: "a fan-out over a paginate whose field does not fill the placeholder",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/orders/{order_id}/items","paginator":{"kind":"single"},"concurrency":4,"over":{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/orders","paginator":{"kind":"single"},"select":"$.items[*].id"}}"#,
            expected_problems: &[
                "fan-out path /orders/{order_id}/items needs {order_id}, but over gives only id",
            ],
        },
        JobProblemsTestCase {
            description: "a fan-out over a paginate without a key field, and zero concurrency",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/orders/{id}","paginator":{"kind":"single"},"concurrency":0,"over":{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/orders","paginator":{"kind":"single"},"select":"$.items"}}"#,
            expected_problems: &[
                "fan_out concurrency must be at least 1",
                "fan_out over a paginate needs a select naming each key's field, such as \"$.items[*].id\", not \"$.items\"",
            ],
        },
        JobProblemsTestCase {
            description: "listed keys must name every placeholder of a path with two",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/{region}/orders/{id}","paginator":{"kind":"single"},"concurrency":2,"over":{"kind":"values","values":[{"region":"uk"}]}}"#,
            expected_problems: &[
                "fan-out path /{region}/orders/{id} needs {id}, which the key {\"region\":\"uk\"} lacks",
            ],
        },
        JobProblemsTestCase {
            description: "a pointer that is not a path, a zero limit and a bad header",
            job: r#"{"kind":"paginate","client":{"connection":"shop","retries":0,"headers":{"X Bad":"1"}},"path":"/o","paginator":{"kind":"next_link","pointer":"next"}}"#,
            expected_problems: &[
                "paginator pointer \"next\" is not a path such as \"$.next\" or \"$.meta.next_cursor\"",
                "header \"X Bad\" is not a valid HTTP header name and value",
            ],
        },
        JobProblemsTestCase {
            description: "an offset limit of zero",
            job: r#"{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/o","paginator":{"kind":"offset","param":"offset","limit_param":"limit","limit":0}}"#,
            expected_problems: &["sl.offset limit must be at least 1"],
        },
        JobProblemsTestCase {
            description: "a paginate selecting a key field",
            job: r#"{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/o","paginator":{"kind":"single"},"select":"$.items[*].id"}"#,
            expected_problems: &[
                "select \"$.items[*].id\" names a key field (id); that form is for fan_out's over, so select the rows instead",
            ],
        },
        JobProblemsTestCase {
            description: "listed values over a path without a placeholder",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/orders","paginator":{"kind":"single"},"concurrency":2,"over":{"kind":"values","values":[1,2]}}"#,
            expected_problems: &[
                "fan-out path /orders has no placeholder, so every key would request the same URL; add one such as {id}",
            ],
        },
    ];
    for test_case in test_cases {
        let job: HttpJob = serde_json::from_str(test_case.job).expect(test_case.description);
        assert_eq!(
            job_problems(&job),
            test_case.expected_problems,
            "{}",
            test_case.description
        );
    }
}
