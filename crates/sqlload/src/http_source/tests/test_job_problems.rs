use crate::http_source::_helpers::job::job_problems;
use crate::http_source::models::HttpJob;
use crate::http_source::tests::test_types::JobProblemsTestCase;
use crate::row_batches::models::ProjectionRules;

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
            description: "a third level may use the keys of both levels above it",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/o/{order_id}/i/{item_id}/n","paginator":{"kind":"single"},"concurrency":2,"over":{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/o/{order_id}/i","paginator":{"kind":"single"},"select":"$.items[*].item_id","concurrency":2,"over":{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/o","paginator":{"kind":"single"},"select":"$.items[*].order_id"}}}"#,
            expected_problems: &[],
        },
        JobProblemsTestCase {
            description: "a third level's placeholder that no level above gives",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/o/{order_id}/i/{item_id}/n/{customer_id}","paginator":{"kind":"single"},"concurrency":2,"over":{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/o/{order_id}/i","paginator":{"kind":"single"},"select":"$.items[*].item_id","concurrency":2,"over":{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/o","paginator":{"kind":"single"},"select":"$.items[*].order_id"}}}"#,
            expected_problems: &[
                "fan-out path /o/{order_id}/i/{item_id}/n/{customer_id} needs {customer_id}, but over gives only order_id, item_id",
            ],
        },
        JobProblemsTestCase {
            description: "a level that picks a key name a level above already gives",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/o/{order_id}/n","paginator":{"kind":"single"},"concurrency":2,"over":{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/o/{order_id}/i","paginator":{"kind":"single"},"select":"$.items[*].order_id","concurrency":2,"over":{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/o","paginator":{"kind":"single"},"select":"$.items[*].order_id"}}}"#,
            expected_problems: &[
                "fan_out over picks order_id, which a level above already gives; name it apart with key=",
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
                "fan-out path /orders has no placeholder, so every key would send the same request; add one such as {id} to the path or json_body",
            ],
        },
        JobProblemsTestCase {
            description: "a fan-out whose own select names a key field",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/orders/{id}","paginator":{"kind":"single"},"select":"$.items[*].sku","concurrency":2,"over":{"kind":"values","values":[1]}}"#,
            expected_problems: &[
                "select \"$.items[*].sku\" names a key field (sku); that form is for fan_out's over, so select the rows instead",
            ],
        },
        JobProblemsTestCase {
            description: "fields on a fan-out's over are refused; they belong to the fan-out's rows",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/orders/{id}","paginator":{"kind":"single"},"concurrency":2,"over":{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/orders","paginator":{"kind":"single"},"select":"$.items[*].id","fields":[{"name":"id","path":"$.id"}]}}"#,
            expected_problems: &[
                "fan_out's over picks keys with its select; fields and keep_record are for the fan-out's own rows",
            ],
        },
        JobProblemsTestCase {
            description: "a one-level fan-out's own select problem comes before its listed key's, as before multi-level fan-out",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/{region}/orders/{id}","paginator":{"kind":"single"},"select":"$.items[*].sku","concurrency":2,"over":{"kind":"values","values":[{"region":"uk"}]}}"#,
            expected_problems: &[
                "select \"$.items[*].sku\" names a key field (sku); that form is for fan_out's over, so select the rows instead",
                "fan-out path /{region}/orders/{id} needs {id}, which the key {\"region\":\"uk\"} lacks",
            ],
        },
        JobProblemsTestCase {
            description: "a one-level over's fields problem comes before its placeholder problem, as before multi-level fan-out",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/orders/{id}","paginator":{"kind":"single"},"concurrency":2,"over":{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/shops/{shop}/orders","paginator":{"kind":"single"},"select":"$.items[*].id","keep_record":"raw"}}"#,
            expected_problems: &[
                "fan_out's over picks keys with its select; fields and keep_record are for the fan-out's own rows",
                "fan_out over path /shops/{shop}/orders has the placeholder {shop}; over must be a plain paginate",
            ],
        },
        JobProblemsTestCase {
            description: "body placeholders filled from a paginate's keys; a GraphQL query's braces stay text",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/lookup","method":"POST","paginator":{"kind":"single"},"concurrency":2,"json_body":{"query":"{ order { lines } }","order":{"id":"{id}"}},"over":{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/orders","paginator":{"kind":"single"},"select":"$.items[*].id"}}"#,
            expected_problems: &[],
        },
        JobProblemsTestCase {
            description: "a body placeholder no level above gives",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/lookup","method":"POST","paginator":{"kind":"single"},"concurrency":2,"json_body":{"order":{"id":"{order_id}"}},"over":{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/orders","paginator":{"kind":"single"},"select":"$.items[*].id"}}"#,
            expected_problems: &["fan-out json_body needs {order_id}, but over gives only id"],
        },
        JobProblemsTestCase {
            description: "a listed key lacking a body placeholder",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/lookup","method":"POST","paginator":{"kind":"single"},"concurrency":2,"json_body":{"region":"{region}","id":"{id}"},"over":{"kind":"values","values":[{"id":1}]}}"#,
            expected_problems: &[
                "fan-out json_body needs {region}, which the key {\"id\":1} lacks",
            ],
        },
        JobProblemsTestCase {
            description: "a batch of zero, with a path placeholder and a placeholder within other text",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/lookup/{id}","method":"POST","paginator":{"kind":"single"},"concurrency":2,"batch":0,"json_body":{"q":"id in {id}"},"over":{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/orders","paginator":{"kind":"single"},"select":"$.items[*].id"}}"#,
            expected_problems: &[
                "fan_out batch must be at least 1",
                "a batched fan-out sends many keys in one request, so its path /lookup/{id} cannot have the placeholder {id}; put the keys in json_body",
                "json_body value \"id in {id}\" holds a placeholder within other text; a batched fan-out fills only a value that is just the placeholder, such as \"{id}\", with the list of its keys",
            ],
        },
        JobProblemsTestCase {
            description: "a batch without a body to put its keys in",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/lookup","method":"POST","paginator":{"kind":"single"},"concurrency":2,"batch":100,"over":{"kind":"values","values":[1]}}"#,
            expected_problems: &[
                "fan-out path /lookup has no placeholder, so every key would send the same request; add one such as {id} to the path or json_body",
                "fan_out batch= sends each batch's keys as a list in json_body, so it needs json_body with a placeholder, such as {\"ids\": \"{id}\"}",
            ],
        },
        JobProblemsTestCase {
            description: "a batched level used as another fan-out's over",
            job: r#"{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/o/{id}/l/{line_id}","paginator":{"kind":"single"},"concurrency":2,"over":{"kind":"fan_out","client":{"connection":"shop","retries":0},"path":"/lookup","method":"POST","paginator":{"kind":"single"},"concurrency":2,"batch":10,"select":"$.items[*].line_id","json_body":{"ids":"{id}"},"over":{"kind":"paginate","client":{"connection":"shop","retries":0},"path":"/orders","paginator":{"kind":"single"},"select":"$.items[*].id"}}}"#,
            expected_problems: &[
                "batch= is for the fan-out that loads rows; a fan_out used as over sends one request per key",
            ],
        },
    ];
    for test_case in test_cases {
        let job: HttpJob = serde_json::from_str(test_case.job).expect(test_case.description);
        assert_eq!(
            job_problems(&job, ProjectionRules::default()),
            test_case.expected_problems,
            "{}",
            test_case.description
        );
    }
}
