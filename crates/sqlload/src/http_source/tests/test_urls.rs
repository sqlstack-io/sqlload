use crate::http_source::_helpers::endpoint::interval;
use crate::http_source::_helpers::urls::{filled_path, link_header_next, redacted, request_url};
use crate::http_source::tests::test_types::UrlsTestCase;

fn base() -> reqwest::Url {
    reqwest::Url::parse("https://api.shop.example/v1/").unwrap_or_else(|_| unreachable!())
}

#[test]
fn given_paths_links_and_secrets_when_building_requests_then_urls_and_errors_are_right() {
    let test_cases = [
        UrlsTestCase {
            description: "a path joins under the base URL's own path",
            value: || {
                request_url(&base(), "/orders")
                    .map(String::from)
                    .unwrap_or_default()
            },
            expected_value: "https://api.shop.example/v1/orders",
        },
        UrlsTestCase {
            description: "a full URL is used as it is",
            value: || {
                request_url(&base(), "https://other.example/x")
                    .map(String::from)
                    .unwrap_or_default()
            },
            expected_value: "https://other.example/x",
        },
        UrlsTestCase {
            description: "placeholders are filled and percent-encoded",
            value: || {
                let key = serde_json::json!({"id": "a b/c", "n": 7});
                filled_path(
                    "/o/{id}/{n}",
                    key.as_object().unwrap_or(&serde_json::Map::new()),
                )
                .unwrap_or_default()
            },
            expected_value: "/o/a%20b%2Fc/7",
        },
        UrlsTestCase {
            description: "a missing key names the placeholder",
            value: || filled_path("/o/{id}", &serde_json::Map::new()).unwrap_err(),
            expected_value: "fan-out path /o/{id} needs id, which the key {} lacks",
        },
        UrlsTestCase {
            description: "the Link header's next relation is found among others",
            value: || {
                link_header_next(
                    "<https://h/p?page=1>; rel=\"prev\", <https://h/p?page=3>; rel=\"next\"",
                )
                .unwrap_or_default()
            },
            expected_value: "https://h/p?page=3",
        },
        UrlsTestCase {
            description: "secrets in error text are hidden",
            value: || {
                redacted(
                    "GET https://h/x?key=s3cret: denied s3cret",
                    &["s3cret".to_owned()],
                )
            },
            expected_value: "GET https://h/x?key=***: denied ***",
        },
        UrlsTestCase {
            description: "600 a minute is one request every 100 ms",
            value: || format!("{:?}", interval("600/m").unwrap_or_default()),
            expected_value: "100ms",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            (test_case.value)(),
            test_case.expected_value,
            "{}",
            test_case.description
        );
    }
}
