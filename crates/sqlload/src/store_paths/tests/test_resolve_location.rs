use crate::store_paths::main::resolve_location::resolve_location;
use crate::store_paths::models::StoreKind;
use crate::store_paths::tests::helpers::bucket;
use crate::store_paths::tests::test_types::ResolveLocationTestCase;

#[test]
fn given_a_path_when_resolving_then_the_store_and_folder_are_found() {
    let test_cases = [
        ResolveLocationTestCase {
            description: "a relative path is inside the connection's bucket",
            connection: bucket("example-bucket-prod"),
            path: "/models/scores",
            expected_location: Ok("s3://example-bucket-prod/models/scores/"),
        },
        ResolveLocationTestCase {
            description: "a full URL to the same bucket is accepted",
            connection: bucket("example-bucket-prod"),
            path: "s3://example-bucket-prod/models",
            expected_location: Ok("s3://example-bucket-prod/models/"),
        },
        ResolveLocationTestCase {
            description: "a URL to another bucket is rejected",
            connection: bucket("example-bucket-prod"),
            path: "s3://example-bucket-test/models",
            expected_location: Err(
                "points at S3 bucket example-bucket-test, but connection \"lake\" is S3 bucket example-bucket-prod",
            ),
        },
        ResolveLocationTestCase {
            description: "without a connection the scheme picks the store",
            connection: None,
            path: "s3://example-bucket-test/a//b",
            expected_location: Ok("s3://example-bucket-test/a/b/"),
        },
        ResolveLocationTestCase {
            description: "an absolute path is the local filesystem",
            connection: None,
            path: "file:///data/exports",
            expected_location: Ok("/data/exports/"),
        },
        ResolveLocationTestCase {
            description: "an absolute path under a filesystem connection becomes relative to its root",
            connection: Some(StoreKind::Filesystem {
                root: "/data/lake".to_owned(),
            }),
            path: "/data/lake/scores",
            expected_location: Ok("/data/lake/scores/"),
        },
        ResolveLocationTestCase {
            description: "a whole bucket is rejected",
            connection: None,
            path: "s3:///x",
            expected_location: Err("is empty"),
        },
        ResolveLocationTestCase {
            description: "an unknown scheme is rejected",
            connection: None,
            path: "ftp://host/x",
            expected_location: Err("not a location SQLLoad understands"),
        },
        ResolveLocationTestCase {
            description: "a relative path needs a connection",
            connection: None,
            path: "models/scores",
            expected_location: Err("not a location SQLLoad understands"),
        },
    ];
    for test_case in test_cases {
        let connection = test_case.connection.as_ref().map(|store| ("lake", store));
        let result = resolve_location(connection, test_case.path)
            .map(|location| location.to_string())
            .map_err(|error| error.to_string());
        assert_eq!(
            result.as_deref().ok(),
            test_case.expected_location.ok(),
            "{}",
            test_case.description
        );
        assert!(
            result
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_location.err().unwrap_or_default()),
            "{}: {:?}",
            test_case.description,
            test_case.path
        );
    }
}
