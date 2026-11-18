use crate::adapters::contract::main::_custom_endpoint::custom_endpoint;
use crate::adapters::duckdb::_helpers::pull_sql::{default_region, s3_secret};
use crate::adapters::duckdb::tests::test_types::{AwsVariablesTestCase, S3SecretTestCase};
use crate::store_paths::models::StoreKind;

#[test]
fn given_an_s3_bucket_when_duckdb_reads_its_files_itself_then_its_secret_carries_sqlloads_keys_only_as_parameters()
 {
    let test_cases = [
        S3SecretTestCase {
            description: "static keys go in key id, secret, token order, scoped to the bucket",
            bucket: "lake-1",
            keys: Some(["AKIAEXAMPLE", "s3cret", "tok"]),
            expected_sql: "sqlload_s3_6c616b652d31 (TYPE s3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?, REGION ?, SCOPE ?)",
            expected_parameters: &["AKIAEXAMPLE", "s3cret", "tok", "eu-west-2", "s3://lake-1/"],
        },
        S3SecretTestCase {
            description: "with no static keys DuckDB finds them through the AWS credential chain",
            bucket: "lake-2",
            keys: None,
            expected_sql: "sqlload_s3_6c616b652d32 (TYPE s3, PROVIDER credential_chain, REGION ?, SCOPE ?)",
            expected_parameters: &["eu-west-2", "s3://lake-2/"],
        },
    ];
    for test_case in test_cases {
        let keys = test_case.keys.map(|keys| keys.map(str::to_owned));
        let (sql, parameters) = s3_secret(test_case.bucket, "eu-west-2".to_owned(), keys.as_ref());
        assert_eq!(
            (sql, parameters),
            (
                format!(
                    "CREATE OR REPLACE TEMPORARY SECRET {}",
                    test_case.expected_sql
                ),
                test_case
                    .expected_parameters
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect::<Vec<_>>()
            ),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_aws_variables_when_duckdb_reads_s3_itself_then_an_empty_one_counts_as_unset() {
    let test_cases = [
        AwsVariablesTestCase {
            description: "an endpoint in the environment leaves the files to SQLLoad's reader, and AWS_REGION wins",
            variables: &[
                ("AWS_ENDPOINT_URL", "http://127.0.0.1:9000"),
                ("AWS_REGION", "eu-west-2"),
                ("AWS_DEFAULT_REGION", "us-west-1"),
            ],
            expected_custom_endpoint: true,
            expected_region: "eu-west-2",
        },
        AwsVariablesTestCase {
            description: "empty variables are unset: DuckDB reads the files itself, in AWS_DEFAULT_REGION",
            variables: &[
                ("AWS_ENDPOINT", ""),
                ("AWS_ENDPOINT_URL", ""),
                ("AWS_REGION", ""),
                ("AWS_DEFAULT_REGION", "us-west-1"),
            ],
            expected_custom_endpoint: false,
            expected_region: "us-west-1",
        },
        AwsVariablesTestCase {
            description: "with no region at all, us-east-1",
            variables: &[("AWS_DEFAULT_REGION", "")],
            expected_custom_endpoint: false,
            expected_region: "us-east-1",
        },
    ];
    let store = StoreKind::S3 {
        bucket: "lake".to_owned(),
        region: None,
        endpoint: None,
        profile: None,
    };
    for test_case in test_cases {
        let variable = |name: &str| {
            test_case
                .variables
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        };
        assert_eq!(
            (
                custom_endpoint(&store, &variable).is_some(),
                default_region(&variable)
            ),
            (
                test_case.expected_custom_endpoint,
                test_case.expected_region.to_owned()
            ),
            "{}",
            test_case.description
        );
    }
}
