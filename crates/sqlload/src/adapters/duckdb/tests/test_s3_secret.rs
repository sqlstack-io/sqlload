use crate::adapters::contract::main::_custom_endpoint::custom_endpoint;
use std::time::{Duration, Instant};

use crate::adapters::duckdb::_helpers::pull_sql::{
    BucketAccess, bucket_access, default_region, s3_secret, secret_current,
};
use crate::adapters::duckdb::tests::test_types::{
    AwsVariablesTestCase, GcsAccessTestCase, S3SecretTestCase, SecretCurrentTestCase,
};
use crate::auth::models::Secret;
use crate::store_paths::models::{HmacKey, StoreKind};

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

#[test]
fn given_a_buckets_secret_already_set_when_a_table_opens_the_bucket_then_it_is_set_again_only_when_its_keys_change_or_the_chain_may_have_expired()
 {
    let keys: &[&str] = &["AKIAEXAMPLE", "s3cret", "", "eu-west-2", "s3://lake/"];
    let rotated: &[&str] = &["AKIAEXAMPLE", "s3cret2", "", "eu-west-2", "s3://lake/"];
    let chain: &[&str] = &["eu-west-2", "s3://lake/"];
    let test_cases = [
        SecretCurrentTestCase {
            description: "nothing set yet",
            set: None,
            wanted: (keys, false),
            expected_current: false,
        },
        SecretCurrentTestCase {
            description: "the same static keys stay current however long ago they were set",
            set: Some((keys, false, 3_600)),
            wanted: (keys, false),
            expected_current: true,
        },
        SecretCurrentTestCase {
            description: "changed keys are set again",
            set: Some((keys, false, 1)),
            wanted: (rotated, false),
            expected_current: false,
        },
        SecretCurrentTestCase {
            description: "a chain secret set just under four minutes ago is current",
            set: Some((chain, true, 239)),
            wanted: (chain, true),
            expected_current: true,
        },
        SecretCurrentTestCase {
            description: "a chain secret set four minutes ago is set again, as its keys may expire",
            set: Some((chain, true, 240)),
            wanted: (chain, true),
            expected_current: false,
        },
        SecretCurrentTestCase {
            description: "keys appearing in the environment replace the chain secret",
            set: Some((chain, true, 1)),
            wanted: (keys, false),
            expected_current: false,
        },
    ];
    let now = Instant::now() + Duration::from_secs(7_200);
    let access = |parameters: &[&str], chain: bool| BucketAccess {
        bucket: "lake".to_owned(),
        extensions: "",
        statement: "CREATE OR REPLACE TEMPORARY SECRET sqlload_s3_6c616b65".to_owned(),
        parameters: parameters.iter().map(|value| (*value).to_owned()).collect(),
        chain,
    };
    for test_case in test_cases {
        let set = test_case.set.map(|(parameters, chain, age)| {
            access(parameters, chain).set_at(now - Duration::from_secs(age))
        });
        let (parameters, chain) = test_case.wanted;
        let wanted = access(parameters, chain).set_at(now);
        assert_eq!(
            secret_current(set.as_ref(), &wanted, now),
            test_case.expected_current,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_gcs_bucket_when_duckdb_reads_its_files_itself_then_its_secret_carries_the_connections_hmac_key_only_as_parameters()
-> Result<(), String> {
    let test_cases = [
        GcsAccessTestCase {
            description: "an HMAC key becomes a GCS secret scoped to the bucket, named apart from an S3 bucket of the same name",
            hmac: Some(("GOOG1EXAMPLE", "hm4c")),
            expected_access: Some((
                "gs://lake",
                "CREATE OR REPLACE TEMPORARY SECRET sqlload_gcs_6c616b65 (TYPE gcs, KEY_ID ?, SECRET ?, SCOPE ?)",
                &["GOOG1EXAMPLE", "hm4c", "gs://lake/"],
            )),
        },
        GcsAccessTestCase {
            description: "without one DuckDB has no way into the bucket, as it takes no service-account key",
            hmac: None,
            expected_access: None,
        },
    ];
    for test_case in test_cases {
        let store = StoreKind::Gcs {
            bucket: "lake".to_owned(),
            service_account_key: Some(Secret("{}".to_owned())),
            endpoint: None,
            hmac: test_case.hmac.map(|(key_id, secret)| HmacKey {
                key_id: key_id.to_owned(),
                secret: Secret(secret.to_owned()),
            }),
        };
        let access = bucket_access(&store).map_err(|error| error.to_string())?;
        let found = access.map(|access| {
            (
                access.bucket,
                access.extensions,
                access.statement,
                access.parameters,
                access.chain,
            )
        });
        assert_eq!(
            found,
            test_case
                .expected_access
                .map(|(bucket, statement, parameters)| {
                    (
                        bucket.to_owned(),
                        "INSTALL httpfs; LOAD httpfs;",
                        statement.to_owned(),
                        parameters
                            .iter()
                            .map(|value| (*value).to_owned())
                            .collect::<Vec<_>>(),
                        false,
                    )
                }),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
