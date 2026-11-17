use crate::adapters::duckdb::_helpers::pull_sql::s3_secret;
use crate::adapters::duckdb::tests::test_types::S3SecretTestCase;

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
