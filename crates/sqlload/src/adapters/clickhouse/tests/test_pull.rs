use crate::adapters::clickhouse::_helpers::dialect::pull::{
    PullCredentials, PulledFile, credential_parameters, object_url, pull_insert,
};
use crate::adapters::clickhouse::tests::helpers::desired;
use crate::adapters::clickhouse::tests::test_types::{ObjectUrlTestCase, PullInsertTestCase};
use crate::column_types::models::ColumnType;

#[test]
fn given_a_parquet_file_in_s3_when_clickhouse_pulls_it_then_each_column_takes_the_file_its_constant_or_null()
 {
    let file = PulledFile {
        url: "https://example-bucket.s3.eu-west-2.amazonaws.com/lake/day=2026-01-01/a.parquet"
            .to_owned(),
        in_file: vec!["id".to_owned(), "raw".to_owned(), "day".to_owned()],
        constants: vec![
            ("day".to_owned(), Some("2026-01-01".to_owned())),
            ("region".to_owned(), None),
        ],
    };
    let credentials = PullCredentials {
        key_id: "AKIDEXAMPLE".to_owned(),
        secret: "it's a\\b\tc\nd".to_owned(),
        token: Some("token".to_owned()),
    };
    let test_cases = [
        PullInsertTestCase {
            description: "credentials are named as query parameters, never written into the statement, and escaped as ClickHouse reads parameter values; the folder's value wins over the file's day, a column no file holds and a missing folder are NULL, a fixed-size binary keeps its zero bytes, and rows are numbered in stored order by one thread",
            columns: vec![
                desired("id", ColumnType::Int),
                desired("raw", ColumnType::Binary),
                desired("gone", ColumnType::Text),
                desired("day", ColumnType::Date),
                desired("region", ColumnType::Text),
                desired("_sqlload_source_row", ColumnType::Int),
                desired("_sqlload_loaded_at", ColumnType::TimestampTz),
            ],
            signed: true,
            expected_parameters: &[
                ("param_sqlload_key_id", "AKIDEXAMPLE"),
                ("param_sqlload_secret", "it's a\\\\b\\tc\\nd"),
                ("param_sqlload_token", "token"),
            ],
            expected_insert: "INSERT INTO `raw`.`s` (`id`, `raw`, `gone`, `day`, `region`, `_sqlload_source_row`, `_sqlload_loaded_at`) SELECT CAST(`id` AS Nullable(int)), CAST(substring(`raw`, 1) AS Nullable(binary)), CAST(NULL AS Nullable(text)), CAST(CAST('2026-01-01' AS Date32) AS Nullable(date)), CAST(NULL AS Nullable(text)), CAST(rowNumberInAllBlocks() + 1 AS Nullable(int)), CAST(parseDateTime64BestEffort('2026-11-08T07:00:00Z', 6, 'UTC') AS Nullable(timestamp_tz)) FROM s3('https://example-bucket.s3.eu-west-2.amazonaws.com/lake/day=2026-01-01/a.parquet', {sqlload_key_id:String}, {sqlload_secret:String}, {sqlload_token:String}, 'Parquet') SETTINGS max_threads = 1, max_insert_threads = 1, input_format_parquet_preserve_order = 1",
        },
        PullInsertTestCase {
            description: "without row numbers the read keeps ClickHouse's parallelism, and a public bucket's read is unsigned, with no credentials sent",
            columns: vec![
                desired("id", ColumnType::Int),
                desired("_sqlload_load_id", ColumnType::Text),
            ],
            signed: false,
            expected_parameters: &[],
            expected_insert: "INSERT INTO `raw`.`s` (`id`, `_sqlload_load_id`) SELECT CAST(`id` AS Nullable(int)), CAST(CAST('load1' AS String) AS Nullable(text)) FROM s3('https://example-bucket.s3.eu-west-2.amazonaws.com/lake/day=2026-01-01/a.parquet', NOSIGN, 'Parquet')",
        },
    ];
    let sql_type = |column_type: &ColumnType| format!("Nullable({column_type})");
    for test_case in test_cases {
        let signed = test_case.signed.then_some(&credentials);
        let insert = pull_insert(
            "`raw`.`s`",
            (&test_case.columns, &sql_type),
            (&file, signed),
            (
                crate::load_naming::models::Naming::Sqlload,
                "load1",
                "2026-11-08T07:00:00Z",
            ),
        );
        let expected_parameters: Vec<(String, String)> = test_case
            .expected_parameters
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        assert_eq!(
            (insert.as_str(), credential_parameters(signed)),
            (test_case.expected_insert, expected_parameters),
            "{}",
            test_case.description
        );
        assert!(
            !insert.contains("it's") && !insert.contains("AKIDEXAMPLE"),
            "{}: the statement holds a credential",
            test_case.description
        );
    }
}

#[test]
fn given_an_object_key_when_clickhouse_reads_it_then_its_url_is_on_aws_or_the_endpoint_and_percent_encoded()
 {
    let test_cases = [
        ObjectUrlTestCase {
            description: "AWS in a known region, by virtual host; a space and a percent sign are encoded",
            bucket: "example-bucket",
            in_endpoint: false,
            region: Some("eu-west-2"),
            endpoint: None,
            key: "lake/day=2026-01-01/part one 100%.parquet",
            expected_url: "https://example-bucket.s3.eu-west-2.amazonaws.com/lake/day=2026-01-01/part%20one%20100%25.parquet",
        },
        ObjectUrlTestCase {
            description: "AWS with no region",
            bucket: "example-bucket",
            in_endpoint: false,
            region: None,
            endpoint: None,
            key: "a.parquet",
            expected_url: "https://example-bucket.s3.amazonaws.com/a.parquet",
        },
        ObjectUrlTestCase {
            description: "a custom endpoint, by path, its trailing slash dropped",
            bucket: "example-bucket",
            in_endpoint: false,
            region: Some("us-east-1"),
            endpoint: Some("http://172.17.0.4:8333/"),
            key: "lake/a.parquet",
            expected_url: "http://172.17.0.4:8333/example-bucket/lake/a.parquet",
        },
        ObjectUrlTestCase {
            description: "a virtual-hosted custom endpoint already names the bucket",
            bucket: "example-bucket",
            in_endpoint: true,
            region: None,
            endpoint: Some("https://example-bucket.objects.example.com"),
            key: "lake/a.parquet",
            expected_url: "https://example-bucket.objects.example.com/lake/a.parquet",
        },
        ObjectUrlTestCase {
            description: "a bucket whose name holds a dot is read by path, which AWS's certificate covers",
            bucket: "example.bucket",
            in_endpoint: false,
            region: Some("eu-west-2"),
            endpoint: None,
            key: "a.parquet",
            expected_url: "https://s3.eu-west-2.amazonaws.com/example.bucket/a.parquet",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            object_url(
                (test_case.bucket, test_case.key),
                test_case.region,
                (test_case.endpoint, test_case.in_endpoint)
            )
            .as_deref(),
            Ok(test_case.expected_url),
            "{}",
            test_case.description
        );
    }
}
