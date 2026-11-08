use crate::adapters::clickhouse::_helpers::dialect::pull::{
    PullCredentials, PulledFile, object_url, pull_insert,
};
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

#[test]
fn given_a_parquet_file_in_s3_when_clickhouse_pulls_it_then_each_column_takes_the_file_its_constant_or_null()
 {
    let column = |name: &str, column_type: ColumnType| DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Source,
    };
    let columns = [
        column("id", ColumnType::Int),
        column("raw", ColumnType::Binary),
        column("gone", ColumnType::Text),
        column("day", ColumnType::Date),
        column("region", ColumnType::Text),
        column("_sqlload_source_row", ColumnType::Int),
        column("_sqlload_loaded_at", ColumnType::TimestampTz),
    ];
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
        secret: "it's".to_owned(),
        token: Some("token".to_owned()),
    };
    let sql_type = |column_type: &ColumnType| format!("Nullable({column_type})");
    assert_eq!(
        pull_insert(
            "`raw`.`s`",
            (&columns, &sql_type),
            &file,
            &credentials,
            ("load1", "2026-11-08T07:00:00Z"),
        ),
        "INSERT INTO `raw`.`s` (`id`, `raw`, `gone`, `day`, `region`, `_sqlload_source_row`, `_sqlload_loaded_at`) SELECT CAST(`id` AS Nullable(int)), CAST(substring(`raw`, 1) AS Nullable(binary)), CAST(NULL AS Nullable(text)), CAST(CAST('2026-01-01' AS Date32) AS Nullable(date)), CAST(NULL AS Nullable(text)), CAST(rowNumberInAllBlocks() + 1 AS Nullable(int)), CAST(parseDateTime64BestEffort('2026-11-08T07:00:00Z', 6, 'UTC') AS Nullable(timestamp_tz)) FROM s3('https://example-bucket.s3.eu-west-2.amazonaws.com/lake/day=2026-01-01/a.parquet', 'AKIDEXAMPLE', 'it\\'s', 'token', 'Parquet') SETTINGS max_threads = 1, max_insert_threads = 1, input_format_parquet_preserve_order = 1",
        "the folder's value wins over the file's day, a column no file holds and a missing folder are NULL, and rows are numbered in stored order"
    );
}

#[test]
fn given_an_object_key_when_clickhouse_reads_it_then_its_url_is_on_aws_or_the_endpoint_and_percent_encoded()
 {
    let test_cases = [
        (
            "AWS in a known region, by virtual host; a space and a percent sign are encoded",
            (Some("eu-west-2"), None),
            "lake/day=2026-01-01/part one 100%.parquet",
            "https://example-bucket.s3.eu-west-2.amazonaws.com/lake/day=2026-01-01/part%20one%20100%25.parquet",
        ),
        (
            "AWS with no region",
            (None, None),
            "a.parquet",
            "https://example-bucket.s3.amazonaws.com/a.parquet",
        ),
        (
            "a custom endpoint, by path, its trailing slash dropped",
            (Some("us-east-1"), Some("http://172.17.0.4:8333/")),
            "lake/a.parquet",
            "http://172.17.0.4:8333/example-bucket/lake/a.parquet",
        ),
    ];
    for (description, (region, endpoint), key, expected_url) in test_cases {
        assert_eq!(
            object_url(("example-bucket", key), region, endpoint).as_deref(),
            Ok(expected_url),
            "{description}"
        );
    }
}
