use crate::config::main::parse_config::parse_config;
use crate::config::models::FileFormat;
use crate::config::tests::test_types::{BigQueryConfigTestCase, ParseConfigErrorTestCase};
use crate::planning::main::load_path::load_path;
use crate::planning::models::LoadPath;

pub(crate) const BIGQUERY: &str = r#"
[source]
kind = "kafka"
brokers = "b"
include = ["shop.*"]

[destination]
kind = "bigquery"
project = "my-proj"
dataset = "Raw"
location = "EU"
service_account_key = "{}"
"#;

#[test]
fn given_bigquery_staging_kinds_when_parsing_then_each_maps_to_its_route() -> Result<(), String> {
    let test_cases = [
        BigQueryConfigTestCase {
            description: "none appends to the Storage Write API's default stream",
            text: "[staging]\nkind = \"none\"\n",
            expected_path: LoadPath::WriteApi,
            expected_format: FileFormat::Parquet,
        },
        BigQueryConfigTestCase {
            description: "internal sends a Parquet file with each load job",
            text: "[staging]\nkind = \"internal\"\n",
            expected_path: LoadPath::UploadLoad,
            expected_format: FileFormat::Parquet,
        },
        BigQueryConfigTestCase {
            description: "gcs loads Parquet files from the bucket",
            text: "[staging]\nkind = \"gcs\"\n[staging.gcs]\nbucket = \"b\"\n",
            expected_path: LoadPath::GcsLoad,
            expected_format: FileFormat::Parquet,
        },
    ];
    for test_case in &test_cases {
        let config = parse_config(&format!("{BIGQUERY}{}", test_case.text), &|_| None)
            .map_err(|error| error.to_string())?;
        let first_column = config
            .envelope
            .columns
            .first()
            .map(|column| (column.name.as_str(), column.sql_type.as_str()));
        assert_eq!(
            (
                load_path(&config, false),
                config.staging.format,
                config.destination.database(),
                config.destination.schema(),
                first_column,
            ),
            (
                test_case.expected_path,
                test_case.expected_format,
                "my-proj",
                "Raw",
                Some(("topic", "STRING")),
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_bigquery_settings_that_cannot_work_when_parsing_then_the_error_names_the_fix() {
    let test_cases = [
        ParseConfigErrorTestCase {
            description: "s3 is not a BigQuery route",
            text: "[staging]\nkind = \"s3\"\n[staging.s3]\nbucket = \"b\"\nregion = \"r\"\n",
            expected_error: "is not a BigQuery route",
        },
        ParseConfigErrorTestCase {
            description: "load jobs read Parquet only",
            text: "[staging]\nkind = \"internal\"\nformat = \"json\"\n",
            expected_error: "BigQuery loads Parquet files",
        },
        ParseConfigErrorTestCase {
            description: "gcs needs its bucket",
            text: "[staging]\nkind = \"gcs\"\n",
            expected_error: "needs a [staging.gcs] section",
        },
        ParseConfigErrorTestCase {
            description: "a bucket section without the gcs route is a mistake",
            text: "[staging]\nkind = \"internal\"\n[staging.gcs]\nbucket = \"b\"\n",
            expected_error: "only used when staging.kind = \"gcs\"",
        },
        ParseConfigErrorTestCase {
            description: "the bucket path needs a date folder",
            text: "[staging]\nkind = \"gcs\"\n[staging.gcs]\nbucket = \"b\"\npath = \"{topic}/\"\n",
            expected_error: "staging.gcs.path must contain {date}",
        },
    ];
    for test_case in &test_cases {
        let error = parse_config(&format!("{BIGQUERY}{}", test_case.text), &|_| None)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(
            error.contains(test_case.expected_error),
            "{}: {error}",
            test_case.description
        );
    }
}
