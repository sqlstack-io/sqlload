use crate::config::main::parse_config::parse_config;
use crate::config::models::FileFormat;
use crate::config::tests::test_types::{
    DatabricksConfigTestCase, ExampleConfigTestCase, ParseConfigErrorTestCase,
};
use crate::planning::main::load_path::load_path;
use crate::planning::models::LoadPath;

const DATABRICKS: &str = r#"
[source]
kind = "kafka"
brokers = "b"
include = ["shop.*"]

[destination]
kind = "databricks"
host = "dbc-1.cloud.databricks.com"
client_id = "id"
client_secret = "secret"
warehouse_id = "w1"
catalog = "Main"
schema = "RAW"
"#;

#[test]
fn given_a_snowflake_config_from_before_databricks_when_parsing_then_it_parses_unchanged()
-> Result<(), String> {
    let test_cases = [ExampleConfigTestCase {
        description: "the shipped example file is a Snowflake streaming config",
        path: "../../sqlload.example.toml",
        expected_database: "RAW",
        expected_schema: "KAFKA",
        expected_path: LoadPath::Streaming,
        expected_format: FileFormat::Json,
        expected_first_column: Some("TOPIC"),
        expected_column_count: 9,
    }];
    for test_case in &test_cases {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(test_case.path);
        let text = std::fs::read_to_string(&path).map_err(|error| error.to_string())?;
        let config =
            parse_config(&text, &|_| Some("x".to_owned())).map_err(|error| error.to_string())?;
        let first_column = config.envelope.columns.first().map(|column| column.name.as_str());
        assert_eq!(
            (
                config.destination.database(),
                config.destination.schema(),
                load_path(&config, false),
                config.staging.format,
                first_column,
                config.envelope.columns.len(),
            ),
            (
                test_case.expected_database,
                test_case.expected_schema,
                test_case.expected_path,
                test_case.expected_format,
                test_case.expected_first_column,
                test_case.expected_column_count,
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_databricks_staging_kinds_when_parsing_then_each_maps_to_its_route() -> Result<(), String> {
    let test_cases = [
        DatabricksConfigTestCase {
            description: "none streams through Zerobus Ingest",
            text: "zerobus_endpoint = \"https://1.zerobus.us-west-2.cloud.databricks.com\"\n[staging]\nkind = \"none\"\n",
            expected_path: LoadPath::Zerobus,
            expected_format: FileFormat::Parquet,
        },
        DatabricksConfigTestCase {
            description: "internal stages Parquet in a volume, the default format on Databricks",
            text: "[staging]\nkind = \"internal\"\n",
            expected_path: LoadPath::VolumeCopy,
            expected_format: FileFormat::Parquet,
        },
        DatabricksConfigTestCase {
            description: "s3 stages Parquet in the bucket without a Snowflake stage",
            text: "[staging]\nkind = \"s3\"\n[staging.s3]\nbucket = \"b\"\nregion = \"us-west-2\"\n",
            expected_path: LoadPath::S3Copy,
            expected_format: FileFormat::Parquet,
        },
    ];
    for test_case in &test_cases {
        let config = parse_config(&format!("{DATABRICKS}{}", test_case.text), &|_| None)
            .map_err(|error| error.to_string())?;
        let first_column = config.envelope.columns.first().map(|column| column.name.as_str());
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
                "main",
                "raw",
                Some("topic"),
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_databricks_settings_that_cannot_work_when_parsing_then_the_error_names_the_fix() {
    let test_cases = [
        ParseConfigErrorTestCase {
            description: "Zerobus Ingest needs its endpoint",
            text: "[staging]\nkind = \"none\"\n",
            expected_error: "needs destination.zerobus_endpoint",
        },
        ParseConfigErrorTestCase {
            description: "COPY INTO loads Parquet only",
            text: "[staging]\nkind = \"internal\"\nformat = \"json\"\n",
            expected_error: "Databricks loads Parquet files",
        },
        ParseConfigErrorTestCase {
            description: "Snowpipe is Snowflake's",
            text: "[staging]\nkind = \"internal\"\nload = \"snowpipe\"\n",
            expected_error: "Databricks loads with COPY INTO",
        },
        ParseConfigErrorTestCase {
            description: "an external stage is Snowflake's",
            text: "[staging]\nkind = \"s3\"\n[staging.s3]\nbucket = \"b\"\nregion = \"r\"\nstage = \"EXT\"\n",
            expected_error: "Unity Catalog external location",
        },
        ParseConfigErrorTestCase {
            description: "Snowflake keys are unknown on Databricks",
            text: "account = \"a\"\n[staging]\nkind = \"internal\"\n",
            expected_error: "unknown field `account`",
        },
    ];
    for test_case in &test_cases {
        let error = parse_config(&format!("{DATABRICKS}{}", test_case.text), &|_| None)
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
