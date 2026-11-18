use std::collections::BTreeMap;

use crate::config::main::parse_config::parse_config;
use crate::naming::main::resolve_streams::resolve_streams;
use crate::planning::main::build_plan::build_plan;
use crate::planning::main::load_pacing::load_pacing;
use crate::planning::models::ExistingObjects;
use crate::planning::tests::helpers::object_summary;
use crate::planning::tests::test_types::{BigQueryPlanTestCase, LoadPacingTestCase};

const BIGQUERY: &str = "[source]\nkind = \"kafka\"\nbrokers = \"b\"\ninclude = [\"*\"]\n[destination]\nkind = \"bigquery\"\nproject = \"my-proj\"\ndataset = \"raw\"\nlocation = \"EU\"\nservice_account_key = \"{}\"\n";

const CREATE: &str = "CREATE TABLE IF NOT EXISTS `my-proj`.`raw`.`shop__orders` (`topic` STRING, `kafka_timestamp` INT64, `kafka_timestamp_type` INT64, `kafka_offset` INT64, `kafka_partition` INT64, `key` STRING, `value` STRING, `exported_at` TIMESTAMP, `date` DATE)";

#[test]
fn given_bigquery_routes_when_planning_then_only_the_table_is_needed() -> Result<(), String> {
    let test_cases = [
        BigQueryPlanTestCase {
            description: "the Write API needs only the table",
            staging: "[staging]\nkind = \"none\"\n",
            existing_columns: &[],
            expected_objects: &["+table shop__orders"],
            expected_sql: &[CREATE],
        },
        BigQueryPlanTestCase {
            description: "a load job with its file needs no stage or bucket object",
            staging: "[staging]\nkind = \"internal\"\n",
            existing_columns: &[],
            expected_objects: &["+table shop__orders"],
            expected_sql: &[CREATE],
        },
        BigQueryPlanTestCase {
            description: "an existing table matches the types INFORMATION_SCHEMA reports",
            staging: "[staging]\nkind = \"gcs\"\n[staging.gcs]\nbucket = \"b\"\n",
            existing_columns: &[
                ("topic", "STRING"),
                ("kafka_timestamp", "INT64"),
                ("kafka_timestamp_type", "INT64"),
                ("kafka_offset", "INT64"),
                ("kafka_partition", "INT64"),
                ("key", "STRING"),
                ("value", "STRING"),
                ("exported_at", "TIMESTAMP"),
                ("date", "DATE"),
            ],
            expected_objects: &["=table shop__orders"],
            expected_sql: &[],
        },
        BigQueryPlanTestCase {
            description: "a column of another type is incompatible",
            staging: "[staging]\nkind = \"none\"\n",
            existing_columns: &[("topic", "STRING"), ("kafka_offset", "STRING")],
            expected_objects: &["!table shop__orders"],
            expected_sql: &[],
        },
    ];
    for test_case in &test_cases {
        let config = parse_config(&format!("{BIGQUERY}{}", test_case.staging), &|_| None)
            .map_err(|error| error.to_string())?;
        let streams = resolve_streams(&config, &["shop.orders".to_owned()])
            .map_err(|problems| problems.join("; "))?;
        let mut existing = ExistingObjects::default();
        let table: BTreeMap<String, String> = test_case
            .existing_columns
            .iter()
            .map(|(name, data_type)| ((*name).to_owned(), (*data_type).to_owned()))
            .collect();
        existing
            .tables
            .extend((!table.is_empty()).then_some(("shop__orders".to_owned(), table)));
        let plan = build_plan(&config, &streams, &existing, false);
        let statements: Vec<&str> = plan
            .missing()
            .iter()
            .map(|object| object.create_sql.as_str())
            .collect();
        assert_eq!(
            (object_summary(&plan), statements),
            (
                test_case
                    .expected_objects
                    .iter()
                    .map(|object| (*object).to_owned())
                    .collect::<Vec<String>>(),
                test_case.expected_sql.to_vec()
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_short_max_age_when_planning_load_jobs_then_the_plan_says_the_minute_pacing_binds()
-> Result<(), String> {
    let line = "    loads at most every 60s, not every max_age 10s: BigQuery takes 1,500 load jobs per table a day, so one starts at most once a minute; max_age = \"5m\" is recommended";
    let test_cases = [
        LoadPacingTestCase {
            description: "a load job route with max_age under a minute is paced",
            staging: "[staging]\nkind = \"internal\"\n[batching]\nmax_age = \"10s\"\n",
            expected_line: Some(line),
        },
        LoadPacingTestCase {
            description: "a topic override under a minute binds too",
            staging: "[staging]\nkind = \"gcs\"\n[staging.gcs]\nbucket = \"b\"\n[batching]\nmax_age = \"5m\"\n[[topics]]\nmatch = \"shop.*\"\nmax_age = \"10s\"\n",
            expected_line: Some(line),
        },
        LoadPacingTestCase {
            description: "the default minute is not under the pacing",
            staging: "[staging]\nkind = \"internal\"\n",
            expected_line: None,
        },
        LoadPacingTestCase {
            description: "the Write API is not paced",
            staging: "[staging]\nkind = \"none\"\n[batching]\nmax_age = \"10s\"\n",
            expected_line: None,
        },
    ];
    for test_case in &test_cases {
        let config = parse_config(&format!("{BIGQUERY}{}", test_case.staging), &|_| None)
            .map_err(|error| error.to_string())?;
        let streams = resolve_streams(&config, &["shop.orders".to_owned()])
            .map_err(|problems| problems.join("; "))?;
        let plan = build_plan(&config, &streams, &ExistingObjects::default(), false);
        let lines: Vec<String> = plan
            .streams
            .iter()
            .filter_map(|stream| load_pacing(&config, stream))
            .collect();
        assert_eq!(
            lines.first().map(String::as_str),
            test_case.expected_line,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
