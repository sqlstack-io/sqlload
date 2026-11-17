use std::collections::BTreeMap;

use crate::config::main::parse_config::parse_config;
use crate::naming::main::resolve_streams::resolve_streams;
use crate::planning::main::build_plan::build_plan;
use crate::planning::models::ExistingObjects;
use crate::planning::tests::helpers::object_summary;
use crate::planning::tests::test_types::DatabricksPlanTestCase;

const DATABRICKS: &str = "[source]\nkind = \"kafka\"\nbrokers = \"b\"\ninclude = [\"*\"]\n[destination]\nkind = \"databricks\"\nhost = \"h\"\nclient_id = \"i\"\nclient_secret = \"s\"\nwarehouse_id = \"w\"\ncatalog = \"main\"\nschema = \"raw\"\nzerobus_endpoint = \"https://z\"\n";

const ENVELOPE: &[(&str, &str)] = &[
    ("topic", "string"),
    ("kafka_timestamp", "bigint"),
    ("kafka_timestamp_type", "int"),
    ("kafka_offset", "bigint"),
    ("kafka_partition", "int"),
    ("key", "string"),
    ("value", "string"),
    ("exported_at", "timestamp"),
    ("date", "date"),
];

#[test]
fn given_databricks_routes_when_planning_then_tables_and_volumes_are_lower_case()
-> Result<(), String> {
    let test_cases = [
        DatabricksPlanTestCase {
            description: "Zerobus Ingest needs only the table",
            staging: "[staging]\nkind = \"none\"\n",
            existing_columns: &[],
            expected_objects: &["+table shop__orders"],
            expected_sql: &["CREATE TABLE IF NOT EXISTS `main`.`raw`.`shop__orders` (`topic` STRING, `kafka_timestamp` BIGINT, `kafka_timestamp_type` INT, `kafka_offset` BIGINT, `kafka_partition` INT, `key` STRING, `value` STRING, `exported_at` TIMESTAMP, `date` DATE)"],
        },
        DatabricksPlanTestCase {
            description: "a volume route also needs the stream's volume",
            staging: "[staging]\nkind = \"internal\"\n",
            existing_columns: &[],
            expected_objects: &["+table shop__orders", "+volume shop__orders_sqlload_stage"],
            expected_sql: &[
                "CREATE TABLE IF NOT EXISTS `main`.`raw`.`shop__orders` (`topic` STRING, `kafka_timestamp` BIGINT, `kafka_timestamp_type` INT, `kafka_offset` BIGINT, `kafka_partition` INT, `key` STRING, `value` STRING, `exported_at` TIMESTAMP, `date` DATE)",
                "CREATE VOLUME IF NOT EXISTS `main`.`raw`.`shop__orders_sqlload_stage`",
            ],
        },
        DatabricksPlanTestCase {
            description: "an existing table matches by name and type family, whatever the case",
            staging: "[staging]\nkind = \"s3\"\n[staging.s3]\nbucket = \"b\"\nregion = \"r\"\n",
            existing_columns: ENVELOPE,
            expected_objects: &["=table shop__orders"],
            expected_sql: &[],
        },
        DatabricksPlanTestCase {
            description: "a column of another family is incompatible",
            staging: "[staging]\nkind = \"none\"\n",
            existing_columns: &[("topic", "string"), ("kafka_offset", "string")],
            expected_objects: &["!table shop__orders"],
            expected_sql: &[],
        },
    ];
    for test_case in &test_cases {
        let config = parse_config(&format!("{DATABRICKS}{}", test_case.staging), &|_| None)
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

