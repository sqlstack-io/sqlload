use crate::adapters::snowflake::tests::helpers::{unload_blocked_for, unload_readers_for};
use crate::adapters::snowflake::tests::test_types::{UnloadBlockedTestCase, UnloadReadersTestCase};
use crate::column_types::models::Warehouse;
use crate::loader::constants::{UNLOADED_OUTSIDE_STAGE, UNLOADED_TIMES};

const MASKS: &str = "the table masks columns";
const OUTSIDE: &str = UNLOADED_OUTSIDE_STAGE;

#[test]
fn given_a_snowflake_sources_unload_when_routing_into_snowflake_then_snowflake_copies_only_what_the_targets_external_stage_covers()
 {
    let test_cases = [
        UnloadBlockedTestCase {
            description: "an S3 folder below the target's external stage is copied by Snowflake",
            unload_to: Some("s3://lake/raw/unload"),
            external: true,
            masks: false,
            expected_blocked: None,
        },
        UnloadBlockedTestCase {
            description: "the stage's own folder, written with a slash, is covered too",
            unload_to: Some("s3://lake/raw/"),
            external: true,
            masks: false,
            expected_blocked: None,
        },
        UnloadBlockedTestCase {
            description: "another folder of the same bucket goes through SQLLoad",
            unload_to: Some("s3://lake/elsewhere/"),
            external: true,
            masks: false,
            expected_blocked: Some(OUTSIDE),
        },
        UnloadBlockedTestCase {
            description: "a folder whose name only starts like the stage's goes through SQLLoad",
            unload_to: Some("s3://lake/rawish/"),
            external: true,
            masks: false,
            expected_blocked: Some(OUTSIDE),
        },
        UnloadBlockedTestCase {
            description: "the same folder in another bucket goes through SQLLoad",
            unload_to: Some("s3://other/raw/unload/"),
            external: true,
            masks: false,
            expected_blocked: Some(OUTSIDE),
        },
        UnloadBlockedTestCase {
            description: "an S3 unload into a target without an external stage goes through SQLLoad",
            unload_to: Some("s3://lake/raw/unload/"),
            external: false,
            masks: false,
            expected_blocked: Some(OUTSIDE),
        },
        UnloadBlockedTestCase {
            description: "the source's user stage is not the target's to read",
            unload_to: None,
            external: true,
            masks: false,
            expected_blocked: Some(OUTSIDE),
        },
        UnloadBlockedTestCase {
            description: "a named stage of the source's is not the target's to read",
            unload_to: Some("@ops.loading.unload"),
            external: true,
            masks: false,
            expected_blocked: Some(OUTSIDE),
        },
        UnloadBlockedTestCase {
            description: "masks need SQLLoad to see the rows, even under the stage",
            unload_to: Some("s3://lake/raw/unload/"),
            external: true,
            masks: true,
            expected_blocked: Some(MASKS),
        },
    ];
    for test_case in &test_cases {
        assert_eq!(
            unload_blocked_for(test_case).as_deref(),
            test_case.expected_blocked,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_snowflake_sources_s3_unload_when_routing_into_another_warehouse_then_databricks_and_clickhouse_read_it_unless_times_or_masks_need_sqlload()
 {
    let test_cases = [
        UnloadReadersTestCase {
            description: "Databricks reads the unload itself",
            warehouse: Warehouse::Databricks,
            masks: false,
            column_type: "NUMBER(38,0)",
            expected_blocked: None,
        },
        UnloadReadersTestCase {
            description: "ClickHouse reads the unload itself",
            warehouse: Warehouse::ClickHouse,
            masks: false,
            column_type: "NUMBER(38,0)",
            expected_blocked: None,
        },
        UnloadReadersTestCase {
            description: "a time column goes through SQLLoad into Databricks, which keeps times as text",
            warehouse: Warehouse::Databricks,
            masks: false,
            column_type: "TIME(9)",
            expected_blocked: Some("the table has a time column"),
        },
        UnloadReadersTestCase {
            description: "a time column goes through SQLLoad into ClickHouse",
            warehouse: Warehouse::ClickHouse,
            masks: false,
            column_type: "TIME(9)",
            expected_blocked: Some("the table has a time column"),
        },
        UnloadReadersTestCase {
            description: "masks need SQLLoad to see the rows",
            warehouse: Warehouse::ClickHouse,
            masks: true,
            column_type: "NUMBER(38,0)",
            expected_blocked: Some(MASKS),
        },
        UnloadReadersTestCase {
            description: "DuckDB turns times back itself",
            warehouse: Warehouse::DuckDb,
            masks: false,
            column_type: "TIME(9)",
            expected_blocked: None,
        },
        UnloadReadersTestCase {
            description: "Postgres never reads files itself",
            warehouse: Warehouse::Postgres,
            masks: false,
            column_type: "NUMBER(38,0)",
            expected_blocked: Some(UNLOADED_TIMES),
        },
    ];
    for test_case in &test_cases {
        let blocked = unload_readers_for(test_case);
        assert_eq!(
            blocked.as_deref().map(|reason| {
                test_case
                    .expected_blocked
                    .filter(|expected| reason.starts_with(expected))
                    .unwrap_or(reason)
            }),
            test_case.expected_blocked,
            "{}",
            test_case.description
        );
    }
}
