use sqlload_testkit::clickhouse::constants::{
    PASSWORD as CLICKHOUSE_PASSWORD, USER as CLICKHOUSE_USER,
};
use sqlload_testkit::clickhouse::main::run_client::run_client;
use sqlload_testkit::clickhouse::main::start_clickhouse::start_clickhouse;
use sqlload_testkit::postgres::constants::PASSWORD;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::run_async;
use crate::merge_helpers::{READINGS_SETUP, run_dedupe_case, run_dedupe_rules};
use crate::test_types::{DedupeRefusalTestCase, DedupeRule, DedupeTargetTestCase};

#[test]
fn given_a_source_repeating_a_merge_key_when_loading_into_each_target_then_one_row_per_key_stays_by_the_chosen_rule()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let clickhouse = start_clickhouse().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(READINGS_SETUP)
            .await
            .map_err(|error| error.to_string())?;
        run_client(&clickhouse, "CREATE DATABASE dedupe", &[]).await?;
        let test_cases = [
            DedupeTargetTestCase {
                description: "DuckDB, through MERGE INTO",
                connection: "kind = \"duckdb\"\ndatabase = \":memory:\"".to_owned(),
                schema: "raw",
                reset: "SELECT 1",
                check_query: "SELECT COALESCE(string_agg(COALESCE(sensor, 'NULL') || ':' || CAST(amount AS VARCHAR), ',' ORDER BY sensor NULLS LAST, amount), '') FROM raw.app__readings",
                expected_rows: [
                    "s1:20,s2:1,s3:4,NULL:7,NULL:8",
                    "s1:30,s2:5,s3:4,NULL:7,NULL:8",
                    "s1:10,s2:5,s3:4,NULL:7,NULL:8",
                    "",
                ],
            },
            DedupeTargetTestCase {
                description: "Postgres, through INSERT ... ON CONFLICT",
                connection: format!(
                    "kind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"sqlload\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"",
                    postgres.port
                ),
                schema: "dedupe",
                reset: "DROP TABLE IF EXISTS dedupe.app__readings",
                check_query: "SELECT COALESCE(string_agg(COALESCE(sensor, 'NULL') || ':' || amount, ',' ORDER BY sensor NULLS LAST, amount), '') FROM dedupe.app__readings",
                expected_rows: [
                    "s1:20,s2:1,s3:4,NULL:7,NULL:8",
                    "s1:30,s2:5,s3:4,NULL:7,NULL:8",
                    "s1:10,s2:5,s3:4,NULL:7,NULL:8",
                    "",
                ],
            },
            DedupeTargetTestCase {
                description: "ClickHouse, through its delta of replaced keys",
                connection: format!(
                    "kind = \"clickhouse\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"{CLICKHOUSE_USER}\"\npassword = \"{CLICKHOUSE_PASSWORD}\"",
                    clickhouse.port
                ),
                schema: "dedupe",
                reset: "DROP TABLE IF EXISTS dedupe.app__readings",
                check_query: "SELECT arrayStringConcat(arrayMap(row -> if(row.1, 'NULL', row.2) || ':' || toString(row.3), arraySort(groupArray((isNull(sensor), ifNull(sensor, ''), amount)))), ',') FROM dedupe.app__readings",
                expected_rows: [
                    "s1:20,s2:1,s3:4,NULL:7,NULL:8",
                    "s1:30,s2:5,s3:4,NULL:7,NULL:8",
                    "s1:10,s2:5,s3:4,NULL:7,NULL:8",
                    "",
                ],
            },
        ];
        for test_case in &test_cases {
            let rows = run_dedupe_rules(test_case, postgres.port).await?;
            assert_eq!(rows, test_case.expected_rows, "{}", test_case.description);
        }
        Ok(())
    })
}

#[test]
fn given_a_dedup_sort_column_it_cannot_rank_by_when_planning_then_plan_refuses_the_table()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(READINGS_SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let test_cases = [
            DedupeRefusalTestCase {
                description: "a masked column",
                rule: DedupeRule {
                    description: "a masked column",
                    settings: "dedup_sort = { seen_at = \"desc\" }\nmask = { seen_at = \"hash\" }",
                    expected_error: "dedup_sort column seen_at is masked, so its values cannot rank rows",
                    expected_plan: "",
                },
                expected_tables: "0",
            },
            DedupeRefusalTestCase {
                description: "a column the source does not have",
                rule: DedupeRule {
                    description: "a column the source does not have",
                    settings: "dedup_sort = { read_at = \"desc\" }",
                    expected_error: "dedup_sort column read_at is not written to the table",
                    expected_plan: "",
                },
                expected_tables: "0",
            },
            DedupeRefusalTestCase {
                description: "an excluded column",
                rule: DedupeRule {
                    description: "an excluded column",
                    settings: "dedup_sort = { seen_at = \"desc\" }\nexclude_columns = [\"seen_at\"]",
                    expected_error: "dedup_sort column seen_at is not written to the table",
                    expected_plan: "",
                },
                expected_tables: "0",
            },
        ];
        for test_case in &test_cases {
            let rows = run_dedupe_case(
                ("kind = \"duckdb\"\ndatabase = \":memory:\"", "raw"),
                postgres.port,
                (
                    &["SELECT 1"],
                    "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'app__readings'",
                ),
                &test_case.rule,
            )
            .await?;
            assert_eq!(
                rows.as_deref(),
                Some(test_case.expected_tables),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
