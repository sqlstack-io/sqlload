use sqlload_testkit::clickhouse::constants::{
    PASSWORD as CLICKHOUSE_PASSWORD, USER as CLICKHOUSE_USER,
};
use sqlload_testkit::clickhouse::main::run_client::run_client;
use sqlload_testkit::clickhouse::main::start_clickhouse::start_clickhouse;
use sqlload_testkit::postgres::constants::PASSWORD;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;
use sqlload_testkit::postgres::main::start_postgres_version::start_postgres_version;

use crate::helpers::{run_async, words};
use crate::merge_helpers::{
    READINGS_SETUP, replace_key_outcome, run_dedupe_case, run_dedupe_rules, run_null_key_loads,
    run_replace_key_loads,
};
use crate::test_types::{
    DedupeRefusalTestCase, DedupeRule, DedupeTargetTestCase, NullKeyTargetTestCase,
    ReplaceKeyRefusalTestCase, ReplaceKeyTargetTestCase,
};

/// What a merge on `(company, item, site)` holds after each of three loads, the source's `qty` growing by 10 between them: one row per key, NULL parts included, each with the latest load's value, and the empty key (`/`) apart from the NULL one.
const NULL_KEY_ROWS: [&str; 3] = [
    "/:4,bolt/s1:1,bolt/NULL:2,NULL/NULL:3",
    "/:14,bolt/s1:11,bolt/NULL:12,NULL/NULL:13",
    "/:24,bolt/s1:21,bolt/NULL:22,NULL/NULL:23",
];

/// [`NULL_KEY_ROWS`] on Postgres 15, followed by the unique index SQLLoad created, which treats NULLs as equal.
const POSTGRES_NULL_KEY_ROWS: [&str; 3] = [
    "/:4,bolt/s1:1,bolt/NULL:2,NULL/NULL:3 | CREATE UNIQUE INDEX app__stock_sqlload_key ON nullkeys.app__stock USING btree (company, item, site) NULLS NOT DISTINCT",
    "/:14,bolt/s1:11,bolt/NULL:12,NULL/NULL:13 | CREATE UNIQUE INDEX app__stock_sqlload_key ON nullkeys.app__stock USING btree (company, item, site) NULLS NOT DISTINCT",
    "/:24,bolt/s1:21,bolt/NULL:22,NULL/NULL:23 | CREATE UNIQUE INDEX app__stock_sqlload_key ON nullkeys.app__stock USING btree (company, item, site) NULLS NOT DISTINCT",
];

/// What `delete_insert` with `replace_key = ["le_version"]` and no cursor holds after each load, each reading the whole source: P01 replaced whole (line C gone), P00 and P02 replaced by the same rows, and the line without a version replaced by the new one, a NULL matching a NULL.
const REPLACE_KEY_ROWS: [&str; 2] = [
    "LE-FY27-P00/Z:0,LE-FY27-P01/A:1,LE-FY27-P01/B:2,LE-FY27-P01/C:3,LE-FY27-P02/X:10,NULL/N:7",
    "LE-FY27-P00/Z:0,LE-FY27-P01/A:100,LE-FY27-P01/B:200,LE-FY27-P02/X:10,NULL/M:9",
];

const REPLACE_KEY_PLAN: &str = "delete_insert (replace_key: le_version; NULLs in keys match)";

#[test]
fn given_a_merge_key_with_null_parts_when_loading_three_times_into_each_target_then_each_key_keeps_one_row_with_the_latest_values()
-> Result<(), String> {
    run_async(|| async move {
        let source = start_postgres().await?;
        let client = connect(&source).await?;
        let postgres = start_postgres_version("15-alpine").await?;
        let clickhouse = start_clickhouse().await?;
        run_client(&clickhouse, "CREATE DATABASE nullkeys", &[]).await?;
        let test_cases = [
            NullKeyTargetTestCase {
                description: "DuckDB, through MERGE INTO",
                connection: "kind = \"duckdb\"\ndatabase = \":memory:\"".to_owned(),
                schema: "raw",
                reset: &["SELECT 1"],
                check_query: "SELECT string_agg(COALESCE(item, 'NULL') || '/' || COALESCE(site, 'NULL') || ':' || CAST(qty AS VARCHAR), ',' ORDER BY item NULLS LAST, site NULLS LAST, qty) FROM raw.app__stock",
                expected_rows: Ok(NULL_KEY_ROWS),
            },
            NullKeyTargetTestCase {
                description: "Postgres 15, the first version whose unique index can treat NULLs as equal, through INSERT ... ON CONFLICT on the index SQLLoad creates NULLS NOT DISTINCT",
                connection: format!(
                    "kind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"sqlload\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"",
                    postgres.port
                ),
                schema: "nullkeys",
                reset: &["DROP TABLE IF EXISTS nullkeys.app__stock"],
                check_query: "SELECT string_agg(COALESCE(item, 'NULL') || '/' || COALESCE(site, 'NULL') || ':' || qty, ',' ORDER BY item NULLS LAST, site NULLS LAST, qty) || ' | ' || (SELECT indexdef FROM pg_indexes WHERE schemaname = 'nullkeys' AND tablename = 'app__stock') FROM nullkeys.app__stock",
                expected_rows: Ok(POSTGRES_NULL_KEY_ROWS),
            },
            NullKeyTargetTestCase {
                description: "ClickHouse, through its delta of replaced keys",
                connection: format!(
                    "kind = \"clickhouse\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"{CLICKHOUSE_USER}\"\npassword = \"{CLICKHOUSE_PASSWORD}\"",
                    clickhouse.port
                ),
                schema: "nullkeys",
                reset: &["DROP TABLE IF EXISTS nullkeys.app__stock"],
                check_query: "SELECT arrayStringConcat(arrayMap(row -> if(row.1, 'NULL', row.2) || '/' || if(row.3, 'NULL', row.4) || ':' || toString(row.5), arraySort(groupArray((isNull(item), ifNull(item, ''), isNull(site), ifNull(site, ''), qty)))), ',') FROM nullkeys.app__stock",
                expected_rows: Ok(NULL_KEY_ROWS),
            },
            NullKeyTargetTestCase {
                description: "Postgres, into a table made by hand whose unique index on the key treats NULLs as distinct: ON CONFLICT would insert a NULL key again on every load, so plan refuses it, naming the index to create",
                connection: format!(
                    "kind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"sqlload\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"",
                    postgres.port
                ),
                schema: "nullkeys",
                reset: &[
                    "DROP TABLE IF EXISTS nullkeys.app__stock",
                    "CREATE TABLE nullkeys.app__stock (company text, item text, site text, qty bigint)",
                    "CREATE UNIQUE INDEX app__stock_key ON nullkeys.app__stock (company, item, site)",
                ],
                check_query: "SELECT 1",
                expected_rows: Err(
                    "its unique index on (company, item, site), nullkeys.app__stock_key, treats NULLs as distinct, so INSERT ... ON CONFLICT would insert a row with a NULL key again on every load",
                ),
            },
        ];
        for test_case in &test_cases {
            let rows = run_null_key_loads(test_case, (&client, source.port)).await;
            assert_eq!(
                rows.as_deref()
                    .map(|rows| rows.to_vec())
                    .map_err(|error| test_case
                        .expected_rows
                        .err()
                        .is_some_and(|expected| error.contains(expected))),
                test_case
                    .expected_rows
                    .map(|rows| rows.iter().map(|row| (*row).to_owned()).collect())
                    .map_err(|_| true),
                "{}: {rows:?}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_source_repeating_a_merge_key_when_loading_into_each_target_then_one_row_per_key_stays_by_the_chosen_rule()
-> Result<(), String> {
    run_async(|| async move {
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
                    "s1:20,s2:1,s3:4,NULL:8",
                    "s1:30,s2:5,s3:4,NULL:8",
                    "s1:30,s2:5,s3:4,NULL:8",
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
                    "s1:20,s2:1,s3:4,NULL:8",
                    "s1:30,s2:5,s3:4,NULL:8",
                    "s1:30,s2:5,s3:4,NULL:8",
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
                    "s1:20,s2:1,s3:4,NULL:8",
                    "s1:30,s2:5,s3:4,NULL:8",
                    "s1:30,s2:5,s3:4,NULL:8",
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
fn given_a_newest_by_column_it_cannot_compare_when_planning_then_plan_refuses_the_table()
-> Result<(), String> {
    run_async(|| async move {
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
                    settings: "newest_by = \"seen_at\"\nmask = { seen_at = \"hash\" }",
                    expected_error: "newest_by column seen_at is masked, so its values cannot say which row is newer",
                    expected_plan: "",
                },
                expected_tables: "0",
            },
            DedupeRefusalTestCase {
                description: "a JSON column, which has no order",
                rule: DedupeRule {
                    description: "a JSON column, which has no order",
                    settings: "newest_by = [\"seen_at\", \"meta\"]",
                    expected_error: "newest_by column meta is json, which has no order to say which row is newer",
                    expected_plan: "",
                },
                expected_tables: "0",
            },
            DedupeRefusalTestCase {
                description: "a column the source does not have",
                rule: DedupeRule {
                    description: "a column the source does not have",
                    settings: "newest_by = \"read_at\"",
                    expected_error: "newest_by column \"read_at\" is not a column of the table",
                    expected_plan: "",
                },
                expected_tables: "0",
            },
            DedupeRefusalTestCase {
                description: "an excluded column",
                rule: DedupeRule {
                    description: "an excluded column",
                    settings: "newest_by = \"seen_at\"\nexclude_columns = [\"seen_at\"]",
                    expected_error: "newest_by column seen_at is not written to the table",
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

#[test]
fn given_a_database_source_with_a_replace_key_and_no_cursor_when_loading_into_each_target_then_every_row_of_the_read_versions_is_replaced()
-> Result<(), String> {
    run_async(|| async move {
        let source = start_postgres().await?;
        let client = connect(&source).await?;
        let clickhouse = start_clickhouse().await?;
        run_client(&clickhouse, "CREATE DATABASE replacekeys", &[]).await?;
        let test_cases = [
            ReplaceKeyTargetTestCase {
                description: "DuckDB, through DELETE ... USING the staged keys, then INSERT, in one transaction",
                connection: "kind = \"duckdb\"\ndatabase = \":memory:\"".to_owned(),
                schema: "raw",
                reset: &["SELECT 1"],
                check_query: "SELECT string_agg(COALESCE(le_version, 'NULL') || '/' || line || ':' || CAST(amount AS VARCHAR), ',' ORDER BY le_version NULLS LAST, line) FROM raw.app__estimates",
                expected_plan: REPLACE_KEY_PLAN,
                expected_rows: REPLACE_KEY_ROWS,
            },
            ReplaceKeyTargetTestCase {
                description: "Postgres, through DELETE ... USING the staged keys, then INSERT, in one transaction",
                connection: format!(
                    "kind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"sqlload\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"",
                    source.port
                ),
                schema: "replacekeys",
                reset: &["DROP TABLE IF EXISTS replacekeys.app__estimates"],
                check_query: "SELECT string_agg(COALESCE(le_version, 'NULL') || '/' || line || ':' || amount, ',' ORDER BY le_version NULLS LAST, line) FROM replacekeys.app__estimates",
                expected_plan: REPLACE_KEY_PLAN,
                expected_rows: REPLACE_KEY_ROWS,
            },
            ReplaceKeyTargetTestCase {
                description: "ClickHouse, through its delta displacing the rows of the delta's keys",
                connection: format!(
                    "kind = \"clickhouse\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"{CLICKHOUSE_USER}\"\npassword = \"{CLICKHOUSE_PASSWORD}\"",
                    clickhouse.port
                ),
                schema: "replacekeys",
                reset: &["DROP TABLE IF EXISTS replacekeys.app__estimates"],
                check_query: "SELECT arrayStringConcat(arrayMap(row -> if(row.1, 'NULL', row.2) || '/' || row.3 || ':' || toString(row.4), arraySort(groupArray((isNull(le_version), ifNull(le_version, ''), line, amount)))), ',') FROM replacekeys.app__estimates",
                expected_plan: REPLACE_KEY_PLAN,
                expected_rows: REPLACE_KEY_ROWS,
            },
        ];
        for test_case in &test_cases {
            let rows = run_replace_key_loads(test_case, (&client, source.port)).await?;
            assert_eq!(rows, test_case.expected_rows, "{}", test_case.description);
        }
        Ok(())
    })
}

#[test]
fn given_a_cursor_column_with_a_replace_key_on_a_database_source_when_planning_then_plan_refuses_it_naming_the_full_read()
-> Result<(), String> {
    run_async(|| async move {
        let source = start_postgres().await?;
        let client = connect(&source).await?;
        let test_cases = [
            ReplaceKeyRefusalTestCase {
                description: "a cursor would read part of a version's rows while the commit deletes all of them, so plan refuses it and creates no table",
                settings: "cursor_column = \"updated_at\"",
                expected_error: "cursor_column updated_at does not apply to delete_insert by replace_key: a load deletes every row of each key value it reads, so the rows of a key value outside the cursor range, which it never reads, would be lost",
                expected_fix: "drop cursor_column, so every load reads the whole source and replaces each key value it holds; or use write_strategy = \"merge\" on a unique_key to read by the cursor",
                expected_tables: "0",
            },
            ReplaceKeyRefusalTestCase {
                description: "without a cursor the table is read whole and loads",
                settings: "",
                expected_error: "",
                expected_fix: "",
                expected_tables: "1",
            },
        ];
        for test_case in &test_cases {
            let (error, tables) =
                replace_key_outcome((&client, source.port), test_case.settings).await?;
            assert_eq!(
                (
                    words(&error).contains(&words(test_case.expected_error))
                        && words(&error).contains(&words(test_case.expected_fix)),
                    error.is_empty(),
                    tables.as_deref(),
                ),
                (
                    true,
                    test_case.expected_error.is_empty(),
                    Some(test_case.expected_tables)
                ),
                "{}: {error}",
                test_case.description
            );
        }
        Ok(())
    })
}
