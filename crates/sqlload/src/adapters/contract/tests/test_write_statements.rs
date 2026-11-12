use crate::adapters::contract::models::{MergeDedupe, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::tests::helpers::{accounts, columns};
use crate::adapters::contract::tests::test_types::WriteStatementsTestCase;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;

const SF_INSERT: &str = "INSERT INTO \"RAW\".\"APP__ACCOUNTS\" (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") SELECT \"ID\", PARSE_JSON(\"PROFILE\"), \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"";
const DUCK_INSERT: &str = "INSERT INTO \"raw\".\"app__accounts\" (\"id\", \"profile\", \"updated_at\", \"_sqlload_load_id\", \"_sqlload_loaded_at\") SELECT \"id\", CAST(\"profile\" AS JSON), \"updated_at\", \"_sqlload_load_id\", \"_sqlload_loaded_at\" FROM \"raw\".\"app__accounts__sqlload_staging_load1\"";

const SF_SWAP_INSERT: &str = "INSERT INTO \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1_SWAP\" (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") SELECT \"ID\", PARSE_JSON(\"PROFILE\"), \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"";

const BQ_INSERT: &str = "INSERT INTO `raw`.`app__accounts` (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) SELECT `id`, PARSE_JSON(`profile`), `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`";

const CH_DELTA_INSERT: &str = "-- sqlload publish {\"SortedInsert\":{\"database\":\"raw\",\"table\":\"app__accounts__sqlload_staging_load1_swap\",\"columns\":[\"id\",\"profile\",\"updated_at\",\"_sqlload_load_id\",\"_sqlload_loaded_at\"],\"insert\":\"INSERT INTO `raw`.`app__accounts__sqlload_staging_load1_swap` (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) SELECT `id` AS `id`, CAST(`profile` AS Nullable(String)) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`\"}}";

const DB_INSERT: &str = "INSERT INTO `raw`.`app__accounts` (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) SELECT `id`, parse_json(`profile`), `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`";

#[test]
fn given_a_write_mode_when_building_statements_then_each_write_is_atomic() {
    let test_cases = [
        WriteStatementsTestCase {
            description: "append inserts only",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Append,
            expected_statements: &["BEGIN", SF_INSERT, "COMMIT"],
        },
        WriteStatementsTestCase {
            description: "merge is one MERGE that updates matches and inserts the rest",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
                dedupe: None,
            },
            expected_statements: &[
                "MERGE INTO \"RAW\".\"APP__ACCOUNTS\" AS __target USING (SELECT \"ID\" AS \"ID\", PARSE_JSON(\"PROFILE\") AS \"PROFILE\", \"UPDATED_AT\" AS \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" AS \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" AS \"_SQLLOAD_LOADED_AT\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\") AS __source ON __target.\"ID\" = __source.\"ID\" WHEN MATCHED THEN UPDATE SET \"PROFILE\" = __source.\"PROFILE\", \"UPDATED_AT\" = __source.\"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" = __source.\"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" = __source.\"_SQLLOAD_LOADED_AT\" WHEN NOT MATCHED THEN INSERT (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") VALUES (__source.\"ID\", __source.\"PROFILE\", __source.\"UPDATED_AT\", __source.\"_SQLLOAD_LOAD_ID\", __source.\"_SQLLOAD_LOADED_AT\")",
            ],
        },
        WriteStatementsTestCase {
            description: "merge with dedupe keeps one staged row per non-NULL key, ranked on the converted values: the highest dedup_sort value, NULLs last, then the later row",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
                dedupe: Some(MergeDedupe {
                    ordinal: "_sqlload_row".to_owned(),
                    sort: Some(("updated_at".to_owned(), true)),
                }),
            },
            expected_statements: &[
                "MERGE INTO \"RAW\".\"APP__ACCOUNTS\" AS __target USING (SELECT \"ID\" AS \"ID\", \"PROFILE\" AS \"PROFILE\", \"UPDATED_AT\" AS \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" AS \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" AS \"_SQLLOAD_LOADED_AT\" FROM (SELECT \"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" FROM (SELECT \"ID\" AS \"ID\", PARSE_JSON(\"PROFILE\") AS \"PROFILE\", \"UPDATED_AT\" AS \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" AS \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" AS \"_SQLLOAD_LOADED_AT\", ROW_NUMBER() OVER (PARTITION BY \"ID\" ORDER BY \"UPDATED_AT\" DESC NULLS LAST, \"_SQLLOAD_ROW\" DESC) AS __sqlload_rank FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\") AS __sqlload_ranked WHERE __sqlload_rank = 1 OR \"ID\" IS NULL) AS __sqlload_kept) AS __source ON __target.\"ID\" = __source.\"ID\" WHEN MATCHED THEN UPDATE SET \"PROFILE\" = __source.\"PROFILE\", \"UPDATED_AT\" = __source.\"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" = __source.\"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" = __source.\"_SQLLOAD_LOADED_AT\" WHEN NOT MATCHED THEN INSERT (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") VALUES (__source.\"ID\", __source.\"PROFILE\", __source.\"UPDATED_AT\", __source.\"_SQLLOAD_LOAD_ID\", __source.\"_SQLLOAD_LOADED_AT\")",
            ],
        },
        WriteStatementsTestCase {
            description: "merge with dedupe and no dedup_sort on BigQuery keeps the last staged row per key",
            warehouse: Warehouse::BigQuery,
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
                dedupe: Some(MergeDedupe {
                    ordinal: "_sqlload_row".to_owned(),
                    sort: None,
                }),
            },
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, `profile` AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM (SELECT `id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at` FROM (SELECT `id` AS `id`, PARSE_JSON(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at`, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `_sqlload_row` DESC) AS __sqlload_rank FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __sqlload_ranked WHERE __sqlload_rank = 1 OR `id` IS NULL) AS __sqlload_kept) AS __source ON __target.`id` = __source.`id` WHEN MATCHED THEN UPDATE SET `profile` = __source.`profile`, `updated_at` = __source.`updated_at`, `_sqlload_load_id` = __source.`_sqlload_load_id`, `_sqlload_loaded_at` = __source.`_sqlload_loaded_at` WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`)",
            ],
        },
        WriteStatementsTestCase {
            description: "merge with dedupe on Databricks keeps the lowest dedup_sort value per key",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
                dedupe: Some(MergeDedupe {
                    ordinal: "_sqlload_source_row".to_owned(),
                    sort: Some(("updated_at".to_owned(), false)),
                }),
            },
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, `profile` AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM (SELECT `id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at` FROM (SELECT `id` AS `id`, parse_json(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at`, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `updated_at` ASC NULLS LAST, `_sqlload_source_row` DESC) AS __sqlload_rank FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __sqlload_ranked WHERE __sqlload_rank = 1 OR `id` IS NULL) AS __sqlload_kept) AS __source ON __target.`id` = __source.`id` WHEN MATCHED THEN UPDATE SET `profile` = __source.`profile`, `updated_at` = __source.`updated_at`, `_sqlload_load_id` = __source.`_sqlload_load_id`, `_sqlload_loaded_at` = __source.`_sqlload_loaded_at` WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`)",
            ],
        },
        WriteStatementsTestCase {
            description: "merge with dedupe on ClickHouse fills the delta with one row per key, so the delta replaces each key once",
            warehouse: Warehouse::ClickHouse,
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
                dedupe: Some(MergeDedupe {
                    ordinal: "_sqlload_row".to_owned(),
                    sort: None,
                }),
            },
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`app__accounts__sqlload_staging_load1_swap`",
                "CREATE TABLE `raw`.`app__accounts__sqlload_staging_load1_swap` AS `raw`.`app__accounts`",
                "-- sqlload publish {\"SortedInsert\":{\"database\":\"raw\",\"table\":\"app__accounts__sqlload_staging_load1_swap\",\"columns\":[\"id\",\"profile\",\"updated_at\",\"_sqlload_load_id\",\"_sqlload_loaded_at\"],\"insert\":\"INSERT INTO `raw`.`app__accounts__sqlload_staging_load1_swap` (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) SELECT `id` AS `id`, `profile` AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM (SELECT `id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at` FROM (SELECT `id` AS `id`, CAST(`profile` AS Nullable(String)) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at`, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `_sqlload_row` DESC) AS __sqlload_rank FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __sqlload_ranked WHERE __sqlload_rank = 1 OR `id` IS NULL) AS __sqlload_kept\"}}",
                "-- sqlload publish {\"Partitions\":{\"target\":\"`raw`.`app__accounts`\",\"delta\":\"`raw`.`app__accounts__sqlload_staging_load1_swap`\",\"copy\":\"`raw`.`app__accounts__sqlload_staging_load1_swap_copy`\",\"aside\":\"`raw`.`app__accounts__sqlload_staging_load1_swap_copy_aside`\",\"displaced\":\"(`id`) IN (SELECT `id` FROM `raw`.`app__accounts__sqlload_staging_load1_swap`)\"}}",
                "DROP TABLE IF EXISTS `raw`.`app__accounts__sqlload_staging_load1_swap`",
            ],
        },
        WriteStatementsTestCase {
            description: "delete_insert replaces the cursor window",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: Some("2026-11-04T09:00:00Z".to_owned()),
                to: None,
                values: Vec::new(),
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"RAW\".\"APP__ACCOUNTS\" WHERE \"UPDATED_AT\" >= CAST('2026-11-04T09:00:00Z' AS TIMESTAMP_TZ(9))",
                SF_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "a text cursor's window is deleted by code point on Snowflake, whatever the column's collation",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::DeleteInsert {
                cursor_column: "code".to_owned(),
                from: Some("B1".to_owned()),
                to: Some("a9".to_owned()),
                values: Vec::new(),
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"RAW\".\"APP__ACCOUNTS\" WHERE COLLATE(\"CODE\", 'utf8') >= CAST('B1' AS VARCHAR) AND COLLATE(\"CODE\", 'utf8') < CAST('a9' AS VARCHAR)",
                SF_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "a text cursor's listed values are deleted by code point on Postgres",
            warehouse: Warehouse::Postgres,
            mode: WriteMode::DeleteInsert {
                cursor_column: "code".to_owned(),
                from: None,
                to: None,
                values: vec!["B1".to_owned()],
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"raw\".\"app__accounts\" WHERE \"code\" COLLATE \"C\" IN (CAST('B1' AS text))",
                "INSERT INTO \"raw\".\"app__accounts\" (\"id\", \"profile\", \"updated_at\", \"_sqlload_load_id\", \"_sqlload_loaded_at\") SELECT \"id\", CAST(\"profile\" AS jsonb), \"updated_at\", \"_sqlload_load_id\", \"_sqlload_loaded_at\" FROM \"pg_temp\".\"app__accounts__sqlload_staging_load1\"",
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "a text cursor's window on DuckDB, which compares bytewise already, is deleted as it is",
            warehouse: Warehouse::DuckDb,
            mode: WriteMode::DeleteInsert {
                cursor_column: "code".to_owned(),
                from: Some("B1".to_owned()),
                to: None,
                values: Vec::new(),
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"raw\".\"app__accounts\" WHERE \"code\" >= CAST('B1' AS VARCHAR)",
                DUCK_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "replace on Snowflake swaps in a copy that keeps the grants",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Replace,
            expected_statements: &[
                "CREATE OR REPLACE TABLE \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1_SWAP\" LIKE \"RAW\".\"APP__ACCOUNTS\" COPY GRANTS",
                SF_SWAP_INSERT,
                "ALTER TABLE \"RAW\".\"APP__ACCOUNTS\" SWAP WITH \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1_SWAP\"",
                "DROP TABLE IF EXISTS \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1_SWAP\"",
            ],
        },
        WriteStatementsTestCase {
            description: "a backfill window bounds the delete on both sides",
            warehouse: Warehouse::DuckDb,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: Some("2026-11-01".to_owned()),
                to: Some("2026-11-02".to_owned()),
                values: Vec::new(),
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"raw\".\"app__accounts\" WHERE \"updated_at\" >= CAST('2026-11-01' AS TIMESTAMP WITH TIME ZONE) AND \"updated_at\" < CAST('2026-11-02' AS TIMESTAMP WITH TIME ZONE)",
                DUCK_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "a files source's partitions are deleted by value, never by range",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: Some("2026-11-01".to_owned()),
                to: None,
                values: vec!["2026-11-01".to_owned(), "2026-11-03".to_owned()],
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"RAW\".\"APP__ACCOUNTS\" WHERE \"UPDATED_AT\" IN (CAST('2026-11-01' AS TIMESTAMP_TZ(9)), CAST('2026-11-03' AS TIMESTAMP_TZ(9)))",
                SF_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "recreate rebuilds the table in one statement, keeping grants on Snowflake",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Recreate,
            expected_statements: &[
                "CREATE OR REPLACE TABLE \"RAW\".\"APP__ACCOUNTS\" (\"ID\" NUMBER(38,0), \"PROFILE\" VARIANT, \"UPDATED_AT\" TIMESTAMP_TZ(9), \"_SQLLOAD_LOAD_ID\" VARCHAR, \"_SQLLOAD_LOADED_AT\" TIMESTAMP_TZ(9)) COPY GRANTS AS SELECT \"ID\", PARSE_JSON(\"PROFILE\"), \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"",
            ],
        },
        WriteStatementsTestCase {
            description: "append on Databricks is one INSERT, since every statement commits alone",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::Append,
            expected_statements: &[DB_INSERT],
        },
        WriteStatementsTestCase {
            description: "delete_insert on Databricks is one MERGE inserting every staged row and deleting the window",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: Some("2026-11-04T09:00:00Z".to_owned()),
                to: Some("2026-11-05T09:00:00Z".to_owned()),
                values: Vec::new(),
            },
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, parse_json(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __source ON FALSE WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`) WHEN NOT MATCHED BY SOURCE AND __target.`updated_at` >= CAST('2026-11-04T09:00:00Z' AS TIMESTAMP) AND __target.`updated_at` < CAST('2026-11-05T09:00:00Z' AS TIMESTAMP) THEN DELETE",
            ],
        },
        WriteStatementsTestCase {
            description: "delete_insert by partition values on Databricks deletes the listed values",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: None,
                to: None,
                values: vec!["2026-11-01".to_owned(), "2026-11-03".to_owned()],
            },
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, parse_json(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __source ON FALSE WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`) WHEN NOT MATCHED BY SOURCE AND __target.`updated_at` IN (CAST('2026-11-01' AS TIMESTAMP), CAST('2026-11-03' AS TIMESTAMP)) THEN DELETE",
            ],
        },
        WriteStatementsTestCase {
            description: "an unbounded delete_insert on Databricks deletes every row it does not insert",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: None,
                to: None,
                values: Vec::new(),
            },
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, parse_json(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __source ON FALSE WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`) WHEN NOT MATCHED BY SOURCE THEN DELETE",
            ],
        },
        WriteStatementsTestCase {
            description: "replace from text rows on Databricks overwrites the table in one commit",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::Replace,
            expected_statements: &[
                "INSERT OVERWRITE `raw`.`app__accounts` (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) SELECT `id`, parse_json(`profile`), `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`",
            ],
        },
        WriteStatementsTestCase {
            description: "append on BigQuery is one INSERT, parsing JSON from its staged text",
            warehouse: Warehouse::BigQuery,
            mode: WriteMode::Append,
            expected_statements: &[BQ_INSERT],
        },
        WriteStatementsTestCase {
            description: "delete_insert on BigQuery is one MERGE inserting every staged row and deleting the window",
            warehouse: Warehouse::BigQuery,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: Some("2026-11-04T09:00:00Z".to_owned()),
                to: Some("2026-11-05T09:00:00Z".to_owned()),
                values: Vec::new(),
            },
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, PARSE_JSON(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __source ON FALSE WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`) WHEN NOT MATCHED BY SOURCE AND __target.`updated_at` >= CAST('2026-11-04T09:00:00Z' AS TIMESTAMP) AND __target.`updated_at` < CAST('2026-11-05T09:00:00Z' AS TIMESTAMP) THEN DELETE",
            ],
        },
        WriteStatementsTestCase {
            description: "replace from text rows on BigQuery, which has no INSERT OVERWRITE, is one MERGE deleting every row it does not insert",
            warehouse: Warehouse::BigQuery,
            mode: WriteMode::Replace,
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, PARSE_JSON(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __source ON FALSE WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`) WHEN NOT MATCHED BY SOURCE THEN DELETE",
            ],
        },
        WriteStatementsTestCase {
            description: "delete_insert on ClickHouse converts the staged rows into a delta in the table's shape, then publishes its partitions, replacing the window's rows",
            warehouse: Warehouse::ClickHouse,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: Some("2026-11-04 09:00:00.000000+00".to_owned()),
                to: Some("2026-11-05 09:00:00.000000+00".to_owned()),
                values: Vec::new(),
            },
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`app__accounts__sqlload_staging_load1_swap`",
                "CREATE TABLE `raw`.`app__accounts__sqlload_staging_load1_swap` AS `raw`.`app__accounts`",
                CH_DELTA_INSERT,
                "-- sqlload publish {\"Partitions\":{\"target\":\"`raw`.`app__accounts`\",\"delta\":\"`raw`.`app__accounts__sqlload_staging_load1_swap`\",\"copy\":\"`raw`.`app__accounts__sqlload_staging_load1_swap_copy`\",\"aside\":\"`raw`.`app__accounts__sqlload_staging_load1_swap_copy_aside`\",\"displaced\":\"`updated_at` >= parseDateTime64BestEffort('2026-11-04 09:00:00.000000+00', 6, 'UTC') AND `updated_at` < parseDateTime64BestEffort('2026-11-05 09:00:00.000000+00', 6, 'UTC')\"}}",
                "DROP TABLE IF EXISTS `raw`.`app__accounts__sqlload_staging_load1_swap`",
            ],
        },
        WriteStatementsTestCase {
            description: "merge on ClickHouse replaces the table's rows whose key the delta holds, partition by partition",
            warehouse: Warehouse::ClickHouse,
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
                dedupe: None,
            },
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`app__accounts__sqlload_staging_load1_swap`",
                "CREATE TABLE `raw`.`app__accounts__sqlload_staging_load1_swap` AS `raw`.`app__accounts`",
                CH_DELTA_INSERT,
                "-- sqlload publish {\"Partitions\":{\"target\":\"`raw`.`app__accounts`\",\"delta\":\"`raw`.`app__accounts__sqlload_staging_load1_swap`\",\"copy\":\"`raw`.`app__accounts__sqlload_staging_load1_swap_copy`\",\"aside\":\"`raw`.`app__accounts__sqlload_staging_load1_swap_copy_aside`\",\"displaced\":\"(`id`) IN (SELECT `id` FROM `raw`.`app__accounts__sqlload_staging_load1_swap`)\"}}",
                "DROP TABLE IF EXISTS `raw`.`app__accounts__sqlload_staging_load1_swap`",
            ],
        },
        WriteStatementsTestCase {
            description: "replace from staged rows on ClickHouse exchanges the delta in whole",
            warehouse: Warehouse::ClickHouse,
            mode: WriteMode::Replace,
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`app__accounts__sqlload_staging_load1_swap`",
                "CREATE TABLE `raw`.`app__accounts__sqlload_staging_load1_swap` AS `raw`.`app__accounts`",
                CH_DELTA_INSERT,
                "-- sqlload publish {\"Exchange\":{\"target\":\"`raw`.`app__accounts`\",\"build\":\"`raw`.`app__accounts__sqlload_staging_load1_swap`\",\"aside\":\"`raw`.`app__accounts__sqlload_staging_load1_swap_aside`\"}}",
                "DROP TABLE IF EXISTS `raw`.`app__accounts__sqlload_staging_load1_swap`",
            ],
        },
    ];
    for test_case in test_cases {
        let table = accounts();
        let columns = columns();
        let (staging, _) = sql_adapter(test_case.warehouse)
            .render_create_staging_table(&table, &columns, "load-1");
        let request = WriteStatementsRequest {
            table: &table,
            columns: &columns,
            mode: &test_case.mode,
            hashed: &[],
            built: None,
        };
        assert_eq!(
            sql_adapter(test_case.warehouse).render_write(&request, &staging),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}
