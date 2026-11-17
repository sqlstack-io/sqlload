use crate::adapters::contract::models::{
    MergeDedupe, TableName, TombstoneWrite, WriteMode, WriteStatementsRequest,
};
use crate::adapters::contract::tests::helpers::{accounts, columns};
use crate::adapters::contract::tests::test_types::{TombstonesTestCase, WriteStatementsTestCase};
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
                not_null_keys: vec!["id".to_owned()],
                dedupe: None,
            },
            expected_statements: &[
                "MERGE INTO \"RAW\".\"APP__ACCOUNTS\" AS __target USING (SELECT \"ID\" AS \"ID\", PARSE_JSON(\"PROFILE\") AS \"PROFILE\", \"UPDATED_AT\" AS \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" AS \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" AS \"_SQLLOAD_LOADED_AT\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\") AS __source ON __target.\"ID\" = __source.\"ID\" WHEN MATCHED THEN UPDATE SET \"PROFILE\" = __source.\"PROFILE\", \"UPDATED_AT\" = __source.\"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" = __source.\"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" = __source.\"_SQLLOAD_LOADED_AT\" WHEN NOT MATCHED THEN INSERT (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") VALUES (__source.\"ID\", __source.\"PROFILE\", __source.\"UPDATED_AT\", __source.\"_SQLLOAD_LOAD_ID\", __source.\"_SQLLOAD_LOADED_AT\")",
            ],
        },
        WriteStatementsTestCase {
            description: "merge with dedupe keeps one staged row per key, ranked on the converted values: the newest by newest_by, NULLs last, then the later row, and none older than the table's row of its key",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
                not_null_keys: vec!["id".to_owned()],
                dedupe: Some(MergeDedupe {
                    ordinal: "_sqlload_row".to_owned(),
                    newest_by: vec!["updated_at".to_owned()],
                }),
            },
            expected_statements: &[
                "MERGE INTO \"RAW\".\"APP__ACCOUNTS\" AS __target USING (SELECT \"ID\" AS \"ID\", \"PROFILE\" AS \"PROFILE\", \"UPDATED_AT\" AS \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" AS \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" AS \"_SQLLOAD_LOADED_AT\" FROM (SELECT \"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" FROM (SELECT \"ID\" AS \"ID\", PARSE_JSON(\"PROFILE\") AS \"PROFILE\", \"UPDATED_AT\" AS \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" AS \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" AS \"_SQLLOAD_LOADED_AT\", ROW_NUMBER() OVER (PARTITION BY \"ID\" ORDER BY \"UPDATED_AT\" DESC NULLS LAST, \"_SQLLOAD_ROW\" DESC) AS __sqlload_rank FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\") AS __sqlload_ranked WHERE __sqlload_rank = 1 AND NOT EXISTS (SELECT 1 FROM \"RAW\".\"APP__ACCOUNTS\" AS __sqlload_newer WHERE __sqlload_newer.\"ID\" = __sqlload_ranked.\"ID\" AND ((__sqlload_newer.\"UPDATED_AT\" IS NOT NULL AND (__sqlload_ranked.\"UPDATED_AT\" IS NULL OR __sqlload_newer.\"UPDATED_AT\" > __sqlload_ranked.\"UPDATED_AT\"))))) AS __sqlload_kept) AS __source ON __target.\"ID\" = __source.\"ID\" WHEN MATCHED THEN UPDATE SET \"PROFILE\" = __source.\"PROFILE\", \"UPDATED_AT\" = __source.\"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" = __source.\"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" = __source.\"_SQLLOAD_LOADED_AT\" WHEN NOT MATCHED THEN INSERT (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") VALUES (__source.\"ID\", __source.\"PROFILE\", __source.\"UPDATED_AT\", __source.\"_SQLLOAD_LOAD_ID\", __source.\"_SQLLOAD_LOADED_AT\")",
            ],
        },
        WriteStatementsTestCase {
            description: "merge with dedupe and no newest_by on BigQuery keeps the last staged row per key",
            warehouse: Warehouse::BigQuery,
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
                not_null_keys: vec!["id".to_owned()],
                dedupe: Some(MergeDedupe {
                    ordinal: "_sqlload_row".to_owned(),
                    newest_by: Vec::new(),
                }),
            },
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, `profile` AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM (SELECT `id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at` FROM (SELECT `id` AS `id`, PARSE_JSON(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at`, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `_sqlload_row` DESC) AS __sqlload_rank FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __sqlload_ranked WHERE __sqlload_rank = 1) AS __sqlload_kept) AS __source ON __target.`id` = __source.`id` WHEN MATCHED THEN UPDATE SET `profile` = __source.`profile`, `updated_at` = __source.`updated_at`, `_sqlload_load_id` = __source.`_sqlload_load_id`, `_sqlload_loaded_at` = __source.`_sqlload_loaded_at` WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`)",
            ],
        },
        WriteStatementsTestCase {
            description: "merge with dedupe on Databricks keeps the newest row per key, unless the table's is newer",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
                not_null_keys: vec!["id".to_owned()],
                dedupe: Some(MergeDedupe {
                    ordinal: "_sqlload_source_row".to_owned(),
                    newest_by: vec!["updated_at".to_owned()],
                }),
            },
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, `profile` AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM (SELECT `id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at` FROM (SELECT `id` AS `id`, parse_json(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at`, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `updated_at` DESC NULLS LAST, `_sqlload_source_row` DESC) AS __sqlload_rank FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __sqlload_ranked WHERE __sqlload_rank = 1 AND NOT EXISTS (SELECT 1 FROM `raw`.`app__accounts` AS __sqlload_newer WHERE __sqlload_newer.`id` = __sqlload_ranked.`id` AND ((__sqlload_newer.`updated_at` IS NOT NULL AND (__sqlload_ranked.`updated_at` IS NULL OR __sqlload_newer.`updated_at` > __sqlload_ranked.`updated_at`))))) AS __sqlload_kept) AS __source ON __target.`id` = __source.`id` WHEN MATCHED THEN UPDATE SET `profile` = __source.`profile`, `updated_at` = __source.`updated_at`, `_sqlload_load_id` = __source.`_sqlload_load_id`, `_sqlload_loaded_at` = __source.`_sqlload_loaded_at` WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`)",
            ],
        },
        WriteStatementsTestCase {
            description: "merge with dedupe on ClickHouse fills the delta with one row per key, so the delta replaces each key once",
            warehouse: Warehouse::ClickHouse,
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
                not_null_keys: vec!["id".to_owned()],
                dedupe: Some(MergeDedupe {
                    ordinal: "_sqlload_row".to_owned(),
                    newest_by: Vec::new(),
                }),
            },
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`app__accounts__sqlload_staging_load1_swap`",
                "CREATE TABLE `raw`.`app__accounts__sqlload_staging_load1_swap` AS `raw`.`app__accounts`",
                "-- sqlload publish {\"SortedInsert\":{\"database\":\"raw\",\"table\":\"app__accounts__sqlload_staging_load1_swap\",\"columns\":[\"id\",\"profile\",\"updated_at\",\"_sqlload_load_id\",\"_sqlload_loaded_at\"],\"insert\":\"INSERT INTO `raw`.`app__accounts__sqlload_staging_load1_swap` (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) SELECT `id` AS `id`, `profile` AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM (SELECT `id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at` FROM (SELECT `id` AS `id`, CAST(`profile` AS Nullable(String)) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at`, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `_sqlload_row` DESC) AS __sqlload_rank FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __sqlload_ranked WHERE __sqlload_rank = 1) AS __sqlload_kept\"}}",
                "-- sqlload publish {\"Partitions\":{\"target\":\"`raw`.`app__accounts`\",\"delta\":\"`raw`.`app__accounts__sqlload_staging_load1_swap`\",\"copy\":\"`raw`.`app__accounts__sqlload_staging_load1_swap_copy`\",\"aside\":\"`raw`.`app__accounts__sqlload_staging_load1_swap_copy_aside`\",\"displaced\":\"(`id`) IN (SELECT `id` FROM `raw`.`app__accounts__sqlload_staging_load1_swap`)\"}}",
                "DROP TABLE IF EXISTS `raw`.`app__accounts__sqlload_staging_load1_swap`",
            ],
        },
        WriteStatementsTestCase {
            description: "delete_insert replaces the cursor window",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::DeleteInsert {
                cursor_column: Some("updated_at".to_owned()),
                from: Some("2026-11-04T09:00:00Z".to_owned()),
                to: None,
                values: Vec::new(),
                replace_key: Vec::new(),
                not_null_keys: Vec::new(),
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
                cursor_column: Some("code".to_owned()),
                from: Some("B1".to_owned()),
                to: Some("a9".to_owned()),
                values: Vec::new(),
                replace_key: Vec::new(),
                not_null_keys: Vec::new(),
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
                cursor_column: Some("code".to_owned()),
                from: None,
                to: None,
                values: vec!["B1".to_owned()],
                replace_key: Vec::new(),
                not_null_keys: Vec::new(),
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
                cursor_column: Some("code".to_owned()),
                from: Some("B1".to_owned()),
                to: None,
                values: Vec::new(),
                replace_key: Vec::new(),
                not_null_keys: Vec::new(),
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"raw\".\"app__accounts\" WHERE \"code\" >= CAST('B1' AS VARCHAR)",
                DUCK_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "delete_insert by replace_key on Snowflake deletes the rows holding a staged key, a NULL matching a NULL, then inserts, in one transaction",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::DeleteInsert {
                cursor_column: None,
                from: None,
                to: None,
                values: Vec::new(),
                replace_key: vec!["id".to_owned()],
                not_null_keys: vec![],
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"RAW\".\"APP__ACCOUNTS\" AS __target USING (SELECT DISTINCT \"ID\" AS \"ID\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\") AS __source WHERE __target.\"ID\" IS NOT DISTINCT FROM __source.\"ID\"",
                SF_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "delete_insert by replace_key on Postgres compares a key column the source declares NOT NULL with =",
            warehouse: Warehouse::Postgres,
            mode: WriteMode::DeleteInsert {
                cursor_column: None,
                from: None,
                to: None,
                values: Vec::new(),
                replace_key: vec!["id".to_owned()],
                not_null_keys: vec!["id".to_owned()],
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"raw\".\"app__accounts\" AS __target USING (SELECT DISTINCT \"id\" AS \"id\" FROM \"pg_temp\".\"app__accounts__sqlload_staging_load1\") AS __source WHERE __target.\"id\" = __source.\"id\"",
                "INSERT INTO \"raw\".\"app__accounts\" (\"id\", \"profile\", \"updated_at\", \"_sqlload_load_id\", \"_sqlload_loaded_at\") SELECT \"id\", CAST(\"profile\" AS jsonb), \"updated_at\", \"_sqlload_load_id\", \"_sqlload_loaded_at\" FROM \"pg_temp\".\"app__accounts__sqlload_staging_load1\"",
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "delete_insert with a cursor and a replace_key on DuckDB deletes the cursor window and the rows of the staged keys",
            warehouse: Warehouse::DuckDb,
            mode: WriteMode::DeleteInsert {
                cursor_column: Some("updated_at".to_owned()),
                from: Some("2026-11-04T09:00:00Z".to_owned()),
                to: None,
                values: Vec::new(),
                replace_key: vec!["id".to_owned()],
                not_null_keys: vec![],
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"raw\".\"app__accounts\" WHERE \"updated_at\" >= CAST('2026-11-04T09:00:00Z' AS TIMESTAMP WITH TIME ZONE)",
                "DELETE FROM \"raw\".\"app__accounts\" AS __target USING (SELECT DISTINCT \"id\" AS \"id\" FROM \"raw\".\"app__accounts__sqlload_staging_load1\") AS __source WHERE __target.\"id\" IS NOT DISTINCT FROM __source.\"id\"",
                DUCK_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "delete_insert by replace_key on BigQuery is one MERGE that deletes the rows of each distinct staged key and inserts every staged row",
            warehouse: Warehouse::BigQuery,
            mode: WriteMode::DeleteInsert {
                cursor_column: None,
                from: None,
                to: None,
                values: Vec::new(),
                replace_key: vec!["id".to_owned()],
                not_null_keys: vec![],
            },
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, PARSE_JSON(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at`, FALSE AS __sqlload_replaces FROM `raw`.`app__accounts__sqlload_staging_load1` UNION ALL SELECT `id`, NULL, NULL, NULL, NULL, TRUE FROM (SELECT DISTINCT `id` AS `id` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __sqlload_keys) AS __source ON __source.__sqlload_replaces AND (__target.`id` = __source.`id` OR (__target.`id` IS NULL AND __source.`id` IS NULL)) WHEN MATCHED THEN DELETE WHEN NOT MATCHED AND NOT __source.__sqlload_replaces THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`)",
            ],
        },
        WriteStatementsTestCase {
            description: "delete_insert with a cursor and a replace_key on Databricks is one MERGE that also deletes the cursor window",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::DeleteInsert {
                cursor_column: Some("updated_at".to_owned()),
                from: Some("2026-11-04T09:00:00Z".to_owned()),
                to: None,
                values: Vec::new(),
                replace_key: vec!["id".to_owned()],
                not_null_keys: vec![],
            },
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, parse_json(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at`, FALSE AS __sqlload_replaces FROM `raw`.`app__accounts__sqlload_staging_load1` UNION ALL SELECT `id`, NULL, NULL, NULL, NULL, TRUE FROM (SELECT DISTINCT `id` AS `id` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __sqlload_keys) AS __source ON __source.__sqlload_replaces AND __target.`id` <=> __source.`id` WHEN MATCHED THEN DELETE WHEN NOT MATCHED AND NOT __source.__sqlload_replaces THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`) WHEN NOT MATCHED BY SOURCE AND __target.`updated_at` >= CAST('2026-11-04T09:00:00Z' AS TIMESTAMP) THEN DELETE",
            ],
        },
        WriteStatementsTestCase {
            description: "delete_insert by replace_key on ClickHouse publishes the delta displacing the rows of its keys, a NULL matching a NULL",
            warehouse: Warehouse::ClickHouse,
            mode: WriteMode::DeleteInsert {
                cursor_column: None,
                from: None,
                to: None,
                values: Vec::new(),
                replace_key: vec!["id".to_owned()],
                not_null_keys: vec![],
            },
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`app__accounts__sqlload_staging_load1_swap`",
                "CREATE TABLE `raw`.`app__accounts__sqlload_staging_load1_swap` AS `raw`.`app__accounts`",
                CH_DELTA_INSERT,
                "-- sqlload publish {\"Partitions\":{\"target\":\"`raw`.`app__accounts`\",\"delta\":\"`raw`.`app__accounts__sqlload_staging_load1_swap`\",\"copy\":\"`raw`.`app__accounts__sqlload_staging_load1_swap_copy`\",\"aside\":\"`raw`.`app__accounts__sqlload_staging_load1_swap_copy_aside`\",\"displaced\":\"(isNull(`id`), ifNull(`id`, defaultValueOfArgumentType(assumeNotNull(`id`)))) IN (SELECT isNull(`id`), ifNull(`id`, defaultValueOfArgumentType(assumeNotNull(`id`))) FROM `raw`.`app__accounts__sqlload_staging_load1_swap`)\"}}",
                "DROP TABLE IF EXISTS `raw`.`app__accounts__sqlload_staging_load1_swap`",
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
                cursor_column: Some("updated_at".to_owned()),
                from: Some("2026-11-01".to_owned()),
                to: Some("2026-11-02".to_owned()),
                values: Vec::new(),
                replace_key: Vec::new(),
                not_null_keys: Vec::new(),
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
                cursor_column: Some("updated_at".to_owned()),
                from: Some("2026-11-01".to_owned()),
                to: None,
                values: vec!["2026-11-01".to_owned(), "2026-11-03".to_owned()],
                replace_key: Vec::new(),
                not_null_keys: Vec::new(),
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
                cursor_column: Some("updated_at".to_owned()),
                from: Some("2026-11-04T09:00:00Z".to_owned()),
                to: Some("2026-11-05T09:00:00Z".to_owned()),
                values: Vec::new(),
                replace_key: Vec::new(),
                not_null_keys: Vec::new(),
            },
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, parse_json(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __source ON FALSE WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`) WHEN NOT MATCHED BY SOURCE AND __target.`updated_at` >= CAST('2026-11-04T09:00:00Z' AS TIMESTAMP) AND __target.`updated_at` < CAST('2026-11-05T09:00:00Z' AS TIMESTAMP) THEN DELETE",
            ],
        },
        WriteStatementsTestCase {
            description: "delete_insert by partition values on Databricks deletes the listed values",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::DeleteInsert {
                cursor_column: Some("updated_at".to_owned()),
                from: None,
                to: None,
                values: vec!["2026-11-01".to_owned(), "2026-11-03".to_owned()],
                replace_key: Vec::new(),
                not_null_keys: Vec::new(),
            },
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, parse_json(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __source ON FALSE WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`) WHEN NOT MATCHED BY SOURCE AND __target.`updated_at` IN (CAST('2026-11-01' AS TIMESTAMP), CAST('2026-11-03' AS TIMESTAMP)) THEN DELETE",
            ],
        },
        WriteStatementsTestCase {
            description: "an unbounded delete_insert on Databricks deletes every row it does not insert",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::DeleteInsert {
                cursor_column: Some("updated_at".to_owned()),
                from: None,
                to: None,
                values: Vec::new(),
                replace_key: Vec::new(),
                not_null_keys: Vec::new(),
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
                cursor_column: Some("updated_at".to_owned()),
                from: Some("2026-11-04T09:00:00Z".to_owned()),
                to: Some("2026-11-05T09:00:00Z".to_owned()),
                values: Vec::new(),
                replace_key: Vec::new(),
                not_null_keys: Vec::new(),
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
                cursor_column: Some("updated_at".to_owned()),
                from: Some("2026-11-04 09:00:00.000000+00".to_owned()),
                to: Some("2026-11-05 09:00:00.000000+00".to_owned()),
                values: Vec::new(),
                replace_key: Vec::new(),
                not_null_keys: Vec::new(),
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
                not_null_keys: vec!["id".to_owned()],
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
            spared_load: None,
        };
        assert_eq!(
            sql_adapter(test_case.warehouse).render_write(&request, &staging),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_soft_tombstones_when_rendering_their_mark_then_a_warehouse_with_update_from_sets_each_rows_latest_tombstone()
 {
    let orders = TableName {
        database: None,
        schema: "raw".to_owned(),
        table: "orders".to_owned(),
    };
    let tombstones = TombstoneWrite {
        tombstones: TableName {
            table: "orders__sqlload_deletes".to_owned(),
            ..orders.clone()
        },
        deleted_at: "deleted_at".to_owned(),
        cursor: vec!["updated_at".to_owned()],
        unique_key: vec!["id".to_owned()],
        not_null_keys: vec!["id".to_owned()],
        soft: true,
    };
    let chosen = "__target.\"_sqlload_deleted_at\" IS NULL AND EXISTS (SELECT 1 FROM \"raw\".\"orders__sqlload_deletes\" AS __tombstone WHERE __target.\"id\" = __tombstone.\"id\" AND (__target.\"updated_at\" IS NULL OR __tombstone.\"deleted_at\" >= __target.\"updated_at\"))";
    let test_cases = [TombstonesTestCase {
        description: "DuckDB counts the rows, then sets each one's latest tombstone time through a grouped join, choosing the same rows",
        warehouse: Warehouse::DuckDb,
        expected_statements: [
            format!(
                "SELECT CAST(COUNT(*) AS VARCHAR) FROM \"raw\".\"orders\" AS __target WHERE {chosen}"
            ),
            format!(
                "UPDATE \"raw\".\"orders\" AS __target SET \"_sqlload_deleted_at\" = __marks.\"_sqlload_deleted_at\" FROM (SELECT __row.\"id\" AS \"id\", CAST(MAX(__tombstone.\"deleted_at\") AS TIMESTAMP WITH TIME ZONE) AS \"_sqlload_deleted_at\" FROM \"raw\".\"orders\" AS __row JOIN \"raw\".\"orders__sqlload_deletes\" AS __tombstone ON __row.\"id\" = __tombstone.\"id\" AND (__row.\"updated_at\" IS NULL OR __tombstone.\"deleted_at\" >= __row.\"updated_at\") WHERE __row.\"_sqlload_deleted_at\" IS NULL GROUP BY __row.\"id\") AS __marks WHERE __target.\"id\" = __marks.\"id\" AND {chosen}"
            ),
        ],
    }];
    for test_case in test_cases {
        assert_eq!(
            sql_adapter(test_case.warehouse).render_tombstones(&orders, &tombstones),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}
