use crate::adapters::contract::models::{
    ChangeApply, ChangeWrite, Truncation, WriteMode, WriteStatementsRequest,
};
use crate::adapters::contract::tests::helpers::{accounts, columns};
use crate::adapters::contract::tests::test_types::CommitStatementsTestCase;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;

const SF_INSERT: &str = "INSERT INTO \"RAW\".\"APP__ACCOUNTS\" (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") SELECT \"ID\", PARSE_JSON(\"PROFILE\"), \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"";
const SF_EMPTY: &str = "DELETE FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"";
const RUN: &str = "INSERT INTO \"RAW\".\"_SQLLOAD_RUNS\" VALUES ('record')";

#[test]
fn given_a_batch_commit_when_building_its_statements_then_the_write_record_and_emptying_are_one_transaction()
 {
    let test_cases = [
        CommitStatementsTestCase {
            description: "a staged append (JSON columns) inserts and empties the staging table in one transaction",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Append,
            run_insert: None,
            keep_staging: false,
            expected_statements: &["BEGIN", SF_INSERT, SF_EMPTY, "COMMIT"],
        },
        CommitStatementsTestCase {
            description: "a window's delete_insert deletes the window, inserts, records the run and empties the staging table",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: Some("2026-11-04 00:00:00.000000+00".to_owned()),
                to: Some("2026-11-05 00:00:00.000000+00".to_owned()),
                values: Vec::new(),
            },
            run_insert: Some(RUN),
            keep_staging: false,
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"RAW\".\"APP__ACCOUNTS\" WHERE \"UPDATED_AT\" >= CAST('2026-11-04 00:00:00.000000+00' AS TIMESTAMP_TZ(9)) AND \"UPDATED_AT\" < CAST('2026-11-05 00:00:00.000000+00' AS TIMESTAMP_TZ(9))",
                SF_INSERT,
                RUN,
                SF_EMPTY,
                "COMMIT",
            ],
        },
        CommitStatementsTestCase {
            description: "a merge commit is wrapped in a transaction with the emptying",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
                dedupe: None,
            },
            run_insert: None,
            keep_staging: false,
            expected_statements: &[
                "BEGIN",
                "MERGE INTO \"RAW\".\"APP__ACCOUNTS\" AS __target USING (SELECT \"ID\" AS \"ID\", PARSE_JSON(\"PROFILE\") AS \"PROFILE\", \"UPDATED_AT\" AS \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" AS \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" AS \"_SQLLOAD_LOADED_AT\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\") AS __source ON __target.\"ID\" = __source.\"ID\" WHEN MATCHED THEN UPDATE SET \"PROFILE\" = __source.\"PROFILE\", \"UPDATED_AT\" = __source.\"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" = __source.\"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" = __source.\"_SQLLOAD_LOADED_AT\" WHEN NOT MATCHED THEN INSERT (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") VALUES (__source.\"ID\", __source.\"PROFILE\", __source.\"UPDATED_AT\", __source.\"_SQLLOAD_LOAD_ID\", __source.\"_SQLLOAD_LOADED_AT\")",
                SF_EMPTY,
                "COMMIT",
            ],
        },
        CommitStatementsTestCase {
            description: "replace swaps the staging table in once at the end, then records the run",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Replace,
            run_insert: Some(RUN),
            keep_staging: true,
            expected_statements: &[
                "CREATE OR REPLACE TABLE \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1_SWAP\" LIKE \"RAW\".\"APP__ACCOUNTS\" COPY GRANTS",
                "INSERT INTO \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1_SWAP\" (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") SELECT \"ID\", PARSE_JSON(\"PROFILE\"), \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"",
                "ALTER TABLE \"RAW\".\"APP__ACCOUNTS\" SWAP WITH \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1_SWAP\"",
                "DROP TABLE IF EXISTS \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1_SWAP\"",
                RUN,
            ],
        },
        CommitStatementsTestCase {
            description: "DuckDB's lone delete_insert by partition values empties the staging table in its transaction",
            warehouse: Warehouse::DuckDb,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: None,
                to: None,
                values: vec!["2026-11-04".to_owned()],
            },
            run_insert: None,
            keep_staging: false,
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"raw\".\"app__accounts\" WHERE \"updated_at\" IN (CAST('2026-11-04' AS TIMESTAMP WITH TIME ZONE))",
                "INSERT INTO \"raw\".\"app__accounts\" (\"id\", \"profile\", \"updated_at\", \"_sqlload_load_id\", \"_sqlload_loaded_at\") SELECT \"id\", CAST(\"profile\" AS JSON), \"updated_at\", \"_sqlload_load_id\", \"_sqlload_loaded_at\" FROM \"raw\".\"app__accounts__sqlload_staging_load1\"",
                "DELETE FROM \"raw\".\"app__accounts__sqlload_staging_load1\"",
                "COMMIT",
            ],
        },
        CommitStatementsTestCase {
            description: "Databricks' delete_insert commits alone, then the run record and the staging table's emptying follow it",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: Some("2026-11-04".to_owned()),
                to: None,
                values: Vec::new(),
            },
            run_insert: Some(RUN),
            keep_staging: false,
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, parse_json(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __source ON FALSE WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`) WHEN NOT MATCHED BY SOURCE AND __target.`updated_at` >= CAST('2026-11-04' AS TIMESTAMP) THEN DELETE",
                RUN,
                "DELETE FROM `raw`.`app__accounts__sqlload_staging_load1`",
            ],
        },
        CommitStatementsTestCase {
            description: "BigQuery's delete_insert commits alone, then the run record, then a DELETE with the WHERE BigQuery requires",
            warehouse: Warehouse::BigQuery,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: Some("2026-11-04".to_owned()),
                to: None,
                values: Vec::new(),
            },
            run_insert: Some(RUN),
            keep_staging: false,
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, PARSE_JSON(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __source ON FALSE WHEN NOT MATCHED THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`) WHEN NOT MATCHED BY SOURCE AND __target.`updated_at` >= CAST('2026-11-04' AS TIMESTAMP) THEN DELETE",
                RUN,
                "DELETE FROM `raw`.`app__accounts__sqlload_staging_load1` WHERE TRUE",
            ],
        },
        CommitStatementsTestCase {
            description: "a Databricks merge batch with a truncate is one MERGE, its truncate a NOT MATCHED BY SOURCE clause, then the staging table's emptying",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::Changes(ChangeWrite {
                apply: ChangeApply::SoftDeletes,
                unique_key: vec!["id".to_owned()],
                truncated: Some(Truncation {
                    lsn: 42,
                    committed_at: "2026-11-07T10:00:00Z".to_owned(),
                }),
            }),
            run_insert: None,
            keep_staging: false,
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, parse_json(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at`, `_sqlload_unchanged` AS `_sqlload_unchanged` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __source ON __target.`id` = __source.`id` WHEN MATCHED AND __source.`_sqlload_lsn` > COALESCE(__target.`_sqlload_lsn`, -1) AND __source.`_sqlload_op` = 'delete' THEN UPDATE SET `_sqlload_deleted_at` = __source.`_sqlload_committed_at`, `_sqlload_load_id` = __source.`_sqlload_load_id`, `_sqlload_loaded_at` = __source.`_sqlload_loaded_at` WHEN MATCHED AND __source.`_sqlload_lsn` > COALESCE(__target.`_sqlload_lsn`, -1) THEN UPDATE SET `profile` = CASE WHEN POSITION(',profile,' IN __source.`_sqlload_unchanged`) > 0 THEN __target.`profile` ELSE __source.`profile` END, `updated_at` = CASE WHEN POSITION(',updated_at,' IN __source.`_sqlload_unchanged`) > 0 THEN __target.`updated_at` ELSE __source.`updated_at` END, `_sqlload_deleted_at` = NULL, `_sqlload_load_id` = __source.`_sqlload_load_id`, `_sqlload_loaded_at` = __source.`_sqlload_loaded_at` WHEN NOT MATCHED AND NOT __source.`_sqlload_op` = 'delete' THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`) WHEN NOT MATCHED BY SOURCE AND __target.`_sqlload_lsn` < 42 AND __target.`_sqlload_deleted_at` IS NULL THEN UPDATE SET `_sqlload_deleted_at` = CAST('2026-11-07T10:00:00Z' AS TIMESTAMP), `_sqlload_op` = 'truncate'",
                "DELETE FROM `raw`.`app__accounts__sqlload_staging_load1`",
            ],
        },
        CommitStatementsTestCase {
            description: "a Databricks hard-delete batch with a truncate deletes the older unmatched rows in its MERGE, and a matched older row keeps none of its values",
            warehouse: Warehouse::Databricks,
            mode: WriteMode::Changes(ChangeWrite {
                apply: ChangeApply::HardDeletes,
                unique_key: vec!["id".to_owned()],
                truncated: Some(Truncation {
                    lsn: 42,
                    committed_at: "2026-11-07T10:00:00Z".to_owned(),
                }),
            }),
            run_insert: None,
            keep_staging: false,
            expected_statements: &[
                "MERGE INTO `raw`.`app__accounts` AS __target USING (SELECT `id` AS `id`, parse_json(`profile`) AS `profile`, `updated_at` AS `updated_at`, `_sqlload_load_id` AS `_sqlload_load_id`, `_sqlload_loaded_at` AS `_sqlload_loaded_at`, `_sqlload_unchanged` AS `_sqlload_unchanged` FROM `raw`.`app__accounts__sqlload_staging_load1`) AS __source ON __target.`id` = __source.`id` WHEN MATCHED AND __source.`_sqlload_lsn` > COALESCE(__target.`_sqlload_lsn`, -1) AND __source.`_sqlload_op` = 'delete' THEN DELETE WHEN MATCHED AND __source.`_sqlload_lsn` > COALESCE(__target.`_sqlload_lsn`, -1) THEN UPDATE SET `profile` = CASE WHEN POSITION(',profile,' IN __source.`_sqlload_unchanged`) > 0 AND __target.`_sqlload_lsn` >= 42 THEN __target.`profile` ELSE __source.`profile` END, `updated_at` = CASE WHEN POSITION(',updated_at,' IN __source.`_sqlload_unchanged`) > 0 AND __target.`_sqlload_lsn` >= 42 THEN __target.`updated_at` ELSE __source.`updated_at` END, `_sqlload_load_id` = __source.`_sqlload_load_id`, `_sqlload_loaded_at` = __source.`_sqlload_loaded_at` WHEN NOT MATCHED AND NOT __source.`_sqlload_op` = 'delete' THEN INSERT (`id`, `profile`, `updated_at`, `_sqlload_load_id`, `_sqlload_loaded_at`) VALUES (__source.`id`, __source.`profile`, __source.`updated_at`, __source.`_sqlload_load_id`, __source.`_sqlload_loaded_at`) WHEN NOT MATCHED BY SOURCE AND __target.`_sqlload_lsn` < 42 THEN DELETE",
                "DELETE FROM `raw`.`app__accounts__sqlload_staging_load1`",
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
        let run_insert = test_case.run_insert.map(str::to_owned);
        assert_eq!(
            sql_adapter(test_case.warehouse).render_commit(
                &request,
                &staging,
                (run_insert, test_case.keep_staging)
            ),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}
