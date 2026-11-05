use crate::adapters::contract::models::{WriteMode, WriteStatementsRequest};
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
