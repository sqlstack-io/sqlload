use crate::column_types::models::Warehouse;
use crate::write_sql::main::staging_table::staging_table;
use crate::write_sql::main::write_statements::write_statements;
use crate::write_sql::models::{WriteMode, WriteStatementsRequest};
use crate::write_sql::tests::helpers::{accounts, columns};
use crate::write_sql::tests::test_types::WriteStatementsTestCase;

const SF_INSERT: &str = "INSERT INTO \"RAW\".\"APP__ACCOUNTS\" (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") SELECT \"ID\", PARSE_JSON(\"PROFILE\"), \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"";
const DUCK_INSERT: &str = "INSERT INTO \"raw\".\"app__accounts\" (\"id\", \"profile\", \"updated_at\", \"_sqlload_load_id\", \"_sqlload_loaded_at\") SELECT \"id\", CAST(\"profile\" AS JSON), \"updated_at\", \"_sqlload_load_id\", \"_sqlload_loaded_at\" FROM \"raw\".\"app__accounts__sqlload_staging_load1\"";

const SF_SWAP_INSERT: &str = "INSERT INTO \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1_SWAP\" (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") SELECT \"ID\", PARSE_JSON(\"PROFILE\"), \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"";

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
            },
            expected_statements: &[
                "MERGE INTO \"RAW\".\"APP__ACCOUNTS\" AS __target USING (SELECT \"ID\" AS \"ID\", PARSE_JSON(\"PROFILE\") AS \"PROFILE\", \"UPDATED_AT\" AS \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" AS \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" AS \"_SQLLOAD_LOADED_AT\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\") AS __source ON __target.\"ID\" = __source.\"ID\" WHEN MATCHED THEN UPDATE SET \"PROFILE\" = __source.\"PROFILE\", \"UPDATED_AT\" = __source.\"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\" = __source.\"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" = __source.\"_SQLLOAD_LOADED_AT\" WHEN NOT MATCHED THEN INSERT (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") VALUES (__source.\"ID\", __source.\"PROFILE\", __source.\"UPDATED_AT\", __source.\"_SQLLOAD_LOAD_ID\", __source.\"_SQLLOAD_LOADED_AT\")",
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
                "DELETE FROM \"RAW\".\"APP__ACCOUNTS\" WHERE \"UPDATED_AT\" >= CAST('2026-11-04T09:00:00Z' AS TIMESTAMP_TZ(6))",
                SF_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "replace on DuckDB is one transaction, which keeps the table's constraints",
            warehouse: Warehouse::DuckDb,
            mode: WriteMode::Replace,
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"raw\".\"app__accounts\"",
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
                "DELETE FROM \"RAW\".\"APP__ACCOUNTS\" WHERE \"UPDATED_AT\" IN (CAST('2026-11-01' AS TIMESTAMP_TZ(6)), CAST('2026-11-03' AS TIMESTAMP_TZ(6)))",
                SF_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "recreate rebuilds the table in one statement, keeping grants on Snowflake",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Recreate,
            expected_statements: &[
                "CREATE OR REPLACE TABLE \"RAW\".\"APP__ACCOUNTS\" (\"ID\" NUMBER(38,0), \"PROFILE\" VARIANT, \"UPDATED_AT\" TIMESTAMP_TZ(6), \"_SQLLOAD_LOAD_ID\" VARCHAR, \"_SQLLOAD_LOADED_AT\" TIMESTAMP_TZ(6)) COPY GRANTS AS SELECT \"ID\", PARSE_JSON(\"PROFILE\"), \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"",
            ],
        },
    ];
    for test_case in test_cases {
        let table = accounts();
        let columns = columns();
        let (staging, _) = staging_table(&table, &columns, "load-1", test_case.warehouse);
        let request = WriteStatementsRequest {
            warehouse: test_case.warehouse,
            table: &table,
            columns: &columns,
            mode: &test_case.mode,
        };
        assert_eq!(
            write_statements(&request, &staging),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}
