use crate::column_types::models::Warehouse;
use crate::write_sql::main::staging_table::staging_table;
use crate::write_sql::main::write_statements::write_statements;
use crate::write_sql::models::{WriteMode, WriteStatementsRequest};
use crate::write_sql::tests::helpers::{accounts, columns};
use crate::write_sql::tests::test_types::WriteStatementsTestCase;

const SF_INSERT: &str = "INSERT INTO \"RAW\".\"APP__ACCOUNTS\" (\"ID\", \"PROFILE\", \"UPDATED_AT\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\") SELECT \"ID\", PARSE_JSON(\"PROFILE\"), \"UPDATED_AT\", 'load-1', CAST('2026-11-04T10:00:00Z' AS TIMESTAMP_TZ(6)) FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"";
const DUCK_INSERT: &str = "INSERT INTO \"raw\".\"app__accounts\" (\"id\", \"profile\", \"updated_at\", \"_sqlload_load_id\", \"_sqlload_loaded_at\") SELECT \"id\", CAST(\"profile\" AS JSON), \"updated_at\", 'load-1', CAST('2026-11-04T10:00:00Z' AS TIMESTAMP WITH TIME ZONE) FROM \"raw\".\"app__accounts__sqlload_staging_load1\"";

#[test]
fn given_a_write_mode_when_building_statements_then_one_transaction_applies_it() {
    let test_cases = [
        WriteStatementsTestCase {
            description: "append inserts only",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Append,
            expected_statements: &["BEGIN", SF_INSERT, "COMMIT"],
        },
        WriteStatementsTestCase {
            description: "merge deletes matching keys first",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"RAW\".\"APP__ACCOUNTS\" WHERE (\"ID\") IN (SELECT \"ID\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\")",
                SF_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "delete_insert replaces the cursor window",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: Some("2026-11-04T09:00:00Z".to_owned()),
                to: None,
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"RAW\".\"APP__ACCOUNTS\" WHERE \"UPDATED_AT\" >= CAST('2026-11-04T09:00:00Z' AS TIMESTAMP_TZ(6))",
                SF_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "replace swaps the whole table inside one transaction",
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
            description: "a backfill window bounds the delete on both sides",
            warehouse: Warehouse::DuckDb,
            mode: WriteMode::DeleteInsert {
                cursor_column: "updated_at".to_owned(),
                from: Some("2026-11-01".to_owned()),
                to: Some("2026-11-02".to_owned()),
            },
            expected_statements: &[
                "BEGIN",
                "DELETE FROM \"raw\".\"app__accounts\" WHERE \"updated_at\" >= CAST('2026-11-01' AS TIMESTAMP WITH TIME ZONE) AND \"updated_at\" < CAST('2026-11-02' AS TIMESTAMP WITH TIME ZONE)",
                DUCK_INSERT,
                "COMMIT",
            ],
        },
        WriteStatementsTestCase {
            description: "recreate rebuilds the table in one statement, keeping grants on Snowflake",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Recreate,
            expected_statements: &[
                "CREATE OR REPLACE TABLE \"RAW\".\"APP__ACCOUNTS\" (\"ID\" NUMBER(38,0), \"PROFILE\" VARIANT, \"UPDATED_AT\" TIMESTAMP_TZ(6), \"_SQLLOAD_LOAD_ID\" VARCHAR, \"_SQLLOAD_LOADED_AT\" TIMESTAMP_TZ(6)) COPY GRANTS AS SELECT \"ID\", PARSE_JSON(\"PROFILE\"), \"UPDATED_AT\", 'load-1', CAST('2026-11-04T10:00:00Z' AS TIMESTAMP_TZ(6)) FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"",
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
            load_id: "load-1",
            loaded_at: "2026-11-04T10:00:00Z",
        };
        assert_eq!(
            write_statements(&request, &staging),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}
