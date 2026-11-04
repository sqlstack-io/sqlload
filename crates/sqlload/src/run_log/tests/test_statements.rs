use crate::column_types::models::Warehouse;
use crate::run_log::tests::helpers::{
    append_logged, create_sql, insert_sql, merge_logged, recreate_logged, select_sql, swap_logged,
};
use crate::run_log::tests::test_types::StatementTestCase;

#[test]
fn given_a_run_log_when_writing_sql_then_each_warehouse_gets_its_statements() -> Result<(), String>
{
    let test_cases = [
        StatementTestCase {
            description: "DuckDB creates a plain table of text columns",
            warehouse: Warehouse::DuckDb,
            build: create_sql,
            expected_sql: "CREATE TABLE IF NOT EXISTS \"analytics\".\"raw\".\"_sqlload_runs\" (\"run_id\" VARCHAR, \"event\" VARCHAR, \"source\" VARCHAR, \"source_table\" VARCHAR, \"table_name\" VARCHAR, \"cursor_column\" VARCHAR, \"cursor_before\" VARCHAR, \"cursor_after\" VARCHAR, \"state\" VARCHAR, \"row_count\" VARCHAR, \"note\" VARCHAR, \"recorded_at\" VARCHAR, \"format_version\" VARCHAR, \"record_json\" VARCHAR)",
        },
        StatementTestCase {
            description: "Snowflake creates a permanent table with 90 days of time travel, and 1 where the edition allows no more",
            warehouse: Warehouse::Snowflake,
            build: create_sql,
            expected_sql: "CREATE TABLE IF NOT EXISTS \"ANALYTICS\".\"RAW\".\"_SQLLOAD_RUNS\" (\"RUN_ID\" VARCHAR, \"EVENT\" VARCHAR, \"SOURCE\" VARCHAR, \"SOURCE_TABLE\" VARCHAR, \"TABLE_NAME\" VARCHAR, \"CURSOR_COLUMN\" VARCHAR, \"CURSOR_BEFORE\" VARCHAR, \"CURSOR_AFTER\" VARCHAR, \"STATE\" VARCHAR, \"ROW_COUNT\" VARCHAR, \"NOTE\" VARCHAR, \"RECORDED_AT\" VARCHAR, \"FORMAT_VERSION\" VARCHAR, \"RECORD_JSON\" VARCHAR) DATA_RETENTION_TIME_IN_DAYS = 90;\nCREATE TABLE IF NOT EXISTS \"ANALYTICS\".\"RAW\".\"_SQLLOAD_RUNS\" (\"RUN_ID\" VARCHAR, \"EVENT\" VARCHAR, \"SOURCE\" VARCHAR, \"SOURCE_TABLE\" VARCHAR, \"TABLE_NAME\" VARCHAR, \"CURSOR_COLUMN\" VARCHAR, \"CURSOR_BEFORE\" VARCHAR, \"CURSOR_AFTER\" VARCHAR, \"STATE\" VARCHAR, \"ROW_COUNT\" VARCHAR, \"NOTE\" VARCHAR, \"RECORDED_AT\" VARCHAR, \"FORMAT_VERSION\" VARCHAR, \"RECORD_JSON\" VARCHAR) DATA_RETENTION_TIME_IN_DAYS = 1",
        },
        StatementTestCase {
            description: "DuckDB appends a record, missing fields as NULL",
            warehouse: Warehouse::DuckDb,
            build: insert_sql,
            expected_sql: "INSERT INTO \"analytics\".\"raw\".\"_sqlload_runs\" (\"run_id\", \"event\", \"source\", \"source_table\", \"table_name\", \"cursor_column\", \"cursor_before\", \"cursor_after\", \"state\", \"row_count\", \"note\", \"recorded_at\", \"format_version\", \"record_json\") VALUES ('load-1', 'run', 'shop', 'orders', 'shop__orders', 'updated_at', NULL, '2026-01-02 00:00:00.000000+00', '\"it''s\\\\x\"', '3', NULL, '2026-11-04T10:00:00.000000Z', '1', <json>)",
        },
        StatementTestCase {
            description: "Snowflake upper-cases the names and escapes backslashes",
            warehouse: Warehouse::Snowflake,
            build: insert_sql,
            expected_sql: "INSERT INTO \"ANALYTICS\".\"RAW\".\"_SQLLOAD_RUNS\" (\"RUN_ID\", \"EVENT\", \"SOURCE\", \"SOURCE_TABLE\", \"TABLE_NAME\", \"CURSOR_COLUMN\", \"CURSOR_BEFORE\", \"CURSOR_AFTER\", \"STATE\", \"ROW_COUNT\", \"NOTE\", \"RECORDED_AT\", \"FORMAT_VERSION\", \"RECORD_JSON\") VALUES ('load-1', 'run', 'shop', 'orders', 'shop__orders', 'updated_at', NULL, '2026-01-02 00:00:00.000000+00', '\"it''s\\\\\\\\x\"', '3', NULL, '2026-11-04T10:00:00.000000Z', '1', <json>)",
        },
        StatementTestCase {
            description: "Snowflake reads a table's records oldest first",
            warehouse: Warehouse::Snowflake,
            build: select_sql,
            expected_sql: "SELECT \"RECORD_JSON\" FROM \"ANALYTICS\".\"RAW\".\"_SQLLOAD_RUNS\" WHERE \"TABLE_NAME\" = 'shop__orders' ORDER BY \"RECORDED_AT\", \"RUN_ID\"",
        },
        StatementTestCase {
            description: "a transaction gets the record before its COMMIT",
            warehouse: Warehouse::Snowflake,
            build: append_logged,
            expected_sql: "BEGIN;\nINSERT INTO t SELECT 1;\nINSERT INTO runs;\nCOMMIT",
        },
        StatementTestCase {
            description: "a lone MERGE is wrapped in a transaction with the record",
            warehouse: Warehouse::Snowflake,
            build: merge_logged,
            expected_sql: "BEGIN;\nMERGE INTO t USING s;\nINSERT INTO runs;\nCOMMIT",
        },
        StatementTestCase {
            description: "DuckDB's DDL is transactional, so a rebuild commits with the record",
            warehouse: Warehouse::DuckDb,
            build: recreate_logged,
            expected_sql: "BEGIN;\nCREATE OR REPLACE TABLE t AS SELECT 1;\nINSERT INTO runs;\nCOMMIT",
        },
        StatementTestCase {
            description: "Snowflake's rebuild commits by itself, so the record follows once the data is in",
            warehouse: Warehouse::Snowflake,
            build: recreate_logged,
            expected_sql: "CREATE OR REPLACE TABLE t AS SELECT 1;\nINSERT INTO runs",
        },
        StatementTestCase {
            description: "Snowflake's swap commits by itself, so the record follows the swap",
            warehouse: Warehouse::Snowflake,
            build: swap_logged,
            expected_sql: "CREATE OR REPLACE TABLE swap LIKE t;\nALTER TABLE t SWAP WITH swap;\nINSERT INTO runs",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            (test_case.build)(test_case.warehouse)?,
            test_case.expected_sql,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
