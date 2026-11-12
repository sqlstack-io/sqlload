use crate::adapters::duckdb::tests::helpers::{unloaded_insert_sql, unloaded_row_loaded};
use crate::adapters::duckdb::tests::test_types::{UnloadedInsertTestCase, UnloadedSqlTestCase};
use crate::column_types::models::ColumnType;

#[test]
fn given_unloaded_microseconds_when_duckdb_reads_the_files_itself_then_times_and_timestamps_are_exact()
 {
    let test_cases = [
        UnloadedInsertTestCase {
            description: "the first instant of year 1, and the last microsecond of a day, without ICU",
            micros: [
                -62_135_596_800_000_000,
                -62_135_596_800_000_000,
                86_399_999_999,
            ],
            time_zone: None,
            expected_values: [
                "0001-01-01 00:00:00",
                "-62135596800000000",
                "23:59:59.999999",
                "12.50",
                "L1",
                "NULL",
            ],
        },
        UnloadedInsertTestCase {
            description: "the last microsecond of year 9999, and midnight, in a session zone behind UTC",
            micros: [253_402_300_799_999_999, 253_402_300_799_999_999, 0],
            time_zone: Some("America/New_York"),
            expected_values: [
                "9999-12-31 23:59:59.999999",
                "253402300799999999",
                "00:00:00",
                "12.50",
                "L1",
                "NULL",
            ],
        },
        UnloadedInsertTestCase {
            description: "the instant the session's zone moves its clocks forward, which a cast through local time would shift",
            micros: [1_710_054_000_123_456, 1_710_054_000_123_456, 1],
            time_zone: Some("America/New_York"),
            expected_values: [
                "2024-03-10 07:00:00.123456",
                "1710054000123456",
                "00:00:00.000001",
                "12.50",
                "L1",
                "NULL",
            ],
        },
        UnloadedInsertTestCase {
            description: "an instant before the epoch, with microseconds, without ICU",
            micros: [-1, -1, 43_200_000_001],
            time_zone: None,
            expected_values: [
                "1969-12-31 23:59:59.999999",
                "-1",
                "12:00:00.000001",
                "12.50",
                "L1",
                "NULL",
            ],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            unloaded_row_loaded(test_case.micros, test_case.time_zone),
            Ok(test_case
                .expected_values
                .iter()
                .map(|value| (*value).to_owned())
                .collect()),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_an_unload_when_rendering_its_insert_then_only_constant_columns_change_it() {
    let test_cases = [
        UnloadedSqlTestCase {
            description: "without constant columns the statement is as before, a column the files lack being NULL",
            constants: &[],
            expected_sql: "INSERT INTO \"main\".\"loaded\" (\"ID\", \"tenant_id\", \"n\", \"_sqlload_load_id\") SELECT CAST(f.\"ID\" AS BIGINT), NULL, NULL, CAST('L1' AS VARCHAR) FROM read_parquet(['/unloaded/data_0.parquet'], hive_partitioning = false) AS f",
        },
        UnloadedSqlTestCase {
            description: "constant columns are their literals cast to their types, every other column as before",
            constants: &[
                ("tenant_id", ColumnType::Text, "it's"),
                ("n", ColumnType::Int, "7"),
            ],
            expected_sql: "INSERT INTO \"main\".\"loaded\" (\"ID\", \"tenant_id\", \"n\", \"_sqlload_load_id\") SELECT CAST(f.\"ID\" AS BIGINT), CAST('it''s' AS VARCHAR), CAST('7' AS BIGINT), CAST('L1' AS VARCHAR) FROM read_parquet(['/unloaded/data_0.parquet'], hive_partitioning = false) AS f",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            unloaded_insert_sql(test_case.constants),
            test_case.expected_sql,
            "{}",
            test_case.description
        );
    }
}
