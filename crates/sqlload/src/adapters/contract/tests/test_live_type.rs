use crate::adapters::contract::tests::helpers::live;
use crate::adapters::contract::tests::test_types::LiveTypeTestCase;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::{ColumnType, Warehouse};

#[test]
fn given_a_reported_column_type_when_reading_it_back_then_it_maps_to_the_portable_type() {
    let test_cases = [
        LiveTypeTestCase {
            description: "Snowflake NUMBER(38,0) is int",
            live: live("NUMBER", Some(38), Some(0), None),
            warehouse: Warehouse::Snowflake,
            expected_type: ColumnType::Int,
        },
        LiveTypeTestCase {
            description: "Snowflake NUMBER(18,2) is a decimal",
            live: live("NUMBER", Some(18), Some(2), None),
            warehouse: Warehouse::Snowflake,
            expected_type: ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
        },
        LiveTypeTestCase {
            description: "Snowflake default-length TEXT is text",
            live: live("TEXT", None, None, Some(16_777_216)),
            warehouse: Warehouse::Snowflake,
            expected_type: ColumnType::Text,
        },
        LiveTypeTestCase {
            description: "Snowflake TEXT with a length is varchar",
            live: live("TEXT", None, None, Some(36)),
            warehouse: Warehouse::Snowflake,
            expected_type: ColumnType::Varchar(36),
        },
        LiveTypeTestCase {
            description: "Snowflake VARIANT is json",
            live: live("VARIANT", None, None, None),
            warehouse: Warehouse::Snowflake,
            expected_type: ColumnType::Json,
        },
        LiveTypeTestCase {
            description: "an unknown Snowflake type is native",
            live: live("GEOGRAPHY", None, None, None),
            warehouse: Warehouse::Snowflake,
            expected_type: ColumnType::Native("GEOGRAPHY".to_owned()),
        },
        LiveTypeTestCase {
            description: "DuckDB DECIMAL(10,2)",
            live: live("DECIMAL(10,2)", Some(10), Some(2), None),
            warehouse: Warehouse::DuckDb,
            expected_type: ColumnType::Decimal {
                precision: 10,
                scale: 2,
            },
        },
        LiveTypeTestCase {
            description: "DuckDB TIMESTAMP is timestamp_ntz",
            live: live("TIMESTAMP", None, None, None),
            warehouse: Warehouse::DuckDb,
            expected_type: ColumnType::TimestampNtz,
        },
        LiveTypeTestCase {
            description: "DuckDB TIMESTAMP WITH TIME ZONE is timestamp_tz",
            live: live("TIMESTAMP WITH TIME ZONE", None, None, None),
            warehouse: Warehouse::DuckDb,
            expected_type: ColumnType::TimestampTz,
        },
        LiveTypeTestCase {
            description: "Databricks bigint is int",
            live: live("bigint", None, None, None),
            warehouse: Warehouse::Databricks,
            expected_type: ColumnType::Int,
        },
        LiveTypeTestCase {
            description: "Databricks decimal(18,2) is a decimal",
            live: live("decimal(18,2)", Some(18), Some(2), None),
            warehouse: Warehouse::Databricks,
            expected_type: ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
        },
        LiveTypeTestCase {
            description: "Databricks varchar(10) keeps its length",
            live: live("varchar(10)", None, None, Some(0)),
            warehouse: Warehouse::Databricks,
            expected_type: ColumnType::Varchar(10),
        },
        LiveTypeTestCase {
            description: "Databricks timestamp is timestamp_tz and timestamp_ntz is timestamp_ntz",
            live: live("timestamp_ntz", None, None, None),
            warehouse: Warehouse::Databricks,
            expected_type: ColumnType::TimestampNtz,
        },
        LiveTypeTestCase {
            description: "Databricks timestamp is an instant",
            live: live("timestamp", None, None, None),
            warehouse: Warehouse::Databricks,
            expected_type: ColumnType::TimestampTz,
        },
        LiveTypeTestCase {
            description: "Databricks array<string> passes through as native",
            live: live("array<string>", None, None, None),
            warehouse: Warehouse::Databricks,
            expected_type: ColumnType::Native("array<string>".to_owned()),
        },
        LiveTypeTestCase {
            description: "BigQuery's legacy INTEGER is int",
            live: live("INTEGER", None, None, None),
            warehouse: Warehouse::BigQuery,
            expected_type: ColumnType::Int,
        },
        LiveTypeTestCase {
            description: "BigQuery NUMERIC without parameters is decimal(38,9)",
            live: live("NUMERIC", None, None, None),
            warehouse: Warehouse::BigQuery,
            expected_type: ColumnType::Decimal {
                precision: 38,
                scale: 9,
            },
        },
        LiveTypeTestCase {
            description: "BigQuery BIGNUMERIC past 38 digits passes through as native",
            live: live("BIGNUMERIC", Some(40), Some(2), None),
            warehouse: Warehouse::BigQuery,
            expected_type: ColumnType::Native("BIGNUMERIC".to_owned()),
        },
        LiveTypeTestCase {
            description: "BigQuery STRING with a maxLength keeps its length",
            live: live("STRING", None, None, Some(10)),
            warehouse: Warehouse::BigQuery,
            expected_type: ColumnType::Varchar(10),
        },
        LiveTypeTestCase {
            description: "BigQuery DATETIME is timestamp_ntz",
            live: live("DATETIME", None, None, None),
            warehouse: Warehouse::BigQuery,
            expected_type: ColumnType::TimestampNtz,
        },
        LiveTypeTestCase {
            description: "BigQuery RECORD passes through as native",
            live: live("RECORD", None, None, None),
            warehouse: Warehouse::BigQuery,
            expected_type: ColumnType::Native("RECORD".to_owned()),
        },
        LiveTypeTestCase {
            description: "ClickHouse DateTime64 with a zone is timestamp_tz, under its Nullable wrapper",
            live: live("Nullable(DateTime64(6, 'UTC'))", None, None, None),
            warehouse: Warehouse::ClickHouse,
            expected_type: ColumnType::TimestampTz,
        },
        LiveTypeTestCase {
            description: "ClickHouse DateTime64 without a zone is timestamp_ntz",
            live: live("DateTime64(6)", None, None, None),
            warehouse: Warehouse::ClickHouse,
            expected_type: ColumnType::TimestampNtz,
        },
        LiveTypeTestCase {
            description: "ClickHouse Decimal64(4) is decimal(18,4)",
            live: live("Nullable(Decimal64(4))", None, None, None),
            warehouse: Warehouse::ClickHouse,
            expected_type: ColumnType::Decimal {
                precision: 18,
                scale: 4,
            },
        },
        LiveTypeTestCase {
            description: "ClickHouse LowCardinality(Nullable(String)) is text",
            live: live("LowCardinality(Nullable(String))", None, None, None),
            warehouse: Warehouse::ClickHouse,
            expected_type: ColumnType::Text,
        },
        LiveTypeTestCase {
            description: "ClickHouse Int32 is narrower than int, so it stays native rather than wrap a wider value",
            live: live("Nullable(Int32)", None, None, None),
            warehouse: Warehouse::ClickHouse,
            expected_type: ColumnType::Native("Nullable(Int32)".to_owned()),
        },
        LiveTypeTestCase {
            description: "ClickHouse UInt16 holds no negative smallint, so it stays native",
            live: live("UInt16", None, None, None),
            warehouse: Warehouse::ClickHouse,
            expected_type: ColumnType::Native("UInt16".to_owned()),
        },
        LiveTypeTestCase {
            description: "ClickHouse Date holds only 1970 on, so it stays native rather than be a date",
            live: live("Nullable(Date)", None, None, None),
            warehouse: Warehouse::ClickHouse,
            expected_type: ColumnType::Native("Nullable(Date)".to_owned()),
        },
        LiveTypeTestCase {
            description: "ClickHouse DateTime64(3) drops microseconds, so it stays native",
            live: live("DateTime64(3, 'UTC')", None, None, None),
            warehouse: Warehouse::ClickHouse,
            expected_type: ColumnType::Native("DateTime64(3, 'UTC')".to_owned()),
        },
        LiveTypeTestCase {
            description: "ClickHouse Array passes through as native",
            live: live("Array(Int64)", None, None, None),
            warehouse: Warehouse::ClickHouse,
            expected_type: ColumnType::Native("Array(Int64)".to_owned()),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            sql_adapter(test_case.warehouse).live_type(&test_case.live),
            test_case.expected_type,
            "{}",
            test_case.description
        );
    }
}
