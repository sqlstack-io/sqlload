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
