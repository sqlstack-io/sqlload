use crate::adapters::contract::tests::test_types::WarehouseTypeTestCase;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::{ColumnType, Warehouse};

#[test]
fn given_a_portable_type_when_mapping_to_a_warehouse_then_the_sql_name_and_loss_are_reported() {
    let decimal = ColumnType::Decimal {
        precision: 18,
        scale: 2,
    };
    let test_cases = [
        WarehouseTypeTestCase {
            description: "Snowflake decimals are NUMBER",
            column_type: decimal.clone(),
            warehouse: Warehouse::Snowflake,
            expected_sql: "NUMBER(18,2)",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "Snowflake stores uuid as text",
            column_type: ColumnType::Uuid,
            warehouse: Warehouse::Snowflake,
            expected_sql: "VARCHAR(36)",
            expected_lossy: true,
        },
        WarehouseTypeTestCase {
            description: "Snowflake json is VARIANT",
            column_type: ColumnType::Json,
            warehouse: Warehouse::Snowflake,
            expected_sql: "VARIANT",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "Snowflake timestamps are microsecond",
            column_type: ColumnType::TimestampTz,
            warehouse: Warehouse::Snowflake,
            expected_sql: "TIMESTAMP_TZ(6)",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "DuckDB does not enforce varchar lengths",
            column_type: ColumnType::Varchar(20),
            warehouse: Warehouse::DuckDb,
            expected_sql: "VARCHAR",
            expected_lossy: true,
        },
        WarehouseTypeTestCase {
            description: "a native type passes through unchanged",
            column_type: ColumnType::Native("GEOGRAPHY".to_owned()),
            warehouse: Warehouse::Snowflake,
            expected_sql: "GEOGRAPHY",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "Parquet files hold the Arrow type",
            column_type: ColumnType::Int,
            warehouse: Warehouse::Parquet,
            expected_sql: "Int64",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "Parquet files cannot hold a native type",
            column_type: ColumnType::Native("GEOGRAPHY".to_owned()),
            warehouse: Warehouse::Parquet,
            expected_sql: "native(\"GEOGRAPHY\")",
            expected_lossy: true,
        },
    ];
    for test_case in test_cases {
        let mapped =
            sql_adapter(test_case.warehouse).render_loader_logical_type(&test_case.column_type);
        assert_eq!(
            mapped.sql, test_case.expected_sql,
            "{}",
            test_case.description
        );
        assert_eq!(
            mapped.lossy.is_some(),
            test_case.expected_lossy,
            "{}",
            test_case.description
        );
    }
}
