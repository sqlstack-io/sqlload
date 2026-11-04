use crate::column_types::main::warehouse_type::warehouse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::column_types::tests::test_types::WarehouseTypeTestCase;

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
            description: "BigQuery uses BIGNUMERIC beyond NUMERIC's range",
            column_type: ColumnType::Decimal {
                precision: 38,
                scale: 10,
            },
            warehouse: Warehouse::BigQuery,
            expected_sql: "BIGNUMERIC(38,10)",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "BigQuery timestamps without a zone are DATETIME",
            column_type: ColumnType::TimestampNtz,
            warehouse: Warehouse::BigQuery,
            expected_sql: "DATETIME",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "Databricks has no time type",
            column_type: ColumnType::Time,
            warehouse: Warehouse::Databricks,
            expected_sql: "STRING",
            expected_lossy: true,
        },
        WarehouseTypeTestCase {
            description: "Postgres json is jsonb",
            column_type: ColumnType::Json,
            warehouse: Warehouse::Postgres,
            expected_sql: "jsonb",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "MotherDuck maps like DuckDB",
            column_type: ColumnType::Binary,
            warehouse: Warehouse::MotherDuck,
            expected_sql: "BLOB",
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
            description: "ClickHouse timestamps with a zone are UTC DateTime64",
            column_type: ColumnType::TimestampTz,
            warehouse: Warehouse::ClickHouse,
            expected_sql: "DateTime64(6, 'UTC')",
            expected_lossy: true,
        },
        WarehouseTypeTestCase {
            description: "SQL Server keeps the offset",
            column_type: ColumnType::TimestampTz,
            warehouse: Warehouse::SqlServer,
            expected_sql: "DATETIMEOFFSET(6)",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "SQL Server long varchars become MAX",
            column_type: ColumnType::Varchar(8000),
            warehouse: Warehouse::SqlServer,
            expected_sql: "NVARCHAR(MAX)",
            expected_lossy: true,
        },
        WarehouseTypeTestCase {
            description: "SQLite keeps decimals as text",
            column_type: decimal,
            warehouse: Warehouse::Sqlite,
            expected_sql: "TEXT",
            expected_lossy: true,
        },
        WarehouseTypeTestCase {
            description: "a native type passes through unchanged",
            column_type: ColumnType::Native("GEOGRAPHY".to_owned()),
            warehouse: Warehouse::Snowflake,
            expected_sql: "GEOGRAPHY",
            expected_lossy: false,
        },
    ];
    for test_case in test_cases {
        let mapped = warehouse_type(&test_case.column_type, test_case.warehouse);
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
