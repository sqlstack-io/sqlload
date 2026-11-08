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
            description: "Snowflake timestamps keep nanoseconds",
            column_type: ColumnType::TimestampTz,
            warehouse: Warehouse::Snowflake,
            expected_sql: "TIMESTAMP_TZ(9)",
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
        WarehouseTypeTestCase {
            description: "BigQuery decimal(38,9) is the widest NUMERIC",
            column_type: ColumnType::Decimal {
                precision: 38,
                scale: 9,
            },
            warehouse: Warehouse::BigQuery,
            expected_sql: "NUMERIC(38,9)",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "BigQuery needs BIGNUMERIC for 30 digits before the point",
            column_type: ColumnType::Decimal {
                precision: 30,
                scale: 0,
            },
            warehouse: Warehouse::BigQuery,
            expected_sql: "BIGNUMERIC(30,0)",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "BigQuery needs BIGNUMERIC for a scale past 9",
            column_type: ColumnType::Decimal {
                precision: 20,
                scale: 10,
            },
            warehouse: Warehouse::BigQuery,
            expected_sql: "BIGNUMERIC(20,10)",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "BigQuery gives a decimal of only fraction digits one digit before the point",
            column_type: ColumnType::Decimal {
                precision: 5,
                scale: 5,
            },
            warehouse: Warehouse::BigQuery,
            expected_sql: "NUMERIC(6,5)",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "BigQuery stores smallint as a 64-bit integer",
            column_type: ColumnType::SmallInt,
            warehouse: Warehouse::BigQuery,
            expected_sql: "INT64",
            expected_lossy: true,
        },
        WarehouseTypeTestCase {
            description: "BigQuery varchar enforces its length",
            column_type: ColumnType::Varchar(10),
            warehouse: Warehouse::BigQuery,
            expected_sql: "STRING(10)",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "ClickHouse keeps a decimal's precision and scale, as a Nullable column",
            column_type: ColumnType::Decimal {
                precision: 38,
                scale: 9,
            },
            warehouse: Warehouse::ClickHouse,
            expected_sql: "Nullable(Decimal(38, 9))",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "ClickHouse stores timestamp_tz as a UTC DateTime64 in microseconds, the offset not kept",
            column_type: ColumnType::TimestampTz,
            warehouse: Warehouse::ClickHouse,
            expected_sql: "Nullable(DateTime64(6, 'UTC'))",
            expected_lossy: true,
        },
        WarehouseTypeTestCase {
            description: "ClickHouse has no time type, so a time is its text",
            column_type: ColumnType::Time,
            warehouse: Warehouse::ClickHouse,
            expected_sql: "Nullable(String)",
            expected_lossy: true,
        },
        WarehouseTypeTestCase {
            description: "a ClickHouse native type is used as written, so LowCardinality and JSON stay opt-in",
            column_type: ColumnType::Native("LowCardinality(String)".to_owned()),
            warehouse: Warehouse::ClickHouse,
            expected_sql: "LowCardinality(String)",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "a DuckDB array is a list of its element type",
            column_type: ColumnType::Array(Box::new(ColumnType::Int)),
            warehouse: Warehouse::DuckDb,
            expected_sql: "BIGINT[]",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "a Postgres array keeps its element's precision and scale",
            column_type: ColumnType::Array(Box::new(decimal.clone())),
            warehouse: Warehouse::Postgres,
            expected_sql: "numeric(18,2)[]",
            expected_lossy: false,
        },
        WarehouseTypeTestCase {
            description: "a Databricks array carries its element's loss: a zoned timestamp is a UTC instant",
            column_type: ColumnType::Array(Box::new(ColumnType::TimestampTz)),
            warehouse: Warehouse::Databricks,
            expected_sql: "ARRAY<TIMESTAMP>",
            expected_lossy: true,
        },
        WarehouseTypeTestCase {
            description: "a BigQuery array holds no NULLs",
            column_type: ColumnType::Array(Box::new(decimal.clone())),
            warehouse: Warehouse::BigQuery,
            expected_sql: "ARRAY<NUMERIC(18,2)>",
            expected_lossy: true,
        },
        WarehouseTypeTestCase {
            description: "a ClickHouse array cannot be NULL itself, only its elements",
            column_type: ColumnType::Array(Box::new(ColumnType::Text)),
            warehouse: Warehouse::ClickHouse,
            expected_sql: "Array(Nullable(String))",
            expected_lossy: true,
        },
        WarehouseTypeTestCase {
            description: "a Snowflake array holds its elements as JSON values",
            column_type: ColumnType::Array(Box::new(decimal.clone())),
            warehouse: Warehouse::Snowflake,
            expected_sql: "ARRAY",
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
