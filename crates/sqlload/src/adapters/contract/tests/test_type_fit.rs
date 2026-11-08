use crate::adapters::contract::tests::helpers::{decimal, live};
use crate::adapters::contract::tests::test_types::TypeFitTestCase;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::{ColumnType, TypeFit, Warehouse};

#[test]
fn given_a_desired_and_live_type_when_comparing_then_the_needed_change_is_named() {
    let test_cases = [
        TypeFitTestCase {
            description: "the same type fits",
            desired: ColumnType::Int,
            live: ColumnType::Int,
            warehouse: Warehouse::Snowflake,
            expected_fit: TypeFit::Fits,
        },
        TypeFitTestCase {
            description: "smallint fits Snowflake's NUMBER(38,0)",
            desired: ColumnType::SmallInt,
            live: ColumnType::Int,
            warehouse: Warehouse::Snowflake,
            expected_fit: TypeFit::Fits,
        },
        TypeFitTestCase {
            description: "uuid fits Snowflake's VARCHAR(36)",
            desired: ColumnType::Uuid,
            live: ColumnType::Varchar(36),
            warehouse: Warehouse::Snowflake,
            expected_fit: TypeFit::Fits,
        },
        TypeFitTestCase {
            description: "a narrower decimal fits a wider one",
            desired: decimal(10, 2),
            live: decimal(18, 4),
            warehouse: Warehouse::Snowflake,
            expected_fit: TypeFit::Fits,
        },
        TypeFitTestCase {
            description: "a longer varchar widens in place",
            desired: ColumnType::Varchar(128),
            live: ColumnType::Varchar(64),
            warehouse: Warehouse::Snowflake,
            expected_fit: TypeFit::WidenInPlace("VARCHAR(128)".to_owned()),
        },
        TypeFitTestCase {
            description: "more precision at the same scale widens in place on Snowflake",
            desired: decimal(18, 2),
            live: decimal(10, 2),
            warehouse: Warehouse::Snowflake,
            expected_fit: TypeFit::WidenInPlace("NUMBER(18,2)".to_owned()),
        },
        TypeFitTestCase {
            description: "int to decimal needs a rebuild on Snowflake",
            desired: decimal(18, 2),
            live: ColumnType::Int,
            warehouse: Warehouse::Snowflake,
            expected_fit: TypeFit::NeedsRebuild("NUMBER(21,2)".to_owned()),
        },
        TypeFitTestCase {
            description: "int to decimal widens in place on DuckDB",
            desired: decimal(18, 2),
            live: ColumnType::Int,
            warehouse: Warehouse::DuckDb,
            expected_fit: TypeFit::WidenInPlace("DECIMAL(21,2)".to_owned()),
        },
        TypeFitTestCase {
            description: "timestamp_ntz to timestamp_tz needs a rebuild on Snowflake",
            desired: ColumnType::TimestampTz,
            live: ColumnType::TimestampNtz,
            warehouse: Warehouse::Snowflake,
            expected_fit: TypeFit::NeedsRebuild("TIMESTAMP_TZ(9)".to_owned()),
        },
        TypeFitTestCase {
            description: "text into an int column is incompatible",
            desired: ColumnType::Text,
            live: ColumnType::Int,
            warehouse: Warehouse::Snowflake,
            expected_fit: TypeFit::Incompatible,
        },
        TypeFitTestCase {
            description: "a smallint column widens to int",
            desired: ColumnType::Int,
            live: ColumnType::SmallInt,
            warehouse: Warehouse::DuckDb,
            expected_fit: TypeFit::WidenInPlace("BIGINT".to_owned()),
        },
        TypeFitTestCase {
            description: "a timestamp with a zone into one without is incompatible",
            desired: ColumnType::TimestampNtz,
            live: ColumnType::TimestampTz,
            warehouse: Warehouse::Snowflake,
            expected_fit: TypeFit::Incompatible,
        },
        TypeFitTestCase {
            description: "a ClickHouse column pinned to LowCardinality(String) fits the column it created, read back as text",
            desired: ColumnType::Native("LowCardinality(String)".to_owned()),
            live: ColumnType::Text,
            warehouse: Warehouse::ClickHouse,
            expected_fit: TypeFit::Fits,
        },
        TypeFitTestCase {
            description: "a ClickHouse column pinned to JSON fits the column it created, read back as json",
            desired: ColumnType::Native("JSON".to_owned()),
            live: ColumnType::Json,
            warehouse: Warehouse::ClickHouse,
            expected_fit: TypeFit::Fits,
        },
        TypeFitTestCase {
            description: "a ClickHouse pin to LowCardinality(String) does not fit a live int column",
            desired: ColumnType::Native("LowCardinality(String)".to_owned()),
            live: ColumnType::Int,
            warehouse: Warehouse::ClickHouse,
            expected_fit: TypeFit::Incompatible,
        },
        TypeFitTestCase {
            description: "a Snowflake array pin fits the ARRAY column it created",
            desired: ColumnType::Array(Box::new(ColumnType::Int)),
            live: sql_adapter(Warehouse::Snowflake).live_type(&live("ARRAY", None, None, None)),
            warehouse: Warehouse::Snowflake,
            expected_fit: TypeFit::Fits,
        },
        TypeFitTestCase {
            description: "a Snowflake array pin does not fit a VARIANT json column",
            desired: ColumnType::Array(Box::new(ColumnType::Int)),
            live: sql_adapter(Warehouse::Snowflake).live_type(&live("VARIANT", None, None, None)),
            warehouse: Warehouse::Snowflake,
            expected_fit: TypeFit::Incompatible,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            sql_adapter(test_case.warehouse).type_fit(&test_case.desired, &test_case.live),
            test_case.expected_fit,
            "{}",
            test_case.description
        );
    }
}
