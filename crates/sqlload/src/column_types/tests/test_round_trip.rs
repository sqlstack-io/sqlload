//! The type SQLLoad creates in a warehouse, declared back for a table loading only there, reads back as the same portable type.

use crate::column_types::models::{ColumnType, Warehouse};
use crate::column_types::tests::helpers::{compared_read_backs, expected_read_backs, read_backs};
use crate::column_types::tests::test_types::{RoundTripException, RoundTripTestCase};

/// ClickHouse (each column `Nullable(...)`) and Parquet files (Arrow types) are left out: a declaration takes neither's types. Arrays are declared as `array(T)` everywhere.
#[test]
fn given_each_portable_type_when_its_warehouse_type_is_declared_for_that_warehouse_then_it_reads_back_as_the_same_type_but_for_the_listed_stand_ins()
 {
    let test_cases = [
        RoundTripTestCase {
            description: "Snowflake",
            warehouse: Warehouse::Snowflake,
            expected_exceptions: vec![
                RoundTripException {
                    description: "Snowflake's integers are all NUMBER(38,0), which holds every 38-digit integer",
                    column_type: ColumnType::SmallInt,
                    expected_rendered: "NUMBER(38,0)",
                    expected_read_back: ColumnType::Decimal {
                        precision: 38,
                        scale: 0,
                    },
                },
                RoundTripException {
                    description: "Snowflake's integers are all NUMBER(38,0), which holds every 38-digit integer",
                    column_type: ColumnType::Int,
                    expected_rendered: "NUMBER(38,0)",
                    expected_read_back: ColumnType::Decimal {
                        precision: 38,
                        scale: 0,
                    },
                },
                RoundTripException {
                    description: "Snowflake has no uuid type; a uuid is stored as its 36 characters of text",
                    column_type: ColumnType::Uuid,
                    expected_rendered: "VARCHAR(36)",
                    expected_read_back: ColumnType::Varchar(36),
                },
            ],
        },
        RoundTripTestCase {
            description: "DuckDB (and MotherDuck)",
            warehouse: Warehouse::DuckDb,
            expected_exceptions: vec![RoundTripException {
                description: "DuckDB doesn't enforce a VARCHAR length, so SQLLoad creates VARCHAR, which is text",
                column_type: ColumnType::Varchar(20),
                expected_rendered: "VARCHAR",
                expected_read_back: ColumnType::Text,
            }],
        },
        RoundTripTestCase {
            description: "Databricks",
            warehouse: Warehouse::Databricks,
            expected_exceptions: vec![
                RoundTripException {
                    description: "SQLLoad doesn't enforce a length on Databricks, so it creates STRING, which is text",
                    column_type: ColumnType::Varchar(20),
                    expected_rendered: "STRING",
                    expected_read_back: ColumnType::Text,
                },
                RoundTripException {
                    description: "Databricks has no time type; a time is stored as text",
                    column_type: ColumnType::Time,
                    expected_rendered: "STRING",
                    expected_read_back: ColumnType::Text,
                },
                RoundTripException {
                    description: "Databricks has no uuid type; a uuid is stored as text",
                    column_type: ColumnType::Uuid,
                    expected_rendered: "STRING",
                    expected_read_back: ColumnType::Text,
                },
            ],
        },
        RoundTripTestCase {
            description: "BigQuery",
            warehouse: Warehouse::BigQuery,
            expected_exceptions: vec![
                RoundTripException {
                    description: "BigQuery's only integer is INT64",
                    column_type: ColumnType::SmallInt,
                    expected_rendered: "INT64",
                    expected_read_back: ColumnType::Int,
                },
                RoundTripException {
                    description: "BigQuery's NUMERIC keeps a digit before the point, so a decimal of only fraction digits gets one more",
                    column_type: ColumnType::Decimal {
                        precision: 5,
                        scale: 5,
                    },
                    expected_rendered: "NUMERIC(6,5)",
                    expected_read_back: ColumnType::Decimal {
                        precision: 6,
                        scale: 5,
                    },
                },
                RoundTripException {
                    description: "BigQuery has no uuid type; a uuid is stored as text",
                    column_type: ColumnType::Uuid,
                    expected_rendered: "STRING",
                    expected_read_back: ColumnType::Text,
                },
            ],
        },
        RoundTripTestCase {
            description: "Postgres",
            warehouse: Warehouse::Postgres,
            expected_exceptions: vec![],
        },
    ];
    for test_case in test_cases {
        let found = compared_read_backs(
            read_backs(test_case.warehouse),
            &test_case.expected_exceptions,
        );
        assert_eq!(
            found,
            expected_read_backs(&test_case.expected_exceptions),
            "{}; stand-ins: {:?}",
            test_case.description,
            test_case
                .expected_exceptions
                .iter()
                .map(|exception| exception.description)
                .collect::<Vec<_>>()
        );
    }
}
