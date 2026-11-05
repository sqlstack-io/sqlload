use crate::column_types::models::{ColumnType, Warehouse};
use crate::write_sql::tests::helpers::changed_staging;
use crate::write_sql::tests::test_types::StagingChangesTestCase;

#[test]
fn given_a_wider_plan_mid_load_when_changing_the_staging_table_then_it_holds_the_new_columns() {
    let test_cases = [
        StagingChangesTestCase {
            description: "nothing changed",
            warehouse: Warehouse::Snowflake,
            new_types: &[ColumnType::Int, ColumnType::Int],
            expected_statements: &[],
        },
        StagingChangesTestCase {
            description: "a new column is added",
            warehouse: Warehouse::DuckDb,
            new_types: &[ColumnType::Int, ColumnType::Int, ColumnType::Text],
            expected_statements: &[
                "ALTER TABLE \"raw\".\"app__accounts__sqlload_staging_load1\" ADD COLUMN \"note\" VARCHAR",
            ],
        },
        StagingChangesTestCase {
            description: "DuckDB widens in place",
            warehouse: Warehouse::DuckDb,
            new_types: &[ColumnType::Int, ColumnType::Float],
            expected_statements: &[
                "ALTER TABLE \"raw\".\"app__accounts__sqlload_staging_load1\" ALTER COLUMN \"score\" TYPE DOUBLE",
            ],
        },
        StagingChangesTestCase {
            description: "Snowflake rewrites the staging table, whose ALTER cannot turn a number into a float",
            warehouse: Warehouse::Snowflake,
            new_types: &[ColumnType::Int, ColumnType::Float, ColumnType::Text],
            expected_statements: &[
                "CREATE OR REPLACE TABLE \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\" COPY GRANTS AS SELECT \"ID\", CAST(\"SCORE\" AS FLOAT) AS \"SCORE\" FROM \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\"",
                "ALTER TABLE \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_LOAD1\" ADD COLUMN \"NOTE\" VARCHAR",
            ],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            changed_staging(test_case.new_types, test_case.warehouse),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}
