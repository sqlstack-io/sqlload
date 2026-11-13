use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::models::DatabricksSql;
use crate::adapters::databricks::tests::helpers::{added, live, table};
use crate::adapters::databricks::tests::test_types::{ChangeColumnsTestCase, RenderedTestCase};
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::ColumnChange;

#[test]
fn given_column_changes_when_rendering_them_for_delta_then_the_table_features_they_need_come_first()
{
    let test_cases = [
        ChangeColumnsTestCase {
            description: "added columns go in one ADD COLUMNS, with no IF NOT EXISTS (Databricks refuses it)",
            live: vec![live("id", ColumnType::Int)],
            changes: vec![
                added("note", ColumnType::Text, "STRING"),
                added("qty", ColumnType::Int, "BIGINT"),
            ],
            expected_statements: &[
                "ALTER TABLE `main`.`raw`.`orders` ADD COLUMNS (`note` STRING, `qty` BIGINT)",
            ],
        },
        ChangeColumnsTestCase {
            description: "an added TIMESTAMP_NTZ column turns on the timestampNtz feature first",
            live: vec![live("id", ColumnType::Int)],
            changes: vec![added("seen", ColumnType::TimestampNtz, "TIMESTAMP_NTZ")],
            expected_statements: &[
                "ALTER TABLE `main`.`raw`.`orders` SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')",
                "ALTER TABLE `main`.`raw`.`orders` ADD COLUMNS (`seen` TIMESTAMP_NTZ)",
            ],
        },
        ChangeColumnsTestCase {
            description: "widening in place turns on type widening first",
            live: vec![live(
                "amount",
                ColumnType::Decimal {
                    precision: 10,
                    scale: 2,
                },
            )],
            changes: vec![ColumnChange::Widen {
                column: "amount".to_owned(),
                from: ColumnType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                to: ColumnType::Decimal {
                    precision: 20,
                    scale: 2,
                },
                sql_type: "DECIMAL(20,2)".to_owned(),
            }],
            expected_statements: &[
                "ALTER TABLE `main`.`raw`.`orders` SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
                "ALTER TABLE `main`.`raw`.`orders` ALTER COLUMN `amount` TYPE DECIMAL(20,2)",
            ],
        },
        ChangeColumnsTestCase {
            description: "a rebuild rewrites the table from itself, keeping column order, before the columns are added",
            live: vec![live("id", ColumnType::Int), live("Qty", ColumnType::Int)],
            changes: vec![
                ColumnChange::Rebuild {
                    column: "Qty".to_owned(),
                    from: ColumnType::Int,
                    to: ColumnType::Float,
                    sql_type: "DOUBLE".to_owned(),
                },
                added("note", ColumnType::Text, "STRING"),
            ],
            expected_statements: &[
                "CREATE OR REPLACE TABLE `main`.`raw`.`orders` AS SELECT `id`, CAST(`Qty` AS DOUBLE) AS `Qty` FROM `main`.`raw`.`orders`",
                "ALTER TABLE `main`.`raw`.`orders` ADD COLUMNS (`note` STRING)",
            ],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            DatabricksSql.render_change_columns(&table(), &test_case.changes, &test_case.live),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_an_unnested_table_when_listing_its_child_tables_then_its_catalog_is_read_by_lower_cased_names()
 {
    let test_cases = [RenderedTestCase {
        description: "the catalog's information_schema, as statements run with no default catalog, by the lower-cased names Unity Catalog reports",
        rendered: DatabricksSql.list_child_tables_sql(&TableName {
            database: Some("Sales".to_owned()),
            schema: "Raw".to_owned(),
            table: "Orders".to_owned(),
        }),
        expected_statement: "SELECT max(CASE WHEN column_name IN ('_sqlload_parent_id', '_dlt_parent_id') THEN '1' ELSE '0' END) || table_name FROM `Sales`.information_schema.columns WHERE table_schema = 'raw' AND left(table_name, 8) = 'orders__' GROUP BY table_name",
    }];
    for test_case in test_cases {
        assert_eq!(
            test_case.rendered, test_case.expected_statement,
            "{}",
            test_case.description
        );
    }
}
