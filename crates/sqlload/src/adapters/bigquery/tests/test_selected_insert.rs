use crate::adapters::bigquery::_helpers::dialect::selected::{SelectedLoad, selected_insert};
use crate::adapters::bigquery::tests::helpers::{desired, orders};
use crate::adapters::bigquery::tests::test_types::SelectedInsertTestCase;
use crate::column_types::models::ColumnType;
use crate::load_naming::models::Compat;
use crate::postgres_source::models::{ReadColumn, WireType};

#[test]
fn given_a_bigquery_source_on_the_targets_connection_when_bigquery_selects_its_rows_itself_then_each_column_comes_from_the_right_place()
 {
    let read = [
        ReadColumn {
            name: "id".to_owned(),
            expression: "`Order ID`".to_owned(),
            wire: WireType::Text,
            output: ColumnType::Int,
        },
        ReadColumn {
            name: "amount".to_owned(),
            expression: "`amount`".to_owned(),
            wire: WireType::Text,
            output: ColumnType::Decimal {
                precision: 12,
                scale: 2,
            },
        },
        ReadColumn {
            name: "doc".to_owned(),
            expression: "`doc`".to_owned(),
            wire: WireType::Text,
            output: ColumnType::Json,
        },
    ];
    let constants = [("region".to_owned(), ColumnType::Varchar(4), "eu".to_owned())];
    let columns = vec![
        desired("id", ColumnType::Int),
        desired(
            "amount",
            ColumnType::Decimal {
                precision: 12,
                scale: 2,
            },
        ),
        desired("doc", ColumnType::Json),
        desired("region", ColumnType::Varchar(4)),
        desired("gone", ColumnType::Text),
        desired("_sqlload_row", ColumnType::Int),
        desired("_sqlload_load_id", ColumnType::Text),
        desired("_sqlload_loaded_at", ColumnType::TimestampTz),
    ];
    let test_cases = [
        SelectedInsertTestCase {
            description: "into the staging table: each read column by its name in the source's query, cast without parameters, JSON as its text, a constant from its value, a column the source lacks NULL, a merge's row number after the earlier batches' among the rows sharing its key (a constant key column groups every row alike), and the batch's load id and time",
            columns: columns.clone(),
            builds: false,
            expected_insert: "INSERT INTO `p`.`raw`.`orders` (`id`, `amount`, `doc`, `region`, `gone`, `_sqlload_row`, `_sqlload_load_id`, `_sqlload_loaded_at`) SELECT CAST(u.`Order ID` AS INT64), CAST(u.`amount` AS NUMERIC), TO_JSON_STRING(u.`doc`), CAST('eu' AS STRING), CAST(NULL AS STRING), 3 * 281474976710656 + ROW_NUMBER() OVER (PARTITION BY u.`Order ID`) - 1, CAST('L1' AS STRING), CAST('2026-11-06T00:00:00Z' AS TIMESTAMP) FROM (SELECT `Order ID`, `amount`, `doc` FROM `p.marts.orders`) AS u",
        },
        SelectedInsertTestCase {
            description: "into a build table JSON stays JSON",
            columns: vec![desired("doc", ColumnType::Json)],
            builds: true,
            expected_insert: "INSERT INTO `p`.`raw`.`orders` (`doc`) SELECT u.`doc` FROM (SELECT `Order ID`, `amount`, `doc` FROM `p.marts.orders`) AS u",
        },
    ];
    for test_case in test_cases {
        let insert = selected_insert(&SelectedLoad {
            into: &orders(),
            columns: &test_case.columns,
            read: &read,
            constants: &constants,
            compat: Compat::Sqlload,
            batch: 3,
            merge_key: &["id".to_owned(), "region".to_owned()],
            builds: test_case.builds,
            load_id: "L1",
            loaded_at: "2026-11-06T00:00:00Z",
            query: "SELECT `Order ID`, `amount`, `doc` FROM `p.marts.orders`",
        });
        assert_eq!(
            insert, test_case.expected_insert,
            "{}",
            test_case.description
        );
    }
}
