use crate::column_types::models::ColumnType;
use crate::databricks_source::_helpers::query::select_query;
use crate::databricks_source::models::DatabricksReadRequest;
use crate::databricks_source::tests::test_types::SelectQueryTestCase;
use crate::postgres_source::models::{ReadColumn, SourceTable, WireType};

#[test]
fn given_a_table_or_query_when_selecting_it_then_names_are_backquoted_in_the_catalog_and_a_query_keeps_the_filter_outside()
 {
    let table = SourceTable {
        split_pages: None,
        schema: "sales".to_owned(),
        name: "or`ders".to_owned(),
        columns: Vec::new(),
        primary_key: Vec::new(),
        indexed_columns: Vec::new(),
        estimated_rows: -1,
        estimated_bytes: -1,
        comment: None,
        listed_keys: Vec::new(),
    };
    let columns = [ReadColumn {
        name: "id".to_owned(),
        expression: "`id`".to_owned(),
        wire: WireType::Text,
        output: ColumnType::Int,
    }];
    let test_cases = [
        SelectQueryTestCase {
            description: "a table is catalog.schema.table, a backquote in a name doubled",
            sql: None,
            filter: Some("`id` > 3"),
            expected_query: "SELECT `id` FROM `main`.`sales`.`or``ders` WHERE `id` > 3",
        },
        SelectQueryTestCase {
            description: "a query loses its final semicolon and a trailing comment ends before the parenthesis",
            sql: Some("  SELECT 1 AS id -- latest only\n;\n"),
            filter: Some("`id` > 3"),
            expected_query: "SELECT `id` FROM (SELECT 1 AS id -- latest only\n) WHERE `id` > 3",
        },
        SelectQueryTestCase {
            description: "no filter, no WHERE",
            sql: None,
            filter: None,
            expected_query: "SELECT `id` FROM `main`.`sales`.`or``ders`",
        },
    ];
    for test_case in test_cases {
        let request = DatabricksReadRequest {
            table: &table,
            sql: test_case.sql,
            columns: &columns,
            filter: test_case.filter,
        };
        assert_eq!(
            select_query("main", &request),
            test_case.expected_query,
            "{}",
            test_case.description
        );
    }
}
