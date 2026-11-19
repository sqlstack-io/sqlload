use serde_json::json;

use crate::bigquery_source::_helpers::type_map::source_column;
use crate::bigquery_source::main::select_problem::select_problem;
use crate::bigquery_source::tests::helpers::orders;
use crate::bigquery_source::tests::test_types::SelectProblemTestCase;
use crate::column_types::models::ColumnType;
use crate::postgres_source::models::SourceTable;

#[test]
fn given_a_bigquery_table_when_a_bigquery_target_could_select_it_itself_then_only_columns_it_holds_as_they_are_allow_it()
 {
    let test_cases = [
        SelectProblemTestCase {
            description: "scalar columns read as their natural types copy exactly",
            fields: vec![
                json!({"name": "id", "type": "INT64", "mode": "REQUIRED"}),
                json!({"name": "amount", "type": "NUMERIC", "precision": "12", "scale": "2"}),
                json!({"name": "big", "type": "BIGNUMERIC", "precision": "38", "scale": "2"}),
                json!({"name": "at", "type": "TIMESTAMP"}),
                json!({"name": "local", "type": "DATETIME"}),
                json!({"name": "doc", "type": "JSON"}),
                json!({"name": "note", "type": "STRING", "maxLength": "10"}),
                json!({"name": "raw", "type": "BYTES"}),
            ],
            pinned: Vec::new(),
            expected_problem: None,
        },
        SelectProblemTestCase {
            description: "a repeated column, which SQLLoad reads as JSON, keeps SQLLoad's reader",
            fields: vec![
                json!({"name": "id", "type": "INT64"}),
                json!({"name": "tags", "type": "STRING", "mode": "REPEATED"}),
            ],
            pinned: Vec::new(),
            expected_problem: Some(
                "tags is BigQuery ARRAY<STRING>, which SQLLoad converts to json",
            ),
        },
        SelectProblemTestCase {
            description: "a record keeps SQLLoad's reader",
            fields: vec![json!({"name": "address", "type": "RECORD"})],
            pinned: Vec::new(),
            expected_problem: Some("address is BigQuery STRUCT, which SQLLoad converts to json"),
        },
        SelectProblemTestCase {
            description: "geography, read as its WKT text, keeps SQLLoad's reader",
            fields: vec![json!({"name": "place", "type": "GEOGRAPHY"})],
            pinned: Vec::new(),
            expected_problem: Some("place is BigQuery GEOGRAPHY, which SQLLoad converts to text"),
        },
        SelectProblemTestCase {
            description: "a column pinned to another type keeps SQLLoad's reader, which converts it",
            fields: vec![json!({"name": "id", "type": "INT64"})],
            pinned: vec![("id", ColumnType::Text)],
            expected_problem: Some(
                "id is pinned as text, which SQLLoad converts from BigQuery INT64",
            ),
        },
        SelectProblemTestCase {
            description: "a pin to the column's own type changes nothing",
            fields: vec![json!({"name": "id", "type": "INT64"})],
            pinned: vec![("id", ColumnType::Int)],
            expected_problem: None,
        },
    ];
    for test_case in test_cases {
        let table = SourceTable {
            columns: test_case.fields.iter().map(source_column).collect(),
            ..orders()
        };
        let pinned = test_case
            .pinned
            .iter()
            .map(|(name, column_type)| ((*name).to_owned(), column_type.clone()))
            .collect();
        assert_eq!(
            select_problem(&table, &pinned).as_deref(),
            test_case.expected_problem,
            "{}",
            test_case.description
        );
    }
}
