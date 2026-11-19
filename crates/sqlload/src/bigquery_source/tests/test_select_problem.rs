use serde_json::json;

use crate::bigquery_source::_helpers::catalog::listed_table;
use crate::bigquery_source::main::select_problem::select_problem;
use crate::bigquery_source::tests::test_types::SelectProblemTestCase;
use crate::column_types::models::ColumnType;

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
            location: "EU",
            expected_problem: None,
        },
        SelectProblemTestCase {
            description: "a repeated column, which SQLLoad reads as JSON, keeps SQLLoad's reader",
            fields: vec![
                json!({"name": "id", "type": "INT64"}),
                json!({"name": "tags", "type": "STRING", "mode": "REPEATED"}),
            ],
            pinned: Vec::new(),
            location: "EU",
            expected_problem: Some(
                "tags is BigQuery ARRAY<STRING>, which SQLLoad converts to json",
            ),
        },
        SelectProblemTestCase {
            description: "a record keeps SQLLoad's reader",
            fields: vec![json!({"name": "address", "type": "RECORD"})],
            pinned: Vec::new(),
            location: "EU",
            expected_problem: Some("address is BigQuery STRUCT, which SQLLoad converts to json"),
        },
        SelectProblemTestCase {
            description: "geography, read as its WKT text, keeps SQLLoad's reader",
            fields: vec![json!({"name": "place", "type": "GEOGRAPHY"})],
            pinned: Vec::new(),
            location: "EU",
            expected_problem: Some("place is BigQuery GEOGRAPHY, which SQLLoad converts to text"),
        },
        SelectProblemTestCase {
            description: "a column pinned to another type keeps SQLLoad's reader, which converts it",
            fields: vec![json!({"name": "id", "type": "INT64"})],
            pinned: vec![("id", ColumnType::Text)],
            location: "EU",
            expected_problem: Some(
                "id is pinned as text, which SQLLoad converts from BigQuery INT64",
            ),
        },
        SelectProblemTestCase {
            description: "a pin to the column's own type changes nothing",
            fields: vec![json!({"name": "id", "type": "INT64"})],
            pinned: vec![("id", ColumnType::Int)],
            location: "EU",
            expected_problem: None,
        },
        SelectProblemTestCase {
            description: "a table in another location than the connection's query jobs keeps SQLLoad's reader, which reads it from anywhere",
            fields: vec![json!({"name": "id", "type": "INT64"})],
            pinned: Vec::new(),
            location: "US",
            expected_problem: Some(
                "the table is in US, not the location query jobs on its connection run in",
            ),
        },
        SelectProblemTestCase {
            description: "a location written in another case is the same location",
            fields: vec![json!({"name": "id", "type": "INT64"})],
            pinned: Vec::new(),
            location: "eu",
            expected_problem: None,
        },
    ];
    for test_case in test_cases {
        let table = listed_table(
            ("shop".to_owned(), "orders".to_owned()),
            &json!({"type": "TABLE", "location": test_case.location, "schema": {"fields": test_case.fields}}),
            "EU",
        );
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
