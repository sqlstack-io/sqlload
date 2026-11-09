use std::collections::BTreeMap;

use crate::bigquery_source::main::read_columns::read_columns;
use crate::bigquery_source::tests::helpers::orders;
use crate::bigquery_source::tests::test_types::ReadColumnsTestCase;
use crate::column_types::models::ColumnType;

#[test]
fn given_pins_when_choosing_read_columns_then_only_readable_ones_pass() {
    let array = |element: ColumnType| ColumnType::Array(Box::new(element));
    let test_cases = [
        ReadColumnsTestCase {
            description: "a repeated column pinned as an array, and INTERVAL pinned as text",
            pinned: vec![
                ("tags", array(ColumnType::Text)),
                ("span", ColumnType::Text),
            ],
            expected_columns: Ok(vec![
                ("`id`", ColumnType::Int),
                ("`tags`", array(ColumnType::Text)),
                ("`span`", ColumnType::Text),
            ]),
        },
        ReadColumnsTestCase {
            description: "an array pin over a scalar, a native pin, and an unpinned INTERVAL are each named",
            pinned: vec![
                ("id", array(ColumnType::Int)),
                ("tags", ColumnType::Native("GEOGRAPHY".to_owned())),
            ],
            expected_columns: Err(vec![
                "id has type INT64, which cannot be read as array(int)",
                "tags has type ARRAY<STRING>, which cannot be read as native(\"GEOGRAPHY\")",
                "span: BigQuery INTERVAL has no portable type; pin one in columns, such as text, or exclude the column",
            ]),
        },
    ];
    for test_case in test_cases {
        let pinned: BTreeMap<String, ColumnType> = test_case
            .pinned
            .into_iter()
            .map(|(name, column_type)| (name.to_owned(), column_type))
            .collect();
        let columns = read_columns(&orders(), &pinned).map(|columns| {
            columns
                .into_iter()
                .map(|column| (column.expression, column.output))
                .collect::<Vec<_>>()
        });
        assert_eq!(
            format!("{columns:?}"),
            format!("{:?}", test_case.expected_columns),
            "{}",
            test_case.description
        );
    }
}
