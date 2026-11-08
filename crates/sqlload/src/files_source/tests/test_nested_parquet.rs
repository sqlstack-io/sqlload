use crate::files_source::tests::helpers::{block_on, nested_parquet_column};
use crate::files_source::tests::test_types::NestedParquetTestCase;

#[test]
fn given_parquet_lists_structs_and_maps_when_reading_then_each_loads_as_json() -> Result<(), String>
{
    let test_cases = [
        NestedParquetTestCase {
            description: "a list is a JSON array: a NULL element is null, an empty list [], a NULL list NULL",
            column: "tags",
            expected_type: "json",
            expected_values: "[\"a\",null,\"say \\\"hi\\\"\"]|[]|NULL",
        },
        NestedParquetTestCase {
            description: "a struct is an object with its fields in order",
            column: "point",
            expected_type: "json",
            expected_values: "{\"x\":1.5,\"y\":1}|{\"x\":-2.25,\"y\":2}|{\"x\":null,\"y\":3}",
        },
        NestedParquetTestCase {
            description: "a map is an object keyed by its keys",
            column: "attrs",
            expected_type: "json",
            expected_values: "{\"k\":1}|{}|{\"a\":null}",
        },
        NestedParquetTestCase {
            description: "lists nest",
            column: "grid",
            expected_type: "json",
            expected_values: "[[1,2],[3]]|[]|[null]",
        },
        NestedParquetTestCase {
            description: "a field of Arrow's NULL type is null, not empty text",
            column: "opt",
            expected_type: "json",
            expected_values: "{\"a\":null,\"b\":1}|{\"a\":null,\"b\":2}|{\"a\":null,\"b\":3}",
        },
        NestedParquetTestCase {
            description: "dictionary-encoded values inside a list keep their type",
            column: "codes",
            expected_type: "json",
            expected_values: "[5,7]|[5]|[]",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            block_on(nested_parquet_column(test_case.column))?,
            (
                test_case.expected_type.to_owned(),
                test_case.expected_values.to_owned()
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
