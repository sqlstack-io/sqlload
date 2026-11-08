use crate::column_types::models::ColumnType;
use crate::row_batches::tests::helpers::shown_json_lists;
use crate::row_batches::tests::test_types::JsonListsTestCase;

#[test]
fn given_json_array_text_when_reading_it_as_lists_then_each_element_converts_exactly_or_the_rows_fail()
 {
    let test_cases = [
        JsonListsTestCase {
            description: "decimals keep every digit, whether written as numbers or strings; a NULL array, an empty one and a null element stay apart",
            texts: &[
                Some("[\"1.50\", -2.25, null, 12345678901234567890123456.78]"),
                None,
                Some("[]"),
            ],
            element: ColumnType::Decimal {
                precision: 38,
                scale: 2,
            },
            expected_lists: Ok("[1.50, -2.25, null, 12345678901234567890123456.78]|null|[]"),
        },
        JsonListsTestCase {
            description: "a zoned timestamp keeps its microseconds as a UTC instant",
            texts: &[Some("[\"2026-11-08T10:00:00.123456+01:00\"]")],
            element: ColumnType::TimestampTz,
            expected_lists: Ok("[2026-11-08T09:00:00.123456Z]"),
        },
        JsonListsTestCase {
            description: "text keeps its quotes and commas",
            texts: &[Some("[\"say \\\"hi\\\"\", \"x,y\"]")],
            element: ColumnType::Text,
            expected_lists: Ok("[say \"hi\", x,y]"),
        },
        JsonListsTestCase {
            description: "a fraction in an integer array fails rather than truncating",
            texts: &[Some("[1, 1.5]")],
            element: ColumnType::Int,
            expected_lists: Err("an element does not convert to List(Int64)"),
        },
        JsonListsTestCase {
            description: "a nested array is not a single value",
            texts: &[Some("[1]"), Some("[[1]]")],
            element: ColumnType::Int,
            expected_lists: Err("row 2"),
        },
        JsonListsTestCase {
            description: "an object is not an array",
            texts: &[Some("{\"a\": 1}")],
            element: ColumnType::Int,
            expected_lists: Err("row 1"),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            shown_json_lists(test_case.texts, &test_case.element),
            test_case
                .expected_lists
                .map(str::to_owned)
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
