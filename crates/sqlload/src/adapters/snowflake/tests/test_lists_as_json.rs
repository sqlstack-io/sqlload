use crate::adapters::snowflake::tests::helpers::staged_float_lists;
use crate::adapters::snowflake::tests::test_types::ListsAsJsonTestCase;

#[test]
fn given_float_arrays_when_staging_for_snowflake_then_values_json_holds_load_and_nan_fails_by_row()
{
    let test_cases = [
        ListsAsJsonTestCase {
            description: "finite floats, NULL elements, empty and NULL arrays load as JSON text",
            rows: vec![Some(vec![Some(1.5), None]), Some(vec![]), None],
            expected_text: Ok("[1.5,null]|[]|null"),
        },
        ListsAsJsonTestCase {
            description: "NaN would load as a NULL element, so the batch fails, naming the row",
            rows: vec![Some(vec![Some(1.5)]), Some(vec![Some(2.0), Some(f64::NAN)])],
            expected_text: Err(
                "column ratios, row 2: an array holds NaN or an infinity, which a Snowflake array holds as JSON and cannot keep; pin array(text) to keep such values as text",
            ),
        },
        ListsAsJsonTestCase {
            description: "an infinity fails the same way",
            rows: vec![Some(vec![Some(f64::NEG_INFINITY)])],
            expected_text: Err(
                "column ratios, row 1: an array holds NaN or an infinity, which a Snowflake array holds as JSON and cannot keep; pin array(text) to keep such values as text",
            ),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            staged_float_lists(test_case.rows),
            test_case
                .expected_text
                .map(str::to_owned)
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
