use crate::loader::_helpers::table_plan::source_filter;
use crate::loader::tests::test_types::SourceFilterTestCase;

#[test]
fn given_cursor_bounds_when_rendering_the_source_filter_then_only_numeric_cursors_compare_bare_numbers()
 {
    let test_cases = [
        SourceFilterTestCase {
            description: "a numeric cursor's bounds are bare numbers, which BigQuery compares with an INT64 column",
            numeric: true,
            lower: Some((">", "41")),
            upper: Some("100.5"),
            expected_filter: Some("`id` > 41 AND `id` < 100.5"),
        },
        SourceFilterTestCase {
            description: "a text cursor holding digits stays a string",
            numeric: false,
            lower: Some((">=", "41")),
            upper: None,
            expected_filter: Some("`id` >= '41'"),
        },
        SourceFilterTestCase {
            description: "a numeric cursor's bound that isn't a plain number is quoted, never written bare",
            numeric: true,
            lower: Some((">=", "1 OR 1=1")),
            upper: Some("NaN"),
            expected_filter: Some("`id` >= '1 OR 1=1' AND `id` < 'NaN'"),
        },
        SourceFilterTestCase {
            description: "no bounds, no filter",
            numeric: true,
            lower: None,
            upper: None,
            expected_filter: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            source_filter("`id`", test_case.numeric, test_case.lower, test_case.upper).as_deref(),
            test_case.expected_filter,
            "{}",
            test_case.description
        );
    }
}
