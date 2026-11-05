use crate::batch_plan::tests::helpers::cut;
use crate::batch_plan::tests::test_types::CutIndexTestCase;

#[test]
fn given_an_ordered_batch_when_a_commit_may_end_then_it_ends_only_where_the_cursor_changes()
-> Result<(), String> {
    let test_cases = [
        CutIndexTestCase {
            description: "rows still holding the last value stay in the commit",
            values: &[5, 5, 6, 7],
            last: Some("5"),
            expected_cut: 2,
            expected_last: Some("7"),
        },
        CutIndexTestCase {
            description: "a batch all of the last value never cuts",
            values: &[5, 5],
            last: Some("5"),
            expected_cut: 2,
            expected_last: Some("5"),
        },
        CutIndexTestCase {
            description: "without a last value the commit may end before the batch",
            values: &[1, 2],
            last: None,
            expected_cut: 0,
            expected_last: Some("2"),
        },
        CutIndexTestCase {
            description: "an empty batch has no last value",
            values: &[],
            last: Some("3"),
            expected_cut: 0,
            expected_last: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            cut(test_case.values, test_case.last)?,
            (
                test_case.expected_cut,
                test_case.expected_last.map(str::to_owned)
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
