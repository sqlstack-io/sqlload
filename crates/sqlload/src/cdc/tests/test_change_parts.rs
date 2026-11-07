use crate::cdc::models::ChangeOp::{Delete, Insert, Truncate, Update};
use crate::cdc::tests::helpers::{change, staged_rows};
use crate::cdc::tests::test_types::ChangePartsTestCase;

/// A spilled batch stages exactly what the same batch in memory stages, whether its keys share one partition or spread over many, and wherever the spill boundary falls.
#[test]
fn given_a_batch_when_it_spills_then_merge_stages_the_same_rows_as_in_memory() -> Result<(), String>
{
    let test_cases = [
        ChangePartsTestCase {
            description: "a row moved to a new key and back deletes the key it left",
            changes: vec![
                change(Update, (10, 1), ["2", "a"], Some("1")),
                change(Update, (10, 2), ["1", "b"], Some("2")),
            ],
            spilled: 1,
            expected_rows: (vec!["delete:2:-", "update:1:b"], None),
        },
        ChangePartsTestCase {
            description: "a chain of moves deletes every key but the last",
            changes: vec![
                change(Update, (10, 1), ["2", "a"], Some("1")),
                change(Update, (11, 1), ["3", "b"], Some("2")),
            ],
            spilled: 1,
            expected_rows: (vec!["delete:1:-", "delete:2:-", "update:3:b"], None),
        },
        ChangePartsTestCase {
            description: "a row moved then deleted deletes both keys",
            changes: vec![
                change(Update, (10, 1), ["2", "a"], Some("1")),
                change(Delete, (11, 1), ["2", "-"], None),
            ],
            spilled: 2,
            expected_rows: (vec!["delete:1:-", "delete:2:-"], None),
        },
        ChangePartsTestCase {
            description: "a row moved into a key the batch already inserted takes its place",
            changes: vec![
                change(Insert, (10, 1), ["2", "a"], None),
                change(Delete, (10, 2), ["2", "-"], None),
                change(Update, (11, 1), ["2", "b"], Some("1")),
            ],
            spilled: 2,
            expected_rows: (vec!["delete:1:-", "update:2:b"], None),
        },
        ChangePartsTestCase {
            description: "only the changes after the newest of two truncates are staged",
            changes: vec![
                change(Insert, (1, 1), ["1", "a"], None),
                change(Truncate, (1, 2), ["-", "-"], None),
                change(Insert, (2, 1), ["2", "b"], None),
                change(Truncate, (3, 1), ["-", "-"], None),
                change(Insert, (4, 1), ["3", "c"], None),
            ],
            spilled: 4,
            expected_rows: (vec!["insert:3:c"], Some(3)),
        },
        ChangePartsTestCase {
            description: "an unchanged TOAST value carries from the disk side to the memory side",
            changes: vec![
                change(Insert, (1, 1), ["1", "big"], None),
                change(Update, (2, 1), ["1", "~"], None),
                change(Update, (3, 1), ["9", "~"], None),
            ],
            spilled: 1,
            expected_rows: (vec!["update:1:big", "update:9:~"], None),
        },
    ];
    for test_case in test_cases {
        let staged = [
            staged_rows(&test_case.changes, None, 1)?,
            staged_rows(&test_case.changes, Some(test_case.spilled), 1)?,
            staged_rows(&test_case.changes, Some(test_case.spilled), usize::MAX)?,
        ];
        let shown: Vec<(Vec<&str>, Option<i64>)> = staged
            .iter()
            .map(|(rows, truncated)| (rows.iter().map(String::as_str).collect(), *truncated))
            .collect();
        assert_eq!(
            shown,
            vec![test_case.expected_rows.clone(); 3],
            "{}: in memory, spilled over many partitions, spilled into one",
            test_case.description
        );
    }
    Ok(())
}
