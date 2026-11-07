use crate::adapters::contract::models::ChangeApply;
use crate::cdc::tests::helpers::{
    buffered_seqs, insert_message, relation_message, staged_texts, update_message,
};
use crate::cdc::tests::test_types::{BufferTestCase, StagedShapesTestCase};

/// A relation message takes effect where it sits in its transaction: a change written before a DDL is never read with the later shape, whose columns would not line up with its values.
#[test]
fn given_a_ddl_inside_a_transaction_when_it_commits_then_earlier_changes_keep_their_shape_and_position()
 {
    let test_cases = [
        BufferTestCase {
            description: "a transaction without a DDL buffers",
            messages: vec![
                relation_message(&["id", "a", "b"]),
                insert_message(&["1", "x", "y"]),
                insert_message(&["2", "z", "w"]),
            ],
            expected_seqs: Ok(vec![1, 2]),
        },
        BufferTestCase {
            description: "a relation sent again between changes takes no position, so seq stays as 0.16.0 numbered it",
            messages: vec![
                relation_message(&["id", "a", "b"]),
                insert_message(&["1", "x", "y"]),
                relation_message(&["id", "a", "b"]),
                insert_message(&["2", "z", "w"]),
            ],
            expected_seqs: Ok(vec![1, 2]),
        },
        BufferTestCase {
            description: "an insert, a middle column dropped, then an insert buffers both",
            messages: vec![
                relation_message(&["id", "a", "b"]),
                insert_message(&["1", "x", "y"]),
                relation_message(&["id", "b"]),
                insert_message(&["2", "z"]),
            ],
            expected_seqs: Ok(vec![1, 2]),
        },
    ];
    for test_case in test_cases {
        for spill_bytes in [usize::MAX, 1] {
            assert_eq!(
                buffered_seqs(&test_case.messages, spill_bytes),
                test_case.expected_seqs.clone().map_err(str::to_owned),
                "{} (keeping {spill_bytes} bytes in memory)",
                test_case.description
            );
        }
    }
}

/// A table that changes shape within a batch stages every change in its own columns: a column a change's shape lacked is written as left out, so `merge` keeps the value it had (from the key's earlier change in the batch, else the live row), and a changelog writes NULL and flags it.
#[test]
fn given_a_shape_change_in_a_batch_when_it_stages_then_each_change_keeps_its_own_columns()
-> Result<(), String> {
    let test_cases = [
        StagedShapesTestCase {
            description: "a column added between transactions",
            transactions: vec![
                vec![
                    relation_message(&["id", "a"]),
                    insert_message(&["1", "x"]),
                    insert_message(&["2", "y"]),
                ],
                vec![
                    relation_message(&["id", "a", "b"]),
                    update_message(&["1", "x2", "z"]),
                    insert_message(&["3", "w", "v"]),
                ],
            ],
            columns: vec!["id", "a", "b"],
            expected_merge: vec!["1,x2,z|-", "2,y,-|,b,", "3,w,v|-"],
            expected_changelog: vec!["1,x,-|,b,", "1,x2,z|-", "2,y,-|,b,", "3,w,v|-"],
        },
        StagedShapesTestCase {
            description: "a middle column dropped between transactions keeps the key's earlier value under merge",
            transactions: vec![
                vec![
                    relation_message(&["id", "a", "b"]),
                    insert_message(&["1", "x", "y"]),
                ],
                vec![
                    relation_message(&["id", "b"]),
                    update_message(&["1", "z"]),
                    insert_message(&["2", "w"]),
                ],
            ],
            columns: vec!["id", "a", "b"],
            expected_merge: vec!["1,x,z|-", "2,-,w|,a,"],
            expected_changelog: vec!["1,-,z|,a,", "1,x,y|-", "2,-,w|,a,"],
        },
        StagedShapesTestCase {
            description: "a middle column dropped inside one transaction",
            transactions: vec![vec![
                relation_message(&["id", "a", "b"]),
                insert_message(&["1", "x", "y"]),
                relation_message(&["id", "b"]),
                insert_message(&["2", "z"]),
            ]],
            columns: vec!["id", "a", "b"],
            expected_merge: vec!["1,x,y|-", "2,-,z|,a,"],
            expected_changelog: vec!["1,x,y|-", "2,-,z|,a,"],
        },
        StagedShapesTestCase {
            description: "a column dropped and a new one added at the end",
            transactions: vec![
                vec![
                    relation_message(&["id", "a", "b"]),
                    insert_message(&["1", "x", "y"]),
                ],
                vec![
                    relation_message(&["id", "b", "c"]),
                    insert_message(&["2", "v", "w"]),
                ],
            ],
            columns: vec!["id", "a", "b", "c"],
            expected_merge: vec!["1,x,y,-|,c,", "2,-,v,w|,a,"],
            expected_changelog: vec!["1,x,y,-|,c,", "2,-,v,w|,a,"],
        },
    ];
    for test_case in test_cases {
        for spill_bytes in [usize::MAX, 1] {
            let staged = [ChangeApply::SoftDeletes, ChangeApply::Changelog].map(|apply| {
                staged_texts(
                    &test_case.transactions,
                    (apply, spill_bytes),
                    &test_case.columns,
                )
            });
            assert_eq!(
                staged,
                [
                    Ok(test_case
                        .expected_merge
                        .iter()
                        .map(|row| (*row).to_owned())
                        .collect()),
                    Ok(test_case
                        .expected_changelog
                        .iter()
                        .map(|row| (*row).to_owned())
                        .collect()),
                ],
                "{} (keeping {spill_bytes} bytes in memory): merge, then changelog",
                test_case.description
            );
        }
    }
    Ok(())
}
