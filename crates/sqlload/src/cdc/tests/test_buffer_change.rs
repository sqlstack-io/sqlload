use crate::adapters::contract::models::ChangeApply;
use crate::cdc::tests::helpers::{
    buffered_seqs, buffered_shape, floored_lsns, identity_relation_message, insert_message,
    of_other_table, other_relation_message, relation_message, staged_texts, update_message,
};
use crate::cdc::tests::test_types::{
    BufferTestCase, FloorTestCase, StagedShapesTestCase, UnionShapeTestCase,
};

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
            gone: vec![],
            columns: vec!["id", "a", "b"],
            expected_merge: Ok(vec!["1,x2,z|-", "2,y,-|,b,", "3,w,v|-"]),
            expected_changelog: Ok(vec!["1,x,-|,b,", "1,x2,z|-", "2,y,-|,b,", "3,w,v|-"]),
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
            gone: vec![],
            columns: vec!["id", "a", "b"],
            expected_merge: Ok(vec!["1,x,z|-", "2,-,w|,a,"]),
            expected_changelog: Ok(vec!["1,-,z|,a,", "1,x,y|-", "2,-,w|,a,"]),
        },
        StagedShapesTestCase {
            description: "a middle column dropped inside one transaction",
            transactions: vec![vec![
                relation_message(&["id", "a", "b"]),
                insert_message(&["1", "x", "y"]),
                relation_message(&["id", "b"]),
                insert_message(&["2", "z"]),
            ]],
            gone: vec![],
            columns: vec!["id", "a", "b"],
            expected_merge: Ok(vec!["1,x,y|-", "2,-,z|,a,"]),
            expected_changelog: Ok(vec!["1,x,y|-", "2,-,z|,a,"]),
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
            gone: vec![],
            columns: vec!["id", "a", "b", "c"],
            expected_merge: Ok(vec!["1,x,y,-|,c,", "2,-,v,w|,a,"]),
            expected_changelog: Ok(vec!["1,x,y,-|,c,", "2,-,v,w|,a,"]),
        },
        StagedShapesTestCase {
            description: "a planned column no change in the batch shows is left out of each, not refused",
            transactions: vec![vec![
                relation_message(&["id", "a"]),
                insert_message(&["1", "x"]),
                update_message(&["2", "y"]),
            ]],
            gone: vec![],
            columns: vec!["id", "a", "c"],
            expected_merge: Ok(vec!["1,x,-|,c,", "2,y,-|,c,"]),
            expected_changelog: Ok(vec!["1,x,-|,c,", "2,y,-|,c,"]),
        },
        StagedShapesTestCase {
            description: "a value under a name the source lost while a planned column is left out is a rename, refused",
            transactions: vec![
                vec![
                    relation_message(&["id", "a"]),
                    insert_message(&["1", "x"]),
                    insert_message(&["2", "w"]),
                ],
                vec![relation_message(&["id", "b"]), update_message(&["1", "y"])],
            ],
            gone: vec!["a"],
            columns: vec!["id", "b"],
            expected_merge: Err(
                "public.t renamed or replaced column a while changes to it were not yet loaded, so their values cannot be placed; run `sqlload load --resnapshot` to copy every table again",
            ),
            expected_changelog: Err(
                "public.t renamed or replaced column a while changes to it were not yet loaded, so their values cannot be placed; run `sqlload load --resnapshot` to copy every table again",
            ),
        },
        StagedShapesTestCase {
            description: "a rename whose later change leaves the TOASTed value unchanged is refused under merge too, where folding would hide it",
            transactions: vec![
                vec![
                    relation_message(&["id", "a", "c"]),
                    update_message(&["1", "x", "big"]),
                ],
                vec![
                    relation_message(&["id", "a", "d"]),
                    update_message(&["1", "y", "~"]),
                ],
            ],
            gone: vec!["c"],
            columns: vec!["id", "a", "d"],
            expected_merge: Err(
                "public.t renamed or replaced column c while changes to it were not yet loaded, so their values cannot be placed; run `sqlload load --resnapshot` to copy every table again",
            ),
            expected_changelog: Err(
                "public.t renamed or replaced column c while changes to it were not yet loaded, so their values cannot be placed; run `sqlload load --resnapshot` to copy every table again",
            ),
        },
        StagedShapesTestCase {
            description: "a column dropped since, with every planned column present, loads",
            transactions: vec![
                vec![
                    relation_message(&["id", "a", "b"]),
                    insert_message(&["1", "x", "y"]),
                ],
                vec![relation_message(&["id", "b"]), update_message(&["1", "z"])],
            ],
            gone: vec!["a"],
            columns: vec!["id", "b"],
            expected_merge: Ok(vec!["1,z|-"]),
            expected_changelog: Ok(vec!["1,y|-", "1,z|-"]),
        },
    ];
    for test_case in test_cases {
        for spill_bytes in [usize::MAX, 1] {
            let staged = [ChangeApply::SoftDeletes, ChangeApply::Changelog].map(|apply| {
                staged_texts(
                    &test_case.transactions,
                    (apply, spill_bytes),
                    (&test_case.columns, &test_case.gone),
                )
            });
            let owned = |rows: &Result<Vec<&str>, &str>| {
                rows.clone()
                    .map(|rows| rows.iter().map(|row| (*row).to_owned()).collect())
                    .map_err(str::to_owned)
            };
            assert_eq!(
                staged,
                [
                    owned(&test_case.expected_merge),
                    owned(&test_case.expected_changelog),
                ],
                "{} (keeping {spill_bytes} bytes in memory): merge, then changelog",
                test_case.description
            );
        }
    }
    Ok(())
}

/// A batch shape keys a column only while every shape keyed it, and is replica identity full only while every shape was, so a change written under a narrower identity keeps its key.
#[test]
fn given_a_replica_identity_change_in_a_batch_when_it_buffers_then_the_shape_keeps_the_narrower_identity()
-> Result<(), String> {
    let test_cases = [
        UnionShapeTestCase {
            description: "default, then full: still default, keyed by id only",
            transactions: vec![
                vec![relation_message(&["id", "a"]), insert_message(&["1", "x"])],
                vec![
                    identity_relation_message(&["id", "a"], 'f'),
                    insert_message(&["2", "y"]),
                ],
            ],
            expected_shape: ('d', vec![true, false]),
        },
        UnionShapeTestCase {
            description: "full, then default: default, keyed by id only",
            transactions: vec![
                vec![
                    identity_relation_message(&["id", "a"], 'f'),
                    insert_message(&["1", "x"]),
                ],
                vec![relation_message(&["id", "a"]), insert_message(&["2", "y"])],
            ],
            expected_shape: ('d', vec![true, false]),
        },
        UnionShapeTestCase {
            description: "full throughout stays full",
            transactions: vec![vec![
                identity_relation_message(&["id", "a"], 'f'),
                insert_message(&["1", "x"]),
                identity_relation_message(&["id", "a", "b"], 'f'),
                insert_message(&["2", "y", "z"]),
            ]],
            expected_shape: ('f', vec![true, true, true]),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            buffered_shape(&test_case.transactions)?,
            test_case.expected_shape,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

/// A table's changes committed before its copy point are already in its copy, so the buffer drops them; those at or after it are kept.
#[test]
fn given_a_table_copied_at_a_point_when_changes_stream_then_only_those_from_the_point_are_kept()
-> Result<(), String> {
    let transactions = || {
        vec![
            vec![relation_message(&["id", "a"]), insert_message(&["1", "x"])],
            vec![update_message(&["1", "y"]), insert_message(&["2", "z"])],
            vec![update_message(&["2", "w"])],
        ]
    };
    let test_cases = [
        FloorTestCase {
            description: "a point before every change keeps them all",
            transactions: transactions(),
            floor: 100,
            expected_lsns: vec![100, 101, 101, 102],
        },
        FloorTestCase {
            description: "a point at the second transaction drops the first",
            transactions: transactions(),
            floor: 101,
            expected_lsns: vec![101, 101, 102],
        },
        FloorTestCase {
            description: "a point after every change drops them all",
            transactions: transactions(),
            floor: 103,
            expected_lsns: vec![],
        },
        FloorTestCase {
            description: "another table's changes in the same transactions are kept, even before the point",
            transactions: vec![
                vec![
                    relation_message(&["id", "a"]),
                    other_relation_message(&["id", "b"]),
                    insert_message(&["1", "x"]),
                    of_other_table(&insert_message(&["1", "p"])),
                ],
                vec![update_message(&["1", "y"]), insert_message(&["2", "z"])],
                vec![
                    update_message(&["2", "w"]),
                    of_other_table(&update_message(&["1", "q"])),
                ],
            ],
            floor: 102,
            expected_lsns: vec![100, 102, 102],
        },
    ];
    for test_case in test_cases {
        for spill_bytes in [usize::MAX, 1] {
            assert_eq!(
                floored_lsns(&test_case.transactions, (test_case.floor, spill_bytes))?,
                test_case.expected_lsns,
                "{} (keeping {spill_bytes} bytes in memory)",
                test_case.description
            );
        }
    }
    Ok(())
}
