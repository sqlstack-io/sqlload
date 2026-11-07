use crate::cdc::tests::helpers::{buffered_seqs, insert_message, relation_message};
use crate::cdc::tests::test_types::BufferTestCase;

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
            description: "an insert, a middle column dropped, then an insert is refused, not misread",
            messages: vec![
                relation_message(&["id", "a", "b"]),
                insert_message(&["1", "x", "y"]),
                relation_message(&["id", "b"]),
                insert_message(&["2", "z"]),
            ],
            expected_seqs: Err(
                "public.t changed shape while streaming; schema changes while streaming come in a later release, so re-snapshot with --resnapshot",
            ),
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
