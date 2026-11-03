use crate::naming::main::shard_of::shard_of;
use crate::naming::tests::test_types::ShardTestCase;

#[test]
fn given_a_new_shard_count_when_assigning_streams_then_only_the_new_shards_share_moves() {
    let test_cases = [
        ShardTestCase {
            description: "one shard owns everything, so adding a second moves about half",
            before: 1,
            after: 2,
            expected_max_moved: 560,
        },
        ShardTestCase {
            description: "four to five instances moves about a fifth, not the 80% of hash modulo",
            before: 4,
            after: 5,
            expected_max_moved: 250,
        },
        ShardTestCase {
            description: "an unchanged count moves nothing",
            before: 3,
            after: 3,
            expected_max_moved: 0,
        },
    ];
    let tables: Vec<String> = (0..1_000)
        .map(|index| format!("SOURCE_{index}__EVENTS"))
        .collect();
    for test_case in &test_cases {
        let moved = tables
            .iter()
            .filter(|table| shard_of(table, test_case.before) != shard_of(table, test_case.after))
            .count();
        assert!(
            moved <= test_case.expected_max_moved,
            "{}: {moved} moved",
            test_case.description
        );
    }
}
