use crate::adapters::duckdb::tests::helpers::merged_rows;
use crate::adapters::duckdb::tests::test_types::MergeDedupeTestCase;

#[test]
fn given_a_merge_commit_repeating_a_key_when_duckdb_runs_it_then_one_row_per_key_wins_by_the_documented_rule()
 {
    let test_cases = [
        MergeDedupeTestCase {
            description: "without newest_by the last row in source order wins, whatever order staging holds the rows in",
            newest_by: &[],
            existing: &[],
            staged: &[
                "(1, 'second', 3, 2)",
                "(1, 'first', 5, 1)",
                "(2, 'only', 1, 3)",
            ],
            expected_rows: &["1:second", "2:only"],
        },
        MergeDedupeTestCase {
            description: "newest_by keeps the highest value even when an earlier row holds it",
            newest_by: &["score"],
            existing: &[],
            staged: &[
                "(1, 'second', 3, 2)",
                "(1, 'first', 5, 1)",
                "(2, 'only', 1, 3)",
            ],
            expected_rows: &["1:first", "2:only"],
        },
        MergeDedupeTestCase {
            description: "a list compares in order: a tie on the first column goes to the higher second, though read earlier",
            newest_by: &["score", "name"],
            existing: &[],
            staged: &["(1, 'b', 5, 1)", "(1, 'a', 5, 2)", "(1, 'z', 4, 3)"],
            expected_rows: &["1:b"],
        },
        MergeDedupeTestCase {
            description: "a NULL loses to any other value",
            newest_by: &["score"],
            existing: &[],
            staged: &["(1, 'low', -7, 1)", "(1, 'null', NULL, 2)"],
            expected_rows: &["1:low"],
        },
        MergeDedupeTestCase {
            description: "two NULLs tie, so the list's next column decides",
            newest_by: &["score", "name"],
            existing: &[],
            staged: &["(1, 'b', NULL, 1)", "(1, 'a', NULL, 2)"],
            expected_rows: &["1:b"],
        },
        MergeDedupeTestCase {
            description: "a tie goes to the later row",
            newest_by: &["score"],
            existing: &[],
            staged: &[
                "(1, 'later', 5, 2)",
                "(1, 'earlier', 5, 1)",
                "(1, 'lower', 4, 3)",
            ],
            expected_rows: &["1:later"],
        },
        MergeDedupeTestCase {
            description: "rows with a NULL key collapse like any other repeated key: the last in source order wins",
            newest_by: &[],
            existing: &[],
            staged: &[
                "(NULL, 'x', 1, 1)",
                "(NULL, 'y', 1, 2)",
                "(3, 'a', 1, 3)",
                "(3, 'b', 1, 4)",
            ],
            expected_rows: &["3:b", "NULL:y"],
        },
        MergeDedupeTestCase {
            description: "rows with a NULL key collapse to the newest, even an earlier row",
            newest_by: &["score"],
            existing: &[],
            staged: &[
                "(NULL, 'high', 7, 1)",
                "(NULL, 'low', 2, 2)",
                "(NULL, 'none', NULL, 3)",
            ],
            expected_rows: &["NULL:high"],
        },
        MergeDedupeTestCase {
            description: "a NULL key the table already holds is updated, not inserted again",
            newest_by: &[],
            existing: &["(NULL, 'old', 0)", "(4, 'kept', 0)"],
            staged: &["(NULL, 'new', 1, 1)"],
            expected_rows: &["4:kept", "NULL:new"],
        },
        MergeDedupeTestCase {
            description: "without newest_by a row in the table is overwritten whatever its values",
            newest_by: &[],
            existing: &["(1, 'old', 9)", "(4, 'kept', 0)"],
            staged: &["(1, 'a', 1, 1)", "(1, 'b', 2, 2)"],
            expected_rows: &["1:b", "4:kept"],
        },
        MergeDedupeTestCase {
            description: "a staged row older than the table's row of its key leaves it, while a newer one updates its own",
            newest_by: &["score"],
            existing: &["(1, 'new', 9)", "(2, 'old', 1)"],
            staged: &["(1, 'older', 3, 1)", "(2, 'newer', 2, 2)"],
            expected_rows: &["1:new", "2:newer"],
        },
        MergeDedupeTestCase {
            description: "a tie with the table's row overwrites it, and so does any value over a NULL",
            newest_by: &["score"],
            existing: &["(1, 'old', 5)", "(2, 'unset', NULL)"],
            staged: &["(1, 'tie', 5, 1)", "(2, 'set', -1, 2)"],
            expected_rows: &["1:tie", "2:set"],
        },
        MergeDedupeTestCase {
            description: "a staged NULL never overwrites a set value, and two NULLs tie, so it overwrites a NULL",
            newest_by: &["score"],
            existing: &["(1, 'set', 5)", "(2, 'unset', NULL)"],
            staged: &["(1, 'null', NULL, 1)", "(2, 'also', NULL, 2)"],
            expected_rows: &["1:set", "2:also"],
        },
        MergeDedupeTestCase {
            description: "a list against the table: a lower first column loses whatever the second, a tie on it goes to the higher second",
            newest_by: &["score", "name"],
            existing: &["(1, 'a', 5)", "(2, 'a', 5)", "(3, 'm', 5)"],
            staged: &["(1, 'z', 4, 1)", "(2, 'b', 5, 2)", "(3, 'c', 5, 3)"],
            expected_rows: &["1:a", "2:b", "3:m"],
        },
    ];

    for test_case in test_cases {
        let merged = merged_rows(test_case.newest_by, (test_case.existing, test_case.staged));

        assert_eq!(
            merged,
            Ok(test_case
                .expected_rows
                .iter()
                .map(|row| (*row).to_owned())
                .collect()),
            "{}",
            test_case.description
        );
    }
}
