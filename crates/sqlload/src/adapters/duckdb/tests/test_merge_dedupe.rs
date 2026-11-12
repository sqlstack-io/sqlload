use crate::adapters::duckdb::tests::helpers::merged_rows;
use crate::adapters::duckdb::tests::test_types::MergeDedupeTestCase;

#[test]
fn given_a_merge_commit_repeating_a_key_when_duckdb_runs_it_then_one_row_per_key_wins_by_the_documented_rule()
 {
    let test_cases = [
        MergeDedupeTestCase {
            description: "without dedup_sort the last row in source order wins, whatever order staging holds the rows in",
            sort: None,
            existing: &[],
            staged: &[
                "(1, 'second', 3, 2)",
                "(1, 'first', 5, 1)",
                "(2, 'only', 1, 3)",
            ],
            expected_rows: &["1:second", "2:only"],
        },
        MergeDedupeTestCase {
            description: "dedup_sort desc keeps the highest value even when an earlier row holds it",
            sort: Some(true),
            existing: &[],
            staged: &[
                "(1, 'second', 3, 2)",
                "(1, 'first', 5, 1)",
                "(2, 'only', 1, 3)",
            ],
            expected_rows: &["1:first", "2:only"],
        },
        MergeDedupeTestCase {
            description: "dedup_sort asc keeps the lowest value",
            sort: Some(false),
            existing: &[],
            staged: &[
                "(1, 'first', 5, 1)",
                "(1, 'second', 3, 2)",
                "(1, 'third', 4, 3)",
            ],
            expected_rows: &["1:second"],
        },
        MergeDedupeTestCase {
            description: "a NULL sort value loses to any other value, under desc",
            sort: Some(true),
            existing: &[],
            staged: &["(1, 'low', -7, 1)", "(1, 'null', NULL, 2)"],
            expected_rows: &["1:low"],
        },
        MergeDedupeTestCase {
            description: "a NULL sort value loses to any other value, under asc",
            sort: Some(false),
            existing: &[],
            staged: &["(1, 'null', NULL, 1)", "(1, 'high', 99, 2)"],
            expected_rows: &["1:high"],
        },
        MergeDedupeTestCase {
            description: "a tie on the sort value goes to the later row",
            sort: Some(true),
            existing: &[],
            staged: &[
                "(1, 'later', 5, 2)",
                "(1, 'earlier', 5, 1)",
                "(1, 'lower', 4, 3)",
            ],
            expected_rows: &["1:later"],
        },
        MergeDedupeTestCase {
            description: "rows with a NULL key are never collapsed",
            sort: None,
            existing: &[],
            staged: &[
                "(NULL, 'x', 1, 1)",
                "(NULL, 'y', 1, 2)",
                "(3, 'a', 1, 3)",
                "(3, 'b', 1, 4)",
            ],
            expected_rows: &["3:b", "NULL:x", "NULL:y"],
        },
        MergeDedupeTestCase {
            description: "a repeated key already in the table is updated once, to the winning row",
            sort: None,
            existing: &["(1, 'old', 9)", "(4, 'kept', 0)"],
            staged: &["(1, 'a', 1, 1)", "(1, 'b', 2, 2)"],
            expected_rows: &["1:b", "4:kept"],
        },
    ];

    for test_case in test_cases {
        let merged = merged_rows(test_case.sort, (test_case.existing, test_case.staged));

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
