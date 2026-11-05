use crate::batch_plan::tests::helpers::{groups, owned_groups};
use crate::batch_plan::tests::test_types::FileGroupsTestCase;

#[test]
fn given_partitioned_files_when_grouping_them_then_batches_hold_whole_partitions_up_to_1000_files()
{
    let test_cases = [
        FileGroupsTestCase {
            description: "small partitions share a batch",
            partitions: &[("2026-11-01", 400), ("2026-11-02", 500)],
            by_value: None,
            expected_groups: &[(900, &[])],
        },
        FileGroupsTestCase {
            description: "a partition that would pass 1,000 files starts the next batch",
            partitions: &[("2026-11-01", 600), ("2026-11-02", 600)],
            by_value: None,
            expected_groups: &[(600, &[]), (600, &[])],
        },
        FileGroupsTestCase {
            description: "a partition of more than 1,000 files is split when it is not replaced as a whole",
            partitions: &[("2026-11-01", 2500)],
            by_value: None,
            expected_groups: &[(1000, &[]), (1000, &[]), (500, &[])],
        },
        FileGroupsTestCase {
            description: "delete_insert keeps a value's files together however many, listing the values",
            partitions: &[("2026-11-01", 10), ("2026-11-02", 1500), ("2026-11-03", 5)],
            by_value: Some("day"),
            expected_groups: &[
                (10, &["2026-11-01"]),
                (1500, &["2026-11-02"]),
                (5, &["2026-11-03"]),
            ],
        },
        FileGroupsTestCase {
            description: "delete_insert packs whole values up to 1,000 files",
            partitions: &[
                ("2026-11-01", 300),
                ("2026-11-02", 300),
                ("2026-11-03", 500),
            ],
            by_value: Some("day"),
            expected_groups: &[(600, &["2026-11-01", "2026-11-02"]), (500, &["2026-11-03"])],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            groups(&test_case),
            owned_groups(test_case.expected_groups),
            "{}",
            test_case.description
        );
    }
}
