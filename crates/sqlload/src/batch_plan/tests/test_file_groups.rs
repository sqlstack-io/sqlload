use crate::batch_plan::tests::helpers::{groups, owned_groups};
use crate::batch_plan::tests::test_types::{FileGroupsTestCase, PartitionFiles};

const GB: u64 = 1000 * 1000 * 1000;

const fn eu(day: &'static str, files: usize) -> PartitionFiles {
    PartitionFiles {
        region: "eu",
        day,
        files,
        file_bytes: 1,
    }
}

#[test]
fn given_partitioned_files_when_grouping_them_then_batches_hold_whole_partitions_up_to_1000_files_or_1_gb()
 {
    let test_cases = [
        FileGroupsTestCase {
            description: "small partitions share a batch",
            partitions: vec![eu("2026-11-01", 400), eu("2026-11-02", 500)],
            by_value: None,
            expected_groups: &[(900, &[])],
        },
        FileGroupsTestCase {
            description: "a partition that would pass 1,000 files starts the next batch",
            partitions: vec![eu("2026-11-01", 600), eu("2026-11-02", 600)],
            by_value: None,
            expected_groups: &[(600, &[]), (600, &[])],
        },
        FileGroupsTestCase {
            description: "a partition of more than 1,000 files is split when it is not replaced as a whole",
            partitions: vec![eu("2026-11-01", 2500)],
            by_value: None,
            expected_groups: &[(1000, &[]), (1000, &[]), (500, &[])],
        },
        FileGroupsTestCase {
            description: "about 1 GB closes a batch before 1,000 files do",
            partitions: vec![
                PartitionFiles {
                    region: "eu",
                    day: "2026-11-01",
                    files: 3,
                    file_bytes: GB / 2,
                },
                PartitionFiles {
                    region: "eu",
                    day: "2026-11-02",
                    files: 1,
                    file_bytes: GB / 2,
                },
            ],
            by_value: None,
            expected_groups: &[(2, &[]), (2, &[])],
        },
        FileGroupsTestCase {
            description: "delete_insert keeps a value's files together however many, listing the values",
            partitions: vec![
                eu("2026-11-01", 10),
                eu("2026-11-02", 1500),
                eu("2026-11-03", 5),
            ],
            by_value: Some("day"),
            expected_groups: &[
                (10, &["2026-11-01"]),
                (1500, &["2026-11-02"]),
                (5, &["2026-11-03"]),
            ],
        },
        FileGroupsTestCase {
            description: "delete_insert packs whole values up to 1,000 files",
            partitions: vec![
                eu("2026-11-01", 300),
                eu("2026-11-02", 300),
                eu("2026-11-03", 500),
            ],
            by_value: Some("day"),
            expected_groups: &[(600, &["2026-11-01", "2026-11-02"]), (500, &["2026-11-03"])],
        },
        FileGroupsTestCase {
            description: "delete_insert never splits a partition for its size either",
            partitions: vec![PartitionFiles {
                region: "eu",
                day: "2026-11-01",
                files: 3,
                file_bytes: GB,
            }],
            by_value: Some("day"),
            expected_groups: &[(3, &["2026-11-01"])],
        },
        FileGroupsTestCase {
            description: "a value's files commit together even when another partition key comes first and more than 1,000 are listed apart",
            partitions: vec![
                eu("2026-11-01", 700),
                eu("2026-11-02", 2),
                PartitionFiles {
                    region: "us",
                    day: "2026-11-01",
                    files: 700,
                    file_bytes: 1,
                },
                PartitionFiles {
                    region: "us",
                    day: "2026-11-02",
                    files: 2,
                    file_bytes: 1,
                },
            ],
            by_value: Some("day"),
            expected_groups: &[(1400, &["2026-11-01"]), (4, &["2026-11-02"])],
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
