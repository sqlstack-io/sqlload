use crate::adapters::files::_helpers::layout::partitions::group_partitions;
use crate::adapters::files::models::PartitionBy;
use crate::adapters::files::tests::helpers::{orders_path, stored};
use crate::adapters::files::tests::test_types::GroupPartitionsTestCase;
use crate::column_types::models::ColumnType;

#[test]
fn given_listed_files_when_grouping_then_partitions_come_oldest_first_and_strays_are_problems() {
    let test_cases = [
        GroupPartitionsTestCase {
            description: "date partitions are ordered by date and only Parquet files count",
            keys: &[
                "day=2026-11-10/part-a-0.parquet",
                "day=2026-11-09/part-b-0.parquet",
                "day=2026-11-09/part-b-1.parquet",
                "day=2026-11-09/notes.txt",
            ],
            partition: Some(("day=", PartitionBy::Date)),
            cursor_type: ColumnType::TimestampTz,
            expected_partitions: &[("2026-11-09", 2), ("2026-11-10", 1)],
            expected_problems: "",
        },
        GroupPartitionsTestCase {
            description: "int partitions are ordered as numbers, not text",
            keys: &["id=10/part-a-0.parquet", "id=9/part-a-0.parquet"],
            partition: Some(("id=", PartitionBy::Value)),
            cursor_type: ColumnType::Int,
            expected_partitions: &[("9", 1), ("10", 1)],
            expected_problems: "",
        },
        GroupPartitionsTestCase {
            description: "a file outside a partition folder is a problem",
            keys: &["part-a-0.parquet", "day=2026-11-09/part-a-0.parquet"],
            partition: Some(("day=", PartitionBy::Date)),
            cursor_type: ColumnType::Date,
            expected_partitions: &[("2026-11-09", 1)],
            expected_problems: "part-a-0.parquet is not in a partition folder day={cursor:date}/",
        },
        GroupPartitionsTestCase {
            description: "a folder whose value does not parse is a problem",
            keys: &["day=soon/part-a-0.parquet"],
            partition: Some(("day=", PartitionBy::Date)),
            cursor_type: ColumnType::Date,
            expected_partitions: &[],
            expected_problems: "\"soon\" is not a date",
        },
        GroupPartitionsTestCase {
            description: "escaped folder names are read back as the values written",
            keys: &[
                "code=%5Fdraft/part-a-0.parquet",
                "code=50%25/part-a-0.parquet",
            ],
            partition: Some(("code=", PartitionBy::Value)),
            cursor_type: ColumnType::Text,
            expected_partitions: &[("50%", 1), ("_draft", 1)],
            expected_problems: "",
        },
        GroupPartitionsTestCase {
            description: "without partitions every file is in one group",
            keys: &["part-a-0.parquet", "part-b-0.parquet"],
            partition: None,
            cursor_type: ColumnType::Int,
            expected_partitions: &[("", 2)],
            expected_problems: "",
        },
        GroupPartitionsTestCase {
            description: "an empty folder has no partitions",
            keys: &[],
            partition: None,
            cursor_type: ColumnType::Int,
            expected_partitions: &[],
            expected_problems: "",
        },
    ];
    for test_case in test_cases {
        let files = test_case.keys.iter().map(|key| stored(key)).collect();
        let path = orders_path(test_case.partition);
        let grouped = group_partitions(files, &path, Some(&test_case.cursor_type));
        let partitions: Vec<(&str, usize)> = grouped
            .partitions
            .iter()
            .map(|partition| {
                (
                    partition.value.as_deref().unwrap_or_default(),
                    partition.files.len(),
                )
            })
            .collect();
        let problems = grouped.problems.join("; ");
        assert_eq!(
            partitions, test_case.expected_partitions,
            "{}",
            test_case.description
        );
        assert!(
            problems.contains(test_case.expected_problems)
                && problems.is_empty() == test_case.expected_problems.is_empty(),
            "{}: {problems}",
            test_case.description
        );
    }
}
