use std::collections::BTreeSet;

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::_helpers::layout::partitions::group_partitions;
use crate::adapters::files::main::plan_replaced_files::plan_replaced_files;
use crate::adapters::files::models::{PartitionBy, ReplaceRequest};
use crate::adapters::files::tests::helpers::{delete_insert, orders_path, stored};
use crate::adapters::files::tests::test_types::ReplacedFilesTestCase;
use crate::column_types::models::ColumnType;

const KEYS: [&str; 3] = [
    "day=2026-11-03/part-a-0.parquet",
    "day=2026-11-04/part-a-0.parquet",
    "day=2026-11-05/part-a-0.parquet",
];

#[test]
fn given_a_write_when_planning_the_replaced_files_then_they_are_known_before_writing() {
    let test_cases = [
        ReplacedFilesTestCase {
            description: "append replaces nothing",
            mode: WriteMode::Append,
            written: &["2026-11-05"],
            expected_keys: Ok(&[]),
        },
        ReplacedFilesTestCase {
            description: "replace replaces every file",
            mode: WriteMode::Replace,
            written: &[],
            expected_keys: Ok(&KEYS),
        },
        ReplacedFilesTestCase {
            description: "delete_insert replaces the window and the partitions it wrote",
            mode: delete_insert(Some("2026-11-05"), None),
            written: &["2026-11-03"],
            expected_keys: Ok(&[
                "day=2026-11-05/part-a-0.parquet",
                "day=2026-11-03/part-a-0.parquet",
            ]),
        },
        ReplacedFilesTestCase {
            description: "a bound that is not a date fails before anything is written",
            mode: delete_insert(Some("yesterday"), None),
            written: &[],
            expected_keys: Err("cursor bound \"yesterday\": \"yesterday\" is not a date"),
        },
        ReplacedFilesTestCase {
            description: "merge is refused",
            mode: WriteMode::Merge {
                unique_key: vec!["id".to_owned()],
            },
            written: &[],
            expected_keys: Err("merge is not supported"),
        },
    ];
    let path = orders_path(Some(("day=", PartitionBy::Date)));
    let listed = group_partitions(
        KEYS.iter().map(|key| stored(key)).collect(),
        &path,
        Some(&ColumnType::Date),
    );
    for test_case in test_cases {
        let request = ReplaceRequest {
            mode: &test_case.mode,
            listed: &listed,
            partition: path.partition.as_ref(),
            cursor_type: Some(&ColumnType::Date),
        };
        let written: BTreeSet<String> = test_case
            .written
            .iter()
            .map(|value| (*value).to_owned())
            .collect();
        let keys = plan_replaced_files(&request).map(|replaced| replaced.keys(&written));
        let expected: Option<Vec<String>> = test_case
            .expected_keys
            .ok()
            .map(|keys| keys.iter().map(|key| (*key).to_owned()).collect());
        assert_eq!(
            keys.as_ref().ok(),
            expected.as_ref(),
            "{}",
            test_case.description
        );
        assert!(
            keys.err()
                .unwrap_or_default()
                .contains(test_case.expected_keys.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
