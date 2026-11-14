use crate::loader::tests::helpers::{merged_file_round_trip, python_spec, reload_keys};
use crate::loader::tests::test_types::{MergedFileTestCase, ReloadOrderTestCase};

#[test]
fn given_a_merged_file_when_its_record_is_read_back_then_the_ledger_has_its_name_and_version()
-> Result<(), String> {
    let spec = python_spec("{}")?;
    let test_cases = [
        MergedFileTestCase {
            description: "a UNC share's file",
            file: (r"\\server\share\drop/a.csv", "120-1700000000000"),
            expected_file: (r"\\server\share\drop/a.csv", "120-1700000000000"),
        },
        MergedFileTestCase {
            description: "a drive path's file",
            file: (r"C:\data\drop/b.csv", "7-1700000000000"),
            expected_file: (r"C:\data\drop/b.csv", "7-1700000000000"),
        },
        MergedFileTestCase {
            description: "an object key holding the separator, split at its last",
            file: ("s3://bucket/drop/a|b.csv", "\"9b2cf5\""),
            expected_file: ("s3://bucket/drop/a|b.csv", "\"9b2cf5\""),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            merged_file_round_trip(&spec, test_case.file)?,
            (
                test_case.expected_file.0.to_owned(),
                test_case.expected_file.1.to_owned()
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_merge_ledger_when_reloading_then_files_go_in_the_order_they_merged_and_the_rest_oldest_first()
 {
    let test_cases = [
        ReloadOrderTestCase {
            description: "recorded files keep their merge order, whatever their modified times",
            files: &[
                ("drop/", "a.csv", 3_000),
                ("drop/", "b.csv", 1_000),
                ("drop/", "c.csv", 2_000),
            ],
            merged: &[
                ("drop/a.csv", "1-3000"),
                ("drop/b.csv", "1-1000"),
                ("drop/c.csv", "1-2000"),
            ],
            expected_keys: &["drop/a.csv", "drop/b.csv", "drop/c.csv"],
        },
        ReloadOrderTestCase {
            description: "a file whose version changed (moved, so copied) keeps its place by name",
            files: &[("done/", "a.csv", 9_000), ("drop/", "b.csv", 1_000)],
            merged: &[("drop/a.csv", "1-500"), ("drop/b.csv", "1-1000")],
            expected_keys: &["done/a.csv", "drop/b.csv"],
        },
        ReloadOrderTestCase {
            description: "a file merged at two versions goes where its current version merged",
            files: &[("drop/", "a.csv", 4_000), ("drop/", "b.csv", 2_000)],
            merged: &[
                ("drop/a.csv", "1-1000"),
                ("drop/b.csv", "1-2000"),
                ("drop/a.csv", "1-4000"),
            ],
            expected_keys: &["drop/b.csv", "drop/a.csv"],
        },
        ReloadOrderTestCase {
            description: "a file in the folder under a merged name, at a version never merged, is a newer file: it follows the archived copy",
            files: &[
                ("drop/", "export.csv", 2_000),
                ("done/", "export.csv", 1_000),
            ],
            merged: &[("drop/export.csv", "1-1000")],
            expected_keys: &["done/export.csv", "drop/export.csv"],
        },
        ReloadOrderTestCase {
            description: "files never merged follow, oldest modified first",
            files: &[
                ("drop/", "new2.csv", 5_000),
                ("drop/", "new1.csv", 4_000),
                ("drop/", "old.csv", 9_000),
            ],
            merged: &[("drop/old.csv", "1-9000")],
            expected_keys: &["drop/old.csv", "drop/new1.csv", "drop/new2.csv"],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            reload_keys(&test_case),
            test_case.expected_keys,
            "{}",
            test_case.description
        );
    }
}
