use crate::loader::tests::helpers::{merged_file_round_trip, python_spec};
use crate::loader::tests::test_types::MergedFileTestCase;

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
