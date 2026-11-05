use crate::parquet_parts::tests::helpers::written_files;
use crate::parquet_parts::tests::test_types::WritePartTestCase;

#[test]
fn given_batches_when_writing_parts_then_files_are_complete_parquet_split_by_schema()
-> Result<(), String> {
    let test_cases = [
        WritePartTestCase {
            description: "batches of one schema share a file",
            batches: &[("id", 10), ("id", 5)],
            expected_files: &[15],
        },
        WritePartTestCase {
            description: "a batch of another schema starts a new file",
            batches: &[("id", 10), ("other", 3), ("other", 2)],
            expected_files: &[10, 5],
        },
        WritePartTestCase {
            description: "one batch of about 10 MB is written in slices of 8,192 rows, ending the first file at the slice where it reaches 6 MB",
            batches: &[("id", 8_000_000)],
            expected_files: &[4_096_000, 3_904_000],
        },
        WritePartTestCase {
            description: "nothing written gives no file",
            batches: &[],
            expected_files: &[],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            written_files(test_case.batches)?,
            test_case.expected_files,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
