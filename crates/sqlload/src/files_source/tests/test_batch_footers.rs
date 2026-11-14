use crate::files_source::tests::helpers::{block_on, read_same_key_files, read_with_broken_footer};
use crate::files_source::tests::test_types::BatchFootersTestCase;

#[test]
fn given_a_batchs_footers_when_reading_its_files_then_no_footer_is_read_again() -> Result<(), String>
{
    let test_cases = [
        BatchFootersTestCase {
            description: "the footer read earlier is reused, so the broken one is never read",
            reuse: true,
            same_key: false,
            expected_outcome: "3",
        },
        BatchFootersTestCase {
            description: "without it the read must fetch the footer, and fails",
            reuse: false,
            same_key: false,
            expected_outcome: "failed",
        },
        BatchFootersTestCase {
            description: "two files with one relative key in two folders each read with their own footer",
            reuse: true,
            same_key: true,
            expected_outcome: "4",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            match test_case.same_key {
                true => block_on(read_same_key_files())?,
                false => block_on(read_with_broken_footer(test_case.reuse))?,
            },
            test_case.expected_outcome,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
