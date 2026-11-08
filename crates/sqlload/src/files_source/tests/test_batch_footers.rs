use crate::files_source::tests::helpers::{
    batch_footers_after_plan, block_on, read_with_broken_footer,
};
use crate::files_source::tests::test_types::{BatchFootersTestCase, SampledFootersTestCase};

#[test]
fn given_a_batchs_footers_when_reading_its_files_then_no_footer_is_read_again() -> Result<(), String>
{
    let test_cases = [
        BatchFootersTestCase {
            description: "the footer read earlier is reused, so the broken one is never read",
            reuse: true,
            expected_outcome: "3",
        },
        BatchFootersTestCase {
            description: "without it the read must fetch the footer, and fails",
            reuse: false,
            expected_outcome: "failed",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            block_on(read_with_broken_footer(test_case.reuse))?,
            test_case.expected_outcome,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_footers_plan_sampled_when_reading_a_batchs_footers_then_the_files_are_not_read_again()
-> Result<(), String> {
    let test_cases = [
        SampledFootersTestCase {
            description: "the batch takes the footer plan read, so the broken file is not read",
            batch_reads: 1,
            expected_outcome: "3",
        },
        SampledFootersTestCase {
            description: "the sampled footer is taken once: a second read fetches the file's own, and fails",
            batch_reads: 2,
            expected_outcome: "failed",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            block_on(batch_footers_after_plan(test_case.batch_reads))?,
            test_case.expected_outcome,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
