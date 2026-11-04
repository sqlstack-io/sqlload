use crate::files_destination::_helpers::spill::spill_order;
use crate::files_destination::tests::test_types::SpillOrderTestCase;

#[test]
fn given_open_files_when_over_a_limit_then_the_largest_are_written_out_first() {
    let test_cases = [
        SpillOrderTestCase {
            description: "within both limits nothing is written out",
            sizes: &[("a", 10), ("b", 20)],
            max_files: 2,
            max_bytes: 30,
            expected_spilled: &[],
        },
        SpillOrderTestCase {
            description: "too many open files writes out the largest until the count fits",
            sizes: &[("a", 10), ("b", 30), ("c", 20)],
            max_files: 1,
            max_bytes: 1000,
            expected_spilled: &["b", "c"],
        },
        SpillOrderTestCase {
            description: "too many bytes writes out the largest until the bytes fit",
            sizes: &[("a", 10), ("b", 30), ("c", 20)],
            max_files: 10,
            max_bytes: 35,
            expected_spilled: &["b"],
        },
    ];
    for test_case in test_cases {
        let sizes: Vec<(String, usize)> = test_case
            .sizes
            .iter()
            .map(|(value, size)| ((*value).to_owned(), *size))
            .collect();
        let spilled = spill_order(&sizes, test_case.max_files, test_case.max_bytes);
        assert_eq!(
            spilled, test_case.expected_spilled,
            "{}",
            test_case.description
        );
    }
}
