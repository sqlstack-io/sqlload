use crate::store_paths::main::escape_partition_value::escape_partition_value;
use crate::store_paths::main::unescape_partition_value::unescape_partition_value;
use crate::store_paths::tests::test_types::PartitionValueTestCase;

#[test]
fn given_a_partition_value_when_escaping_then_the_folder_name_is_safe_and_reads_back() {
    let test_cases = [
        PartitionValueTestCase {
            description: "plain values are kept",
            value: "2026-11-04",
            expected_escaped: "2026-11-04",
        },
        PartitionValueTestCase {
            description: "a leading underscore would hide the folder",
            value: "_draft",
            expected_escaped: "%5Fdraft",
        },
        PartitionValueTestCase {
            description: "a leading dot would hide the folder",
            value: ".x.y",
            expected_escaped: "%2Ex.y",
        },
        PartitionValueTestCase {
            description: "percent signs, slashes and Hive's characters are escaped",
            value: "50%/a=b:c",
            expected_escaped: "50%25%2Fa%3Db%3Ac",
        },
        PartitionValueTestCase {
            description: "control characters are escaped and other text is kept",
            value: "tab\there é",
            expected_escaped: "tab%09here é",
        },
    ];
    for test_case in test_cases {
        let escaped = escape_partition_value(test_case.value);
        assert_eq!(
            escaped, test_case.expected_escaped,
            "{}",
            test_case.description
        );
        assert_eq!(
            unescape_partition_value(&escaped),
            test_case.value,
            "{}: reads back",
            test_case.description
        );
    }
}
