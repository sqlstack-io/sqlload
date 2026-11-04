use crate::store_paths::main::hive_values::hive_values;
use crate::store_paths::tests::test_types::HiveValuesTestCase;

#[test]
fn given_an_object_key_when_reading_hive_folders_then_each_key_value_is_returned() {
    let test_cases = [
        HiveValuesTestCase {
            description: "every key=value folder in order",
            key: "snapshot_date=2026-11-03/snapshot_id=abc/part-0.parquet",
            expected_values: &[("snapshot_date", "2026-11-03"), ("snapshot_id", "abc")],
        },
        HiveValuesTestCase {
            description: "plain folders and the file name are ignored",
            key: "extra/region=eu/file=x.parquet",
            expected_values: &[("region", "eu")],
        },
        HiveValuesTestCase {
            description: "escaped values are decoded",
            key: "city=New%20York/part.parquet",
            expected_values: &[("city", "New York")],
        },
    ];
    for test_case in test_cases {
        let values = hive_values(test_case.key);
        let values: Vec<(&str, &str)> = values
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        assert_eq!(
            values, test_case.expected_values,
            "{}",
            test_case.description
        );
    }
}
