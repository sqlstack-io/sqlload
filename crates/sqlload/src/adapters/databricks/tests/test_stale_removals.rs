use crate::adapters::databricks::_helpers::stage::folders::stale_removals;
use crate::adapters::databricks::tests::helpers::listed_load;
use crate::adapters::databricks::tests::test_types::StaleRemovalsTestCase;

const BEFORE: i64 = 1_000;

#[test]
fn given_listed_load_folders_when_choosing_stale_ones_then_only_wholly_old_loads_lose_their_listed_files()
 {
    let test_cases = [
        StaleRemovalsTestCase {
            description: "a load whose files are all older than the cutoff loses those files, then its batch and load folders",
            loads: vec![("old", vec![Some(10), Some(999)])],
            expected_files: vec!["/v/sqlload/old/00001/part-0", "/v/sqlload/old/00001/part-1"],
            expected_folders: vec!["/v/sqlload/old/00001", "/v/sqlload/old"],
        },
        StaleRemovalsTestCase {
            description: "one file at the cutoff keeps the whole load, its old files included",
            loads: vec![("mixed", vec![Some(10), Some(BEFORE)])],
            expected_files: vec![],
            expected_folders: vec![],
        },
        StaleRemovalsTestCase {
            description: "a file listed without a time keeps the whole load",
            loads: vec![("undated", vec![Some(10), None])],
            expected_files: vec![],
            expected_folders: vec![],
        },
        StaleRemovalsTestCase {
            description: "a load holding no file only has its folders tried, which a load writing meanwhile keeps",
            loads: vec![("empty", vec![])],
            expected_files: vec![],
            expected_folders: vec!["/v/sqlload/empty/00001", "/v/sqlload/empty"],
        },
    ];
    for test_case in test_cases {
        let listed: Vec<_> = test_case
            .loads
            .iter()
            .map(|(name, times)| listed_load(name, times))
            .collect();
        let removals = stale_removals(&listed, BEFORE);
        assert_eq!(
            (removals.files, removals.folders),
            (
                test_case
                    .expected_files
                    .iter()
                    .map(|path| (*path).to_owned())
                    .collect(),
                test_case
                    .expected_folders
                    .iter()
                    .map(|path| (*path).to_owned())
                    .collect(),
            ),
            "{}",
            test_case.description
        );
    }
}
